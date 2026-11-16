package com.alibaba.datax.core.statistics.plugin.task.sink;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 脏数据落盘的全局管理器。由于收集器由固定签名的反射构造器创建，无法通过构造参数传入，
 * 因此用 jobId 做 key 的静态注册表把 collector 与本管理器桥接起来（一个 JVM 只跑一个 job，
 * 回放是独立 JVM）。
 *
 * 负责：channel 注册表、跨 task 的全局总量计量与超限策略、manifest 汇总、
 * 正常/失败关闭的完整性、JVM shutdown hook 兜底。
 */
public class DirtyRecordSinkManager {

    private static final Logger LOG = LoggerFactory.getLogger(DirtyRecordSinkManager.class);

    private static final ConcurrentHashMap<Long, DirtyRecordSinkManager> REGISTRY =
            new ConcurrentHashMap<Long, DirtyRecordSinkManager>();

    private final DirtySinkContext context;

    private final ConcurrentHashMap<SinkChannelKey, SinkChannel> channels =
            new ConcurrentHashMap<SinkChannelKey, SinkChannel>();

    private final SinkManifest manifest;

    /**
     * 已预留（允许写入）的总字节数，全局上限以此为准。
     */
    private final AtomicLong reservedBytes = new AtomicLong(0);

    private final AtomicLong droppedRecords = new AtomicLong(0);

    private final AtomicBoolean capReached = new AtomicBoolean(false);

    private final AtomicBoolean closed = new AtomicBoolean(false);

    private volatile boolean closing = false;

    private Thread shutdownHook;

    private DirtyRecordSinkManager(DirtySinkContext context) {
        this.context = context;
        this.manifest = new SinkManifest();
        this.manifest.setJobId(context.getJobId());
        this.manifest.setSinkName(context.getSinkName());
        this.manifest.setCreatedAt(System.currentTimeMillis());
        this.manifest.setTotalSizeCapBytes(context.getTotalSizeCapBytes());
        this.manifest.setMaskedColumns(context.getMaskedColumns());
    }

    public static void register(DirtySinkContext context) {
        context.prepareDirectory();
        DirtyRecordSinkManager manager = new DirtyRecordSinkManager(context);
        DirtyRecordSinkManager old = REGISTRY.putIfAbsent(context.getJobId(), manager);
        if (old != null) {
            if (!old.closed.get()) {
                throw DataXException.asDataXException(FrameworkErrorCode.DIRTY_RECORD_SINK_ERROR,
                        "jobId=" + context.getJobId() + " 的脏数据落盘管理器已存在且未关闭，sinkName="
                                + context.getSinkName());
            }
            // 残留的已关闭条目：替换为新管理器
            REGISTRY.put(context.getJobId(), manager);
        }
        manager.attachShutdownHook();
        LOG.info("脏数据落盘已启用，sinkDir={}, fileMaxSize={}, totalSizeCap={}, overCapPolicy={}",
                context.getSinkDir().getAbsolutePath(), context.getMaxFileBytes(),
                context.getTotalSizeCapBytes(), context.getOverCapPolicy());
    }

    public static DirtyRecordSinkManager getOrNull(long jobId) {
        return REGISTRY.get(jobId);
    }

    public static void remove(long jobId) {
        REGISTRY.remove(jobId);
    }

    /**
     * collector 懒加载获取自己的 channel。
     */
    public SinkChannel acquire(int taskGroupId, int taskId, int attempt, PluginType pluginType) {
        if (closing) {
            throw DataXException.asDataXException(FrameworkErrorCode.DIRTY_RECORD_SINK_ERROR,
                    "脏数据落盘管理器正在关闭，不再接受新的channel: " + pluginType);
        }
        SinkChannelKey key = new SinkChannelKey(taskGroupId, taskId, attempt, pluginType);
        SinkChannel existing = channels.get(key);
        if (existing != null) {
            return existing;
        }
        SinkChannel created = new SinkChannel(this, context, key);
        SinkChannel raced = channels.putIfAbsent(key, created);
        return raced == null ? created : raced;
    }

    /**
     * 预留 n 个字节的总量额度。
     *
     * @return true=可以写入；false=STOP_RECORD 策略下超限，调用方丢弃该记录
     * @throws DataXException FAIL_JOB 策略下超限时抛 SINK_CAP_EXCEEDED
     */
    boolean reserve(long n) {
        long current = reservedBytes.addAndGet(n);
        if (current <= context.getTotalSizeCapBytes()) {
            return true;
        }
        capReached.set(true);
        if (context.isFailJobOnCapExceeded()) {
            throw DataXException.asDataXException(
                    FrameworkErrorCode.DIRTY_RECORD_SINK_CAP_EXCEEDED,
                    String.format("脏数据落盘总量超过上限[%d]字节，当前已预留[%d]字节，"
                            + "overCapPolicy=FAIL_JOB，任务失败",
                            context.getTotalSizeCapBytes(), current));
        }
        droppedRecords.incrementAndGet();
        return false;
    }

    /**
     * 写入失败时回滚预留额度。
     */
    void release(long n) {
        reservedBytes.addAndGet(-n);
    }

    void publishFileEntry(SinkFileEntry entry) {
        manifest.addFileEntry(entry);
    }

    public long getDroppedRecords() {
        return droppedRecords.get();
    }

    public long getReservedBytes() {
        return reservedBytes.get();
    }

    public DirtySinkContext getContext() {
        return context;
    }

    public void flushAll() {
        for (SinkChannel channel : channels.values()) {
            channel.flush();
        }
    }

    /**
     * 关闭所有 channel、finalize 全部文件、原子写 manifest 与状态标记。幂等。
     *
     * @param success job 是否成功；决定 manifest.jobStatus 与 _SUCCESS/_FAILED 标记
     */
    public void close(boolean success) {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        closing = true;
        try {
            // 1. 每个 channel flush 并 finalize 活动文件
            for (SinkChannel channel : channels.values()) {
                SinkFileEntry entry = channel.close();
                if (entry != null) {
                    manifest.addFileEntry(entry);
                }
            }

            // 2. manifest 汇总并原子写入
            manifest.setClosedAt(System.currentTimeMillis());
            manifest.setCapReached(capReached.get());
            manifest.setDroppedRecords(droppedRecords.get());
            manifest.setJobStatus(success
                    ? SinkConstants.JOB_STATUS_SUCCESS : SinkConstants.JOB_STATUS_FAILED);
            manifest.writeAtomically(context.getSinkDir());

            // 3. 写状态标记（manifest 已落盘，标记只作快速判断）
            writeMarker(success ? SinkConstants.SUCCESS_MARKER : SinkConstants.FAILED_MARKER);

            LOG.info("脏数据落盘关闭: sinkDir={}, status={}, files={}, records={}, bytes={}, dropped={}",
                    context.getSinkDir().getAbsolutePath(), manifest.getJobStatus(),
                    manifest.getFiles().size(), manifest.getTotalRecords(),
                    manifest.getTotalBytes(), droppedRecords.get());
        } finally {
            detachShutdownHook();
            remove(context.getJobId());
        }
    }

    public boolean isClosed() {
        return closed.get();
    }

    private void writeMarker(String markerName) {
        File marker = new File(context.getSinkDir(), markerName);
        try {
            Files.write(marker.toPath(),
                    String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            // 标记写失败不影响 manifest 完整性，仅告警
            LOG.warn("状态标记文件写入失败: {}", marker.getAbsolutePath(), e);
        }
    }

    private void attachShutdownHook() {
        this.shutdownHook = new Thread(new Runnable() {
            @Override
            public void run() {
                try {
                    flushAll();
                    if (!closed.get()) {
                        close(false);
                    }
                } catch (Throwable t) {
                    LOG.warn("shutdown hook 兜底关闭脏数据落盘时出错", t);
                }
            }
        }, "datax-dirty-sink-shutdown-hook");
        Runtime.getRuntime().addShutdownHook(shutdownHook);
    }

    private void detachShutdownHook() {
        if (shutdownHook != null) {
            try {
                Runtime.getRuntime().removeShutdownHook(shutdownHook);
            } catch (IllegalStateException e) {
                // JVM 已在关闭过程中，忽略
            }
        }
    }
}
