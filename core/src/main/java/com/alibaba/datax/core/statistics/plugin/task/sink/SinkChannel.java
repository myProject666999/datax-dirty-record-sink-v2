package com.alibaba.datax.core.statistics.plugin.task.sink;

import com.alibaba.datax.common.element.Record;
import com.alibaba.fastjson2.JSON;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 每个脏数据 collector 实例对应一个 channel，独占自己的滚动文件。
 * 全局总量计量与超限策略由 {@link DirtyRecordSinkManager} 统一管理。
 */
public class SinkChannel {

    private final DirtyRecordSinkManager manager;

    private final DirtySinkContext context;

    private final SinkChannelKey key;

    private final RollingJsonlWriter writer;

    private final DirtyRecordSerializer serializer;

    private final AtomicLong sequence = new AtomicLong(0);

    public SinkChannel(DirtyRecordSinkManager manager, DirtySinkContext context,
                       SinkChannelKey key) {
        this.manager = manager;
        this.context = context;
        this.key = key;
        this.writer = new RollingJsonlWriter(context.getSinkDir(), key.filePrefix(context.getJobId()),
                context.getMaxFileBytes(), context.isFlushEveryRecord());
        this.serializer = new DirtyRecordSerializer(context, key.getTaskGroupId(),
                key.getTaskId(), key.getAttempt(), key.getPluginType());
        this.writer.setClosedFileConsumer(new java.util.function.Consumer<SinkFileEntry>() {
            @Override
            public void accept(SinkFileEntry entry) {
                manager.publishFileEntry(entry);
            }
        });
    }

    /**
     * 序列化并落盘一条脏数据。返回 true 表示实际写入，false 表示因总量上限被丢弃。
     * FAIL_JOB 策略下超限时直接抛异常使 job 失败。
     */
    public boolean append(Record record, Throwable throwable, String errorMessage) {
        long seq = sequence.incrementAndGet();
        java.util.Map<String, Object> line = serializer.toLine(record, throwable, errorMessage, seq);
        String json = JSON.toJSONString(line);
        byte[] payload = json.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        long bytes = payload.length + 1L;

        // 全局总量计量（跨所有 channel）
        if (!manager.reserve(bytes)) {
            // 返回 false 表示丢弃；FAIL_JOB 的异常已在 reserve 内抛出
            return false;
        }

        try {
            writer.writeLine(json, seq);
            return true;
        } catch (RuntimeException e) {
            manager.release(bytes);
            throw e;
        }
    }

    public void flush() {
        writer.flush();
    }

    /**
     * 关闭活动文件并返回最后的清单条目（可能为 null）。
     */
    public SinkFileEntry close() {
        return writer.closeCurrentFile();
    }
}
