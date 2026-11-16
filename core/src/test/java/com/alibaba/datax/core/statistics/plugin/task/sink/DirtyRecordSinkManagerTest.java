package com.alibaba.datax.core.statistics.plugin.task.sink;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class DirtyRecordSinkManagerTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @After
    public void clearRegistry() {
        DirtyRecordSinkManager.remove(100L);
        DirtyRecordSinkManager.remove(101L);
    }

    private DirtySinkContext context(long jobId, long cap, String policy) {
        return new DirtySinkContext(jobId, folder.getRoot(),
                "sink-" + jobId + "-" + System.nanoTime(),
                64L * 1024L, cap, policy, true,
                java.util.Collections.<Integer, ColumnMasker>emptyMap(), null, 0);
    }

    private Record oneRecord(long value) {
        Record record = new DefaultRecord();
        record.addColumn(new LongColumn(value));
        return record;
    }

    @Test
    public void testStopRecordDropsAfterCap() {
        DirtySinkContext ctx = context(100L, 300L, SinkConstants.OVER_CAP_POLICY_STOP_RECORD);
        DirtyRecordSinkManager.register(ctx);
        DirtyRecordSinkManager manager = DirtyRecordSinkManager.getOrNull(100L);
        SinkChannel channel = manager.acquire(0, 0, 1, PluginType.WRITER);

        int written = 0;
        int dropped = 0;
        for (int i = 0; i < 200; i++) {
            if (channel.append(oneRecord(i), null, "msg")) {
                written++;
            } else {
                dropped++;
            }
        }
        assertTrue("应存在被丢弃的记录", dropped > 0);
        assertTrue(manager.getDroppedRecords() > 0);
        manager.close(true);

        SinkManifest manifest = SinkManifest.read(
                new File(ctx.getSinkDir(), SinkConstants.MANIFEST_FILE_NAME));
        assertTrue(manifest.isCapReached());
        assertEquals(dropped, manifest.getDroppedRecords());
        assertEquals(written, manifest.getTotalRecords());
        assertTrue(new File(ctx.getSinkDir(), SinkConstants.SUCCESS_MARKER).exists());
    }

    @Test
    public void testFailJobThrowsOnCap() {
        DirtySinkContext ctx = context(101L, 200L, SinkConstants.OVER_CAP_POLICY_FAIL_JOB);
        DirtyRecordSinkManager.register(ctx);
        DirtyRecordSinkManager manager = DirtyRecordSinkManager.getOrNull(101L);
        SinkChannel channel = manager.acquire(0, 0, 1, PluginType.READER);
        try {
            for (int i = 0; i < 200; i++) {
                channel.append(oneRecord(i), null, "msg");
            }
            fail("FAIL_JOB 策略下超总量应抛异常");
        } catch (DataXException e) {
            assertEquals(FrameworkErrorCode.DIRTY_RECORD_SINK_CAP_EXCEEDED.getCode(),
                    e.getErrorCode().getCode());
        }
        manager.close(false);
        assertTrue(new File(ctx.getSinkDir(), SinkConstants.FAILED_MARKER).exists());
    }

    @Test
    public void testConcurrentChannelsNoConflictAndTotalsReconcile() throws Exception {
        // 给足上限，保证全部写入，重点验证多线程并发不串写、文件互不冲突
        DirtySinkContext ctx = context(100L, 1024L * 1024L * 10L,
                SinkConstants.OVER_CAP_POLICY_STOP_RECORD);
        DirtyRecordSinkManager.register(ctx);
        final DirtyRecordSinkManager manager = DirtyRecordSinkManager.getOrNull(100L);

        int threadCount = 8;
        int perThread = 100;
        CountDownLatch start = new CountDownLatch(1);
        CountDownLatch done = new CountDownLatch(threadCount);
        final AtomicInteger failures = new AtomicInteger(0);

        for (int t = 0; t < threadCount; t++) {
            final int tgId = t;
            new Thread(new Runnable() {
                @Override
                public void run() {
                    try {
                        start.await();
                        // 每个线程模拟一个独立 task 的 reader/writer 两个 collector
                        SinkChannel reader = manager.acquire(tgId, tgId, 1, PluginType.READER);
                        SinkChannel writer = manager.acquire(tgId, tgId, 1, PluginType.WRITER);
                        for (int i = 0; i < perThread; i++) {
                            reader.append(oneRecord(i), null, "r");
                            writer.append(oneRecord(i), null, "w");
                        }
                    } catch (Throwable e) {
                        failures.incrementAndGet();
                    } finally {
                        done.countDown();
                    }
                }
            }).start();
        }
        start.countDown();
        done.await();

        assertEquals(0, failures.get());
        manager.close(true);

        SinkManifest manifest = SinkManifest.read(
                new File(ctx.getSinkDir(), SinkConstants.MANIFEST_FILE_NAME));
        // 每个 task 两个 channel，共 threadCount*2 个文件（每个 channel 数据量未超 fileMaxSize）
        assertEquals(threadCount * 2, manifest.getFiles().size());
        assertEquals(threadCount * 2L * perThread, manifest.getTotalRecords());

        // 逐文件校验：每行都是独立完整 JSON，行数与 manifest 一致
        for (SinkFileEntry entry : manifest.getFiles()) {
            File f = new File(ctx.getSinkDir(), entry.getFileName());
            java.util.List<String> lines = java.nio.file.Files.readAllLines(
                    f.toPath(), java.nio.charset.StandardCharsets.UTF_8);
            assertEquals(entry.getRecords(), lines.size());
            for (String line : lines) {
                assertNotNull(com.alibaba.fastjson2.JSON.parseObject(line));
            }
        }
    }

    @Test
    public void testCloseIsIdempotent() {
        DirtySinkContext ctx = context(100L, 1024L, SinkConstants.OVER_CAP_POLICY_STOP_RECORD);
        DirtyRecordSinkManager.register(ctx);
        DirtyRecordSinkManager manager = DirtyRecordSinkManager.getOrNull(100L);
        manager.acquire(0, 0, 1, PluginType.WRITER).append(oneRecord(1), null, "m");
        manager.close(true);
        manager.close(true); // 重复关闭不抛异常
        assertTrue(manager.isClosed());
    }
}
