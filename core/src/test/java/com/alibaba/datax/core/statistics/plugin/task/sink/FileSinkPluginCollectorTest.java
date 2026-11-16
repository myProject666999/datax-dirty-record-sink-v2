package com.alibaba.datax.core.statistics.plugin.task.sink;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.communication.CommunicationTool;
import com.alibaba.datax.core.statistics.plugin.task.FileSinkPluginCollector;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileSinkPluginCollectorTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @After
    public void clearRegistry() {
        DirtyRecordSinkManager.remove(1L);
    }

    private Record record(long value) {
        Record r = new DefaultRecord();
        r.addColumn(new LongColumn(value));
        return r;
    }

    @Test
    public void testDisabledCreatesNoFilesAndCountersStillBump() {
        Configuration conf = Configuration.newDefault();
        conf.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, 1L);
        Communication communication = new Communication();
        FileSinkPluginCollector collector = new FileSinkPluginCollector(
                conf, communication, PluginType.WRITER);

        collector.collectDirtyRecord(record(1), new RuntimeException("x"), "msg");
        collector.collectDirtyRecord(record(2), null, "msg2");

        File[] files = folder.getRoot().listFiles();
        assertEquals("关闭落盘时不得创建任何文件", 0L, (long) files.length);
        assertEquals(2L, (long) communication.getLongCounter(CommunicationTool.WRITE_FAILED_RECORDS));
    }

    @Test
    public void testEnabledWritesReaderAndWriterFilesAndBumpsCounters() {
        Configuration conf = Configuration.newDefault();
        conf.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, 1L);
        conf.set(CoreConstant.DATAX_JOB_SETTING_DIRTY_RECORD_ENABLE, true);
        conf.set(CoreConstant.DATAX_JOB_SETTING_DIRTY_RECORD_PATH,
                folder.getRoot().getAbsolutePath());
        conf.set(CoreConstant.DATAX_JOB_SETTING_DIRTY_RECORD_SINK_NAME, "collector-it");
        DirtySinkContext ctx = DirtySinkContext.fromJob(conf);
        DirtyRecordSinkManager.register(ctx);

        Communication readerComm = new Communication();
        Communication writerComm = new Communication();
        FileSinkPluginCollector readerCollector = new FileSinkPluginCollector(
                conf, readerComm, PluginType.READER);
        FileSinkPluginCollector writerCollector = new FileSinkPluginCollector(
                conf, writerComm, PluginType.WRITER);
        TaskContextHolder.bind(readerCollector, 0, 3, 1);
        TaskContextHolder.bind(writerCollector, 0, 3, 1);

        readerCollector.collectDirtyRecord(record(1), null, "reader dirty");
        writerCollector.collectDirtyRecord(record(2), new RuntimeException("w"), "writer dirty");
        // null record 在父类中直接 warn+return，不计数、不落盘
        writerCollector.collectDirtyRecord(null, null, "null record");

        assertEquals(1L, (long) readerComm.getLongCounter(CommunicationTool.READ_FAILED_RECORDS));
        assertEquals(1L, (long) writerComm.getLongCounter(CommunicationTool.WRITE_FAILED_RECORDS));

        DirtyRecordSinkManager manager = DirtyRecordSinkManager.getOrNull(1L);
        manager.close(true);

        File sinkDir = new File(folder.getRoot(), "collector-it");
        File[] jsonl = sinkDir.listFiles(new java.io.FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().endsWith(SinkConstants.JSONL_SUFFIX);
            }
        });
        assertEquals(2, jsonl.length);
        boolean hasReaderFile = false;
        boolean hasWriterFile = false;
        for (File f : jsonl) {
            hasReaderFile |= f.getName().contains("-reader.");
            hasWriterFile |= f.getName().contains("-writer.");
        }
        assertTrue(hasReaderFile);
        assertTrue(hasWriterFile);
        assertTrue(new File(sinkDir, SinkConstants.SUCCESS_MARKER).exists());
        assertTrue(new File(sinkDir, SinkConstants.MANIFEST_FILE_NAME).exists());
    }

    @Test
    public void testDisabledFlagIsFalseByDefault() {
        Configuration conf = Configuration.newDefault();
        assertFalse(DirtySinkContext.isEnabled(conf));
        conf.set(CoreConstant.DATAX_JOB_SETTING_DIRTY_RECORD_ENABLE, false);
        assertFalse(DirtySinkContext.isEnabled(conf));
        conf.set(CoreConstant.DATAX_JOB_SETTING_DIRTY_RECORD_ENABLE, true);
        assertTrue(DirtySinkContext.isEnabled(conf));
    }
}
