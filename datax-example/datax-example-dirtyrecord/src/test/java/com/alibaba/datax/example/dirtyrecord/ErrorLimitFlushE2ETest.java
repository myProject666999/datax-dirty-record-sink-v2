package com.alibaba.datax.example.dirtyrecord;

import com.alibaba.datax.core.statistics.plugin.task.sink.SinkConstants;
import com.alibaba.datax.core.statistics.plugin.task.sink.SinkFileEntry;
import com.alibaba.datax.core.statistics.plugin.task.sink.SinkManifest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * errorLimit.record=0：第一条脏数据即让 job 失败，但已落盘内容必须完整：
 * manifest 与 _FAILED 齐全，所有 jsonl 可完整解析，无 .writing 半文件。
 */
public class ErrorLimitFlushE2ETest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testFailureLeavesCompleteSink() throws Exception {
        String sinkName = "error-limit";
        String jobPath = DirtyE2EHelper.materializeJob(
                "error-limit-job.json", folder.getRoot().getAbsolutePath(), sinkName, null);

        boolean failed = false;
        try {
            DirtyE2EHelper.runJob(jobPath);
        } catch (Exception e) {
            failed = true;
        }
        assertTrue("errorLimit.record=0 时 job 必须失败", failed);

        File sinkDir = DirtyE2EHelper.sinkDir(folder.getRoot(), sinkName);
        assertTrue(new File(sinkDir, SinkConstants.FAILED_MARKER).exists());
        assertTrue(new File(sinkDir, SinkConstants.MANIFEST_FILE_NAME).exists());

        SinkManifest manifest = SinkManifest.read(
                new File(sinkDir, SinkConstants.MANIFEST_FILE_NAME));
        assertEquals(SinkConstants.JOB_STATUS_FAILED, manifest.getJobStatus());
        // manifest 中每个文件都能逐行解析，没有半行
        for (SinkFileEntry entry : manifest.getFiles()) {
            File f = new File(sinkDir, entry.getFileName());
            for (String line : java.nio.file.Files.readAllLines(
                    f.toPath(), java.nio.charset.StandardCharsets.UTF_8)) {
                com.alibaba.fastjson2.JSON.parseObject(line);
            }
        }
        File[] writing = sinkDir.listFiles(new java.io.FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().endsWith(SinkConstants.WRITING_SUFFIX);
            }
        });
        assertTrue("失败后不得残留 .writing 半文件", writing == null || writing.length == 0);
    }
}
