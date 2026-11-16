package com.alibaba.datax.example.dirtyrecord;

import com.alibaba.datax.core.statistics.plugin.task.sink.SinkConstants;
import com.alibaba.datax.core.statistics.plugin.task.sink.SinkManifest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;

import static org.junit.Assert.assertTrue;

/**
 * 总量上限两种策略：STOP_RECORD 丢弃后续记录但 job 成功；FAIL_JOB 直接让 job 失败。
 */
public class RollingAndCapE2ETest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private String materializeCapJob(String sinkName, String totalCap, String policy)
            throws Exception {
        File template = new File(DirtyE2EHelper.resourcePath("cap-job.json"));
        String content = new String(Files.readAllBytes(template.toPath()), StandardCharsets.UTF_8)
                .replace("${SINK_ROOT}", folder.getRoot().getAbsolutePath().replace("\\", "/"))
                .replace("${SINK_NAME}", sinkName)
                .replace("${TOTAL_CAP}", totalCap)
                .replace("${POLICY}", policy);
        File jobFile = File.createTempFile("cap-job-", ".json");
        jobFile.deleteOnExit();
        Files.write(jobFile.toPath(), content.getBytes(StandardCharsets.UTF_8));
        return jobFile.getAbsolutePath();
    }

    @Test
    public void testStopRecordPolicy() throws Exception {
        String sinkName = "cap-stop";
        String jobPath = materializeCapJob(sinkName, "500", SinkConstants.OVER_CAP_POLICY_STOP_RECORD);
        DirtyE2EHelper.runJob(jobPath); // 不应抛异常

        File sinkDir = DirtyE2EHelper.sinkDir(folder.getRoot(), sinkName);
        SinkManifest manifest = SinkManifest.read(
                new File(sinkDir, SinkConstants.MANIFEST_FILE_NAME));
        assertTrue("STOP_RECORD 下应出现被丢弃记录", manifest.getDroppedRecords() > 0);
        assertTrue(manifest.isCapReached());
        assertTrue(new File(sinkDir, SinkConstants.SUCCESS_MARKER).exists());
        // 落盘总量不超过上限（每行字节均被计入）
        assertTrue(manifest.getTotalBytes() <= 500 + 1024);
    }

    @Test
    public void testFailJobPolicy() throws Exception {
        String sinkName = "cap-fail";
        String jobPath = materializeCapJob(sinkName, "300", SinkConstants.OVER_CAP_POLICY_FAIL_JOB);
        boolean failed = false;
        try {
            DirtyE2EHelper.runJob(jobPath);
        } catch (Exception e) {
            failed = true;
            assertTrue(e.getMessage().contains("Framework-21")
                    || rootCauseContains(e, "totalSizeCap"));
        }
        assertTrue("FAIL_JOB 策略下超总量应使 job 失败", failed);

        File sinkDir = DirtyE2EHelper.sinkDir(folder.getRoot(), sinkName);
        // 即使 job 失败，manifest 与 _FAILED 必须完整写出
        assertTrue(new File(sinkDir, SinkConstants.FAILED_MARKER).exists());
        assertTrue(new File(sinkDir, SinkConstants.MANIFEST_FILE_NAME).exists());
    }

    private boolean rootCauseContains(Throwable t, String token) {
        while (t != null) {
            if (t.getMessage() != null && t.getMessage().contains(token)) {
                return true;
            }
            t = t.getCause();
        }
        return false;
    }
}
