package com.alibaba.datax.example.dirtyrecord;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * 默认关闭（无 job.setting.dirtyRecord 配置）：不产生任何 sink 目录，
 * 作业正常完成，行为与历史版本一致。
 */
public class DefaultOffE2ETest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testNoSinkWhenDisabled() throws Exception {
        String jobPath = DirtyE2EHelper.materializeJob(
                "default-off-job.json", folder.getRoot().getAbsolutePath(), "should-not-exist", null);
        DirtyE2EHelper.runJob(jobPath);

        File sinkDir = DirtyE2EHelper.sinkDir(folder.getRoot(), "should-not-exist");
        assertFalse("关闭落盘时不得创建 sink 目录", sinkDir.exists());
        File[] anyFiles = folder.getRoot().listFiles();
        assertEquals(0, anyFiles == null ? 0 : anyFiles.length);
    }
}
