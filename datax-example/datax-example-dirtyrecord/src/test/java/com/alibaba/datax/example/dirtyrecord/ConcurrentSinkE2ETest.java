package com.alibaba.datax.example.dirtyrecord;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import com.alibaba.datax.core.statistics.plugin.task.sink.SinkConstants;
import com.alibaba.datax.core.statistics.plugin.task.sink.SinkManifest;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * 多 channel 并发写入：文件互不冲突、无串写，manifest 与磁盘文件对账一致，
 * 且小 fileMaxSize 会触发滚动。
 */
public class ConcurrentSinkE2ETest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testConcurrentAndRolling() throws Exception {
        String sinkName = "concurrent";
        String jobPath = DirtyE2EHelper.materializeJob(
                "concurrent-job.json", folder.getRoot().getAbsolutePath(), sinkName, null);
        DirtyE2EHelper.runJob(jobPath);

        File sinkDir = DirtyE2EHelper.sinkDir(folder.getRoot(), sinkName);
        List<File> files = DirtyE2EHelper.jsonlFiles(sinkDir);
        // 5 个 task 各有 reader/writer 两个 collector；600B 的小阈值在每条约 200B 的脏数据下
        // 必然产生滚动，文件数应超过 collector 总数(10)
        assertTrue("小fileMaxSize应触发滚动，实际文件数=" + files.size(), files.size() > 10);

        // 每行都是独立可解析 JSON，且按文件统计行数与 manifest 一致
        SinkManifest manifest = SinkManifest.read(
                new File(sinkDir, SinkConstants.MANIFEST_FILE_NAME));
        long totalLines = 0;
        for (File f : files) {
            List<String> lines = java.nio.file.Files.readAllLines(
                    f.toPath(), java.nio.charset.StandardCharsets.UTF_8);
            for (String line : lines) {
                JSONObject obj = JSON.parseObject(line);
                assertTrue(obj.containsKey("sequence"));
            }
            totalLines += lines.size();
        }
        // channel=5 切出 5 个 task，每个 task reader 15 条 + writer 15 条脏数据
        assertEquals(150, totalLines);
        assertEquals(totalLines, manifest.getTotalRecords());
        assertEquals(files.size(), manifest.getFiles().size());
        // 没有 .writing 残留
        File[] writing = sinkDir.listFiles(new java.io.FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().endsWith(SinkConstants.WRITING_SUFFIX);
            }
        });
        assertEquals(0, writing.length);
    }
}
