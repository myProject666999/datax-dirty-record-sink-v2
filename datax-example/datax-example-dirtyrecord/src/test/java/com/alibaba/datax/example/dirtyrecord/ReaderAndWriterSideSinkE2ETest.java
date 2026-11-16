package com.alibaba.datax.example.dirtyrecord;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * reader 与 writer 两侧脏数据都能落盘，且字段完整（含来源 meta）。
 */
public class ReaderAndWriterSideSinkE2ETest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testBothSidesSinkWithCompleteFields() throws Exception {
        String sinkName = "both-sides";
        String jobPath = DirtyE2EHelper.materializeJob(
                "basic-sink-job.json", folder.getRoot().getAbsolutePath(), sinkName, null);
        DirtyE2EHelper.runJob(jobPath);

        File sinkDir = DirtyE2EHelper.sinkDir(folder.getRoot(), sinkName);
        assertTrue(new File(sinkDir, "_SUCCESS").exists());
        assertTrue(new File(sinkDir, "manifest.json").exists());

        List<File> files = DirtyE2EHelper.jsonlFiles(sinkDir);
        boolean readerFile = false;
        boolean writerFile = false;
        int total = 0;
        for (File f : files) {
            readerFile |= f.getName().contains("-reader.");
            writerFile |= f.getName().contains("-writer.");
        }
        assertTrue("应存在 reader 侧脏数据文件", readerFile);
        assertTrue("应存在 writer 侧脏数据文件", writerFile);

        boolean sawReaderError = false;
        boolean sawWriterError = false;
        boolean sawMeta = false;
        for (String line : DirtyE2EHelper.allJsonlLines(sinkDir)) {
            JSONObject obj = JSON.parseObject(line);
            assertNotNull(obj.get("version"));
            assertNotNull(obj.get("jobId"));
            assertNotNull(obj.get("taskGroupId"));
            assertNotNull(obj.get("taskId"));
            assertNotNull(obj.get("attempt"));
            assertNotNull(obj.get("timestamp"));
            assertNotNull(obj.get("sequence"));
            assertNotNull(obj.get("errorMessage"));
            assertNotNull(obj.get("exceptionClass"));
            JSONArray columns = obj.getJSONArray("columns");
            assertTrue(columns.size() == 2);
            String type = obj.getString("pluginType");
            if ("READER".equals(type)) {
                sawReaderError = true;
            }
            if ("WRITER".equals(type)) {
                sawWriterError = true;
            }
            if (obj.containsKey("meta")
                    && "mock://part-0".equals(obj.getJSONObject("meta").getString("filePath"))) {
                sawMeta = true;
            }
            total++;
        }
        // reader 2 条 + writer 2 条
        assertEquals(4, total);
        assertTrue(sawReaderError);
        assertTrue(sawWriterError);
        assertTrue("来源信息(meta.filePath)应落盘", sawMeta);
    }
}
