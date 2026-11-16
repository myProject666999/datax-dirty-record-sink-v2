package com.alibaba.datax.example.dirtyrecord;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * 按列名(sha256)与按列下标(replace)脱敏：原文不出现在任何落盘字节中，脱敏标记与策略落盘。
 */
public class MaskingE2ETest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testMaskingIrreversibleOnDisk() throws Exception {
        String sinkName = "masking";
        String jobPath = DirtyE2EHelper.materializeJob(
                "masking-job.json", folder.getRoot().getAbsolutePath(), sinkName, null);
        DirtyE2EHelper.runJob(jobPath);

        File sinkDir = DirtyE2EHelper.sinkDir(folder.getRoot(), sinkName);
        // 原文 name-0/name-1/name-2 属于 secret 列，落盘后不得出现
        assertFalse("secret 列原文不得落盘", DirtyE2EHelper.anyLineContains(sinkDir, "name-0"));
        assertFalse(DirtyE2EHelper.anyLineContains(sinkDir, "name-1"));

        boolean sawSha256 = false;
        boolean sawReplace = false;
        for (String line : DirtyE2EHelper.allJsonlLines(sinkDir)) {
            JSONObject obj = JSON.parseObject(line);
            JSONArray columns = obj.getJSONArray("columns");
            // 下标 0：replace 为 ###
            JSONObject idCol = columns.getJSONObject(0);
            assertEquals(Boolean.TRUE, idCol.getBoolean("masked"));
            assertEquals("replace", idCol.getString("maskStrategy"));
            assertEquals("###", idCol.getString("rawData"));
            sawReplace = true;

            // 下标 1：sha256 64位 hex，且不含原文
            JSONObject secretCol = columns.getJSONObject(1);
            assertEquals(Boolean.TRUE, secretCol.getBoolean("masked"));
            assertEquals("sha256", secretCol.getString("maskStrategy"));
            String digest = secretCol.getString("rawData");
            assertEquals(64, digest.length());
            assertFalse(digest.contains("name-"));
            sawSha256 = true;
        }
        assertTrue(sawSha256);
        assertTrue(sawReplace);
    }
}
