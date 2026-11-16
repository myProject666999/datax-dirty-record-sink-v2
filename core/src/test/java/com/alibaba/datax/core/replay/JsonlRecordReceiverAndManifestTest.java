package com.alibaba.datax.core.replay;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.statistics.plugin.task.sink.SinkConstants;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class JsonlRecordReceiverAndManifestTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private String validLine(long seq, String value) {
        return "{\"version\":1,\"jobId\":1,\"taskGroupId\":0,\"taskId\":0,\"attempt\":1,"
                + "\"pluginType\":\"WRITER\",\"sequence\":" + seq + ",\"columnCount\":1,"
                + "\"columns\":[{\"index\":0,\"type\":\"STRING\",\"rawData\":\"" + value + "\"}]}";
    }

    private File writeFile(String name, String... lines) throws Exception {
        File f = new File(folder.getRoot(), name);
        StringBuilder sb = new StringBuilder();
        for (String line : lines) {
            sb.append(line).append('\n');
        }
        Files.write(f.toPath(), sb.toString().getBytes(StandardCharsets.UTF_8));
        return f;
    }

    @Test
    public void testReadsMultipleFilesInNameOrder() throws Exception {
        File f1 = writeFile("dirty-j1-tg0-t0-a1-writer.000001.jsonl",
                validLine(1, "a"), validLine(2, "b"));
        File f2 = writeFile("dirty-j1-tg0-t0-a1-writer.000002.jsonl",
                validLine(3, "c"));
        JsonlRecordReceiver receiver = new JsonlRecordReceiver(
                new ArrayList<File>(Arrays.asList(f1, f2)));

        Record r1 = receiver.getFromReader();
        Record r2 = receiver.getFromReader();
        Record r3 = receiver.getFromReader();
        Record r4 = receiver.getFromReader();
        assertEquals("a", r1.getColumn(0).asString());
        assertEquals("b", r2.getColumn(0).asString());
        assertEquals("c", r3.getColumn(0).asString());
        assertNull(r4);
        receiver.shutdown();
    }

    @Test
    public void testTruncatedLastLineReportsFileLineAndOffset() throws Exception {
        File f = writeFile("dirty.000001.jsonl",
                validLine(1, "ok"),
                "{\"version\":1,\"columns\":["); // 截断的坏 JSON
        JsonlRecordReceiver receiver = new JsonlRecordReceiver(
                new ArrayList<File>(java.util.Collections.singletonList(f)));
        assertNotNull(receiver.getFromReader());
        try {
            receiver.getFromReader();
            fail("截断行应报错");
        } catch (DataXException e) {
            assertEquals(FrameworkErrorCode.REPLAY_FILE_CORRUPT.getCode(),
                    e.getErrorCode().getCode());
            assertTrue(e.getMessage().contains(f.getName()));
            assertTrue(e.getMessage().contains("line=2"));
            assertTrue(e.getMessage().contains("byteOffset="));
        }
    }

    @Test
    public void testBlankLineRejected() throws Exception {
        File f = writeFile("dirty.000001.jsonl", validLine(1, "ok"), "   ");
        JsonlRecordReceiver receiver = new JsonlRecordReceiver(
                new ArrayList<File>(java.util.Collections.singletonList(f)));
        receiver.getFromReader();
        try {
            receiver.getFromReader();
            fail("空行应报错");
        } catch (DataXException e) {
            assertTrue(e.getMessage().contains("空行"));
        }
    }

    @Test
    public void testMissingRequiredFieldRejected() throws Exception {
        File f = writeFile("dirty.000001.jsonl",
                "{\"version\":1,\"columns\":[]}"); // 缺 pluginType
        JsonlRecordReceiver receiver = new JsonlRecordReceiver(
                new ArrayList<File>(java.util.Collections.singletonList(f)));
        try {
            receiver.getFromReader();
            fail("缺字段应报错");
        } catch (DataXException e) {
            assertTrue(e.getMessage().contains("pluginType"));
        }
    }

    @Test
    public void testDuplicateReplayBlockedAndForceOverrides() throws Exception {
        writeFile("dirty.000001.jsonl", validLine(1, "a"));
        Files.write(new File(folder.getRoot(), SinkConstants.REPLAYED_MARKER).toPath(),
                "123".getBytes(StandardCharsets.UTF_8));

        ReplayManifestReader reader = new ReplayManifestReader();
        try {
            reader.prepare(folder.getRoot(), false, false);
            fail("存在_REPLAYED且未force时应拒绝回放");
        } catch (DataXException e) {
            assertEquals(FrameworkErrorCode.REPLAY_DUPLICATE_SINK.getCode(),
                    e.getErrorCode().getCode());
        }
        // force 下继续（manifest 缺失，再用 allowManifestMissing 放行）
        ReplayManifestReader.ReplayPlan plan = reader.prepare(folder.getRoot(), true, true);
        assertEquals(1, plan.getOrderedFiles().size());
    }

    @Test
    public void testManifestMissingGate() throws Exception {
        writeFile("dirty.000001.jsonl", validLine(1, "a"));
        ReplayManifestReader reader = new ReplayManifestReader();
        try {
            reader.prepare(folder.getRoot(), false, false);
            fail("无manifest且未显式允许时应报错");
        } catch (DataXException e) {
            assertEquals(FrameworkErrorCode.REPLAY_MANIFEST_ERROR.getCode(),
                    e.getErrorCode().getCode());
        }
        ReplayManifestReader.ReplayPlan plan = reader.prepare(folder.getRoot(), false, true);
        assertEquals(1, plan.getOrderedFiles().size());
        assertNull(plan.getManifest());
    }

    @Test
    public void testWritingResidueRejected() throws Exception {
        writeFile("dirty.000001.jsonl", validLine(1, "a"));
        writeFile("dirty.000002.jsonl.writing", validLine(2, "b"));
        try {
            new ReplayManifestReader().prepare(folder.getRoot(), false, true);
            fail(".writing 残留应报错");
        } catch (DataXException e) {
            assertEquals(FrameworkErrorCode.REPLAY_FILE_CORRUPT.getCode(),
                    e.getErrorCode().getCode());
            assertTrue(e.getMessage().contains(".writing"));
        }
    }

    @Test
    public void testChecksumMismatchRejected() throws Exception {
        File dataFile = writeFile("dirty.000001.jsonl", validLine(1, "a"));
        // 手工构造一个 sha256 错误的 manifest
        String manifest = "{\"version\":1,\"jobId\":1,\"sinkName\":\"s\",\"createdAt\":1,"
                + "\"closedAt\":2,\"jobStatus\":\"FAILED\",\"totalFiles\":1,\"totalRecords\":1,"
                + "\"totalBytes\":" + dataFile.length() + ",\"totalSizeCapBytes\":9999,"
                + "\"capReached\":false,\"droppedRecords\":0,\"maskedColumns\":[],"
                + "\"files\":[{\"fileName\":\"dirty.000001.jsonl\",\"records\":1,\"bytes\":"
                + dataFile.length() + ",\"sha256\":\"deadbeef\",\"firstSequence\":1,"
                + "\"lastSequence\":1}]}";
        Files.write(new File(folder.getRoot(), SinkConstants.MANIFEST_FILE_NAME).toPath(),
                manifest.getBytes(StandardCharsets.UTF_8));
        try {
            new ReplayManifestReader().prepare(folder.getRoot(), false, false);
            fail("sha256 不一致应报错");
        } catch (DataXException e) {
            assertEquals(FrameworkErrorCode.REPLAY_MANIFEST_ERROR.getCode(),
                    e.getErrorCode().getCode());
            assertTrue(e.getMessage().contains("sha256"));
        }
    }

    @Test
    public void testValidManifestChecksumPasses() throws Exception {
        File dataFile = writeFile("dirty.000001.jsonl", validLine(1, "a"));
        String sha = org.apache.commons.codec.digest.DigestUtils.sha256Hex(
                new java.io.FileInputStream(dataFile));
        String manifest = "{\"version\":1,\"jobId\":1,\"sinkName\":\"s\",\"createdAt\":1,"
                + "\"closedAt\":2,\"jobStatus\":\"SUCCESS\",\"totalFiles\":1,\"totalRecords\":1,"
                + "\"totalBytes\":" + dataFile.length() + ",\"totalSizeCapBytes\":9999,"
                + "\"capReached\":false,\"droppedRecords\":0,\"maskedColumns\":[],"
                + "\"files\":[{\"fileName\":\"dirty.000001.jsonl\",\"records\":1,\"bytes\":"
                + dataFile.length() + ",\"sha256\":\"" + sha + "\",\"firstSequence\":1,"
                + "\"lastSequence\":1}]}";
        Files.write(new File(folder.getRoot(), SinkConstants.MANIFEST_FILE_NAME).toPath(),
                manifest.getBytes(StandardCharsets.UTF_8));
        ReplayManifestReader.ReplayPlan plan =
                new ReplayManifestReader().prepare(folder.getRoot(), false, false);
        assertEquals(1, plan.getOrderedFiles().size());
        assertNotNull(plan.getManifest());
        List<String> disk = new ArrayList<String>();
        for (File f : plan.getOrderedFiles()) {
            disk.add(f.getName());
        }
        assertTrue(disk.contains("dirty.000001.jsonl"));
    }
}
