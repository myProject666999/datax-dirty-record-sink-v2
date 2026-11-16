package com.alibaba.datax.core.statistics.plugin.task.sink;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import org.junit.Test;

import java.util.Base64;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class DirtyRecordSerializerTest {

    private DirtySinkContext contextWithoutMask() {
        return DirtySinkContext.forReplay(42L,
                new java.io.File(System.getProperty("java.io.tmpdir")),
                "sink-test", 1024L, 10240L, null, 0);
    }

    private Record fullRecord() {
        Record record = new DefaultRecord();
        record.addColumn(new LongColumn(123L));
        record.addColumn(new StringColumn("hello"));
        record.addColumn(new DoubleColumn(1.5d));
        record.addColumn(new BoolColumn(true));
        record.addColumn(new DateColumn(1731740000000L));
        record.addColumn(new BytesColumn(new byte[]{0, 1, 2, (byte) 255}));
        record.addColumn(new StringColumn()); // null
        Map<String, String> meta = new HashMap<String, String>();
        meta.put("filePath", "mock://part-0");
        record.setMeta(meta);
        return record;
    }

    @Test
    public void testFieldCompleteness() {
        DirtyRecordSerializer serializer = new DirtyRecordSerializer(
                contextWithoutMask(), 7, 9, 2, PluginType.WRITER);
        RuntimeException ex = new RuntimeException("boom");
        Map<String, Object> line = serializer.toLine(fullRecord(), ex, "约束冲突", 5L);

        assertEquals(SinkConstants.LINE_VERSION, line.get(SinkConstants.FIELD_VERSION));
        assertEquals(42L, line.get(SinkConstants.FIELD_JOB_ID));
        assertEquals(7, line.get(SinkConstants.FIELD_TASK_GROUP_ID));
        assertEquals(9, line.get(SinkConstants.FIELD_TASK_ID));
        assertEquals(2, line.get(SinkConstants.FIELD_ATTEMPT));
        assertEquals("WRITER", line.get(SinkConstants.FIELD_PLUGIN_TYPE));
        assertEquals(5L, line.get(SinkConstants.FIELD_SEQUENCE));
        assertEquals("约束冲突", line.get(SinkConstants.FIELD_ERROR_MESSAGE));
        assertEquals("java.lang.RuntimeException", line.get(SinkConstants.FIELD_EXCEPTION_CLASS));
        assertEquals("boom", line.get(SinkConstants.FIELD_EXCEPTION_MESSAGE));
        assertEquals(7, line.get(SinkConstants.FIELD_COLUMN_COUNT));
        assertNotNull(line.get(SinkConstants.FIELD_META));
        assertEquals("mock://part-0",
                ((Map<?, ?>) line.get(SinkConstants.FIELD_META)).get("filePath"));

        @SuppressWarnings("unchecked")
        java.util.List<Map<String, Object>> columns =
                (java.util.List<Map<String, Object>>) line.get(SinkConstants.FIELD_COLUMNS);
        assertEquals(7, columns.size());
        Map<String, Object> longCol = columns.get(0);
        assertEquals("LONG", longCol.get(SinkConstants.COLUMN_TYPE));
        // LongColumn 的 rawData 内部为 BigInteger
        assertEquals(123, ((Number) longCol.get(SinkConstants.COLUMN_RAW_DATA)).intValue());
        assertEquals("STRING", columns.get(1).get(SinkConstants.COLUMN_TYPE));
        assertEquals("hello", columns.get(1).get(SinkConstants.COLUMN_RAW_DATA));
        assertEquals("BOOL", columns.get(3).get(SinkConstants.COLUMN_TYPE));
        assertEquals(Boolean.TRUE, columns.get(3).get(SinkConstants.COLUMN_RAW_DATA));

        Map<String, Object> dateCol = columns.get(4);
        assertEquals("DATE", dateCol.get(SinkConstants.COLUMN_TYPE));
        assertEquals(1731740000000L, dateCol.get(SinkConstants.COLUMN_RAW_DATA));
        assertEquals("epochMillis", dateCol.get(SinkConstants.COLUMN_ENCODING));

        Map<String, Object> bytesCol = columns.get(5);
        assertEquals("BYTES", bytesCol.get(SinkConstants.COLUMN_TYPE));
        byte[] decoded = Base64.getDecoder().decode((String) bytesCol.get(SinkConstants.COLUMN_RAW_DATA));
        assertArrayEquals(new byte[]{0, 1, 2, (byte) 255}, decoded);
        assertEquals("base64", bytesCol.get(SinkConstants.COLUMN_ENCODING));

        Map<String, Object> nullCol = columns.get(6);
        assertEquals("NULL", nullCol.get(SinkConstants.COLUMN_TYPE));
        assertNull(nullCol.get(SinkConstants.COLUMN_RAW_DATA));
    }

    @Test
    public void testReaderSideAndBlankFieldsOmitted() {
        DirtyRecordSerializer serializer = new DirtyRecordSerializer(
                contextWithoutMask(), 0, 1, 1, PluginType.READER);
        Record record = new DefaultRecord();
        record.addColumn(new StringColumn("x"));
        Map<String, Object> line = serializer.toLine(record, null, "  ", 1L);

        assertEquals("READER", line.get(SinkConstants.FIELD_PLUGIN_TYPE));
        assertFalse(line.containsKey(SinkConstants.FIELD_ERROR_MESSAGE));
        assertFalse(line.containsKey(SinkConstants.FIELD_EXCEPTION_CLASS));
        assertFalse(line.containsKey(SinkConstants.FIELD_EXCEPTION_MESSAGE));
        assertFalse(line.containsKey(SinkConstants.FIELD_META));
    }

    @Test
    public void testMaskingAppliedBeforeSerialization() {
        // 通过带脱敏规则的 job 配置构造 context
        String jobJson = "{"
                + "\"job\":{\"content\":[{\"reader\":{\"name\":\"r\",\"parameter\":{\"column\":[\"id_card\"]}},"
                + "\"writer\":{\"name\":\"w\",\"parameter\":{\"column\":[\"id_card\"]}}}],"
                + "\"setting\":{\"dirtyRecord\":{\"enable\":true,\"path\":\""
                + System.getProperty("java.io.tmpdir").replace('\\', '/') + "\","
                + "\"mask\":{\"rules\":[{\"columnName\":\"id_card\",\"strategy\":\"sha256\"}]}}}}}";
        com.alibaba.datax.common.util.Configuration conf =
                com.alibaba.datax.common.util.Configuration.from(jobJson);
        conf.set("core.container.job.id", 1L);
        DirtySinkContext context = DirtySinkContext.fromJob(conf);
        DirtyRecordSerializer serializer = new DirtyRecordSerializer(
                context, 0, 0, 1, PluginType.WRITER);

        Record record = new DefaultRecord();
        record.addColumn(new StringColumn("110101199001011234"));
        Map<String, Object> line = serializer.toLine(record, null, "msg", 1L);

        @SuppressWarnings("unchecked")
        java.util.List<Map<String, Object>> columns =
                (java.util.List<Map<String, Object>>) line.get(SinkConstants.FIELD_COLUMNS);
        Map<String, Object> masked = columns.get(0);
        assertEquals(Boolean.TRUE, masked.get(SinkConstants.COLUMN_MASKED));
        assertEquals("sha256", masked.get(SinkConstants.COLUMN_MASK_STRATEGY));
        assertEquals("STRING", masked.get(SinkConstants.COLUMN_TYPE));
        String digest = (String) masked.get(SinkConstants.COLUMN_RAW_DATA);
        assertEquals(64, digest.length());
        assertFalse(digest.contains("110101199001011234"));
        assertEquals(org.apache.commons.codec.digest.DigestUtils.sha256Hex("110101199001011234"),
                digest);
    }

    @Test
    public void testNullValueColumnNotMasked() {
        Map<Integer, ColumnMasker> maskers = new HashMap<Integer, ColumnMasker>();
        maskers.put(0, new ReplaceColumnMasker("***"));
        // 直接用 forReplay 无法注入 maskers，这里验证序列化器对 null 列不走 masker：
        // 通过 job 配置 + null 列验证
        String jobJson = "{\"job\":{\"content\":[{\"reader\":{\"name\":\"r\",\"parameter\":{\"column\":[\"phone\"]}},"
                + "\"writer\":{\"name\":\"w\",\"parameter\":{\"column\":[\"phone\"]}}}],"
                + "\"setting\":{\"dirtyRecord\":{\"enable\":true,\"path\":\""
                + System.getProperty("java.io.tmpdir").replace('\\', '/') + "\","
                + "\"mask\":{\"rules\":[{\"columnName\":\"phone\",\"strategy\":\"replace\",\"replacement\":\"***\"}]}}}}}";
        com.alibaba.datax.common.util.Configuration conf =
                com.alibaba.datax.common.util.Configuration.from(jobJson);
        conf.set("core.container.job.id", 1L);
        DirtySinkContext context = DirtySinkContext.fromJob(conf);
        DirtyRecordSerializer serializer = new DirtyRecordSerializer(
                context, 0, 0, 1, PluginType.READER);

        Record record = new DefaultRecord();
        record.addColumn(new StringColumn());
        Map<String, Object> line = serializer.toLine(record, null, "msg", 1L);
        @SuppressWarnings("unchecked")
        java.util.List<Map<String, Object>> columns =
                (java.util.List<Map<String, Object>>) line.get(SinkConstants.FIELD_COLUMNS);
        assertEquals("NULL", columns.get(0).get(SinkConstants.COLUMN_TYPE));
        assertNull(columns.get(0).get(SinkConstants.COLUMN_MASKED));
        assertTrue(context.getMaskedColumns().contains(0));
        assertEquals(Collections.singletonList(0), context.getMaskedColumns());
    }
}
