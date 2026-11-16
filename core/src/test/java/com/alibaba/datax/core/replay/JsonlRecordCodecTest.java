package com.alibaba.datax.core.replay;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.core.statistics.plugin.task.sink.SinkConstants;
import com.alibaba.fastjson2.JSONObject;
import org.junit.Test;

import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class JsonlRecordCodecTest {

    private final JsonlRecordCodec codec = new JsonlRecordCodec();

    private JSONObject column(int index, String type, Object rawData) {
        JSONObject element = new JSONObject();
        element.put(SinkConstants.COLUMN_INDEX, index);
        element.put(SinkConstants.COLUMN_TYPE, type);
        element.put(SinkConstants.COLUMN_RAW_DATA, rawData);
        return element;
    }

    @Test
    public void testRoundTripAllColumnTypes() {
        JSONObject line = new JSONObject();
        line.put(SinkConstants.FIELD_VERSION, 1);
        line.put(SinkConstants.FIELD_PLUGIN_TYPE, "WRITER");
        line.put(SinkConstants.FIELD_COLUMN_COUNT, 8);
        java.util.List<JSONObject> columns = new java.util.ArrayList<JSONObject>();
        columns.add(column(0, "LONG", 123L));
        columns.add(column(1, "INT", 7));
        columns.add(column(2, "DOUBLE", 2.5d));
        columns.add(column(3, "BOOL", true));
        columns.add(column(4, "STRING", "text"));
        columns.add(column(5, "DATE", 1731740000000L));
        columns.add(column(6, "BYTES", Base64.getEncoder().encodeToString(new byte[]{1, 2, 3})));
        columns.add(column(7, "NULL", null));
        line.put(SinkConstants.FIELD_COLUMNS, columns);
        Map<String, String> meta = new HashMap<String, String>();
        meta.put("filePath", "/a/b.csv");
        line.put(SinkConstants.FIELD_META, new JSONObject(meta));

        Record record = codec.toRecord(line);
        assertEquals(8, record.getColumnNumber());

        Column longCol = record.getColumn(0);
        assertEquals(Column.Type.LONG, longCol.getType());
        assertEquals(123, ((Number) longCol.getRawData()).intValue());
        // DataX 中 INT 与 LONG 都由 LongColumn 承载（Column.Type.LONG）
        assertEquals(Column.Type.LONG, record.getColumn(1).getType());
        assertEquals(7, ((Number) record.getColumn(1).getRawData()).intValue());
        assertEquals(Column.Type.DOUBLE, record.getColumn(2).getType());
        assertEquals(2.5d, record.getColumn(2).asDouble(), 0.0001d);
        assertEquals(Boolean.TRUE, ((BoolColumn) record.getColumn(3)).getRawData());
        assertEquals("text", record.getColumn(4).asString());

        DateColumn dateCol = (DateColumn) record.getColumn(5);
        assertEquals(1731740000000L, dateCol.asLong().longValue());

        BytesColumn bytesCol = (BytesColumn) record.getColumn(6);
        assertArrayEquals(new byte[]{1, 2, 3}, bytesCol.asBytes());

        Column nullCol = record.getColumn(7);
        // DataX 无 NullColumn 实现，NULL 落盘类型还原为 rawData=null 的空 StringColumn
        assertNull(nullCol.getRawData());

        assertEquals("/a/b.csv", record.getMeta().get("filePath"));
    }

    @Test
    public void testBadTypeFallsBackToString() {
        JSONObject line = new JSONObject();
        line.put(SinkConstants.FIELD_VERSION, 1);
        line.put(SinkConstants.FIELD_PLUGIN_TYPE, "READER");
        line.put(SinkConstants.FIELD_COLUMN_COUNT, 1);
        java.util.List<JSONObject> columns = new java.util.ArrayList<JSONObject>();
        columns.add(column(0, "BAD", "unparseable-token"));
        line.put(SinkConstants.FIELD_COLUMNS, columns);

        Record record = codec.toRecord(line);
        assertEquals(StringColumn.class, record.getColumn(0).getClass());
        assertEquals("unparseable-token", record.getColumn(0).asString());
    }

    @Test
    public void testMaskedColumnReplaysAsStoredString() {
        JSONObject line = new JSONObject();
        line.put(SinkConstants.FIELD_VERSION, 1);
        line.put(SinkConstants.FIELD_PLUGIN_TYPE, "WRITER");
        line.put(SinkConstants.FIELD_COLUMN_COUNT, 1);
        JSONObject masked = column(0, "STRING", "a1b2c3digest");
        masked.put(SinkConstants.COLUMN_MASKED, true);
        masked.put(SinkConstants.COLUMN_MASK_STRATEGY, "sha256");
        java.util.List<JSONObject> columns = new java.util.ArrayList<JSONObject>();
        columns.add(masked);
        line.put(SinkConstants.FIELD_COLUMNS, columns);

        Record record = codec.toRecord(line);
        assertEquals("a1b2c3digest", record.getColumn(0).asString());
    }

    @Test
    public void testNullConstructorsExist() {
        // 确认各类空值列构造行为符合回放预期
        assertEquals(Column.Type.LONG, new LongColumn().getType());
        assertEquals(Column.Type.DOUBLE, new DoubleColumn().getType());
        assertEquals(Column.Type.DATE, new DateColumn().getType());
        assertEquals(Column.Type.BYTES, new BytesColumn().getType());
    }
}
