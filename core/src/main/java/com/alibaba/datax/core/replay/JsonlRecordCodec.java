package com.alibaba.datax.core.replay;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.BytesColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.statistics.plugin.task.sink.SinkConstants;
import com.alibaba.datax.core.transport.record.DefaultRecord;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.fastjson2.JSONArray;
import com.alibaba.fastjson2.JSONObject;

import java.util.Base64;
import java.util.Map;

/**
 * 把一条 JSONL 行 Map 还原为 DataX Record。
 * DATE 按 epochMillis 还原，BYTES 按 base64 还原，BAD 退化为 StringColumn 保留内容；
 * 脱敏列以落盘的 STRING 值回放（原值不可恢复，这是不可逆脱敏的必然语义）。
 */
public class JsonlRecordCodec {

    public Record toRecord(JSONObject line) {
        JSONArray columns = line.getJSONArray(SinkConstants.FIELD_COLUMNS);
        if (columns == null) {
            throw corrupt(line, "缺少 columns 字段");
        }
        Integer columnCount = line.getInteger(SinkConstants.FIELD_COLUMN_COUNT);
        if (columnCount != null && columnCount != columns.size()) {
            throw corrupt(line, "columnCount=" + columnCount
                    + " 与 columns 实际长度 " + columns.size() + " 不一致");
        }

        Record record = new DefaultRecord();
        for (int i = 0; i < columns.size(); i++) {
            JSONObject element = columns.getJSONObject(i);
            if (element == null) {
                record.addColumn(new StringColumn());
                continue;
            }
            record.addColumn(toColumn(element, line, i));
        }

        JSONObject meta = line.getJSONObject(SinkConstants.FIELD_META);
        if (meta != null) {
            @SuppressWarnings("unchecked")
            Map<String, String> metaMap = meta.toJavaObject(Map.class);
            record.setMeta(metaMap);
        }
        return record;
    }

    private Column toColumn(JSONObject element, JSONObject line, int index) {
        String typeName = element.getString(SinkConstants.COLUMN_TYPE);
        if (typeName == null) {
            throw corrupt(line, "第" + index + "列缺少 type 字段");
        }
        Column.Type type;
        try {
            type = Column.Type.valueOf(typeName);
        } catch (IllegalArgumentException e) {
            throw corrupt(line, "第" + index + "列的未知类型: " + typeName);
        }
        Object rawData = element.get(SinkConstants.COLUMN_RAW_DATA);

        if (type == Column.Type.NULL || rawData == null) {
            return nullColumn(type);
        }

        try {
            switch (type) {
                case INT:
                case LONG:
                    return new LongColumn(((Number) toNumber(rawData)).longValue());
                case DOUBLE:
                    return new DoubleColumn(((Number) toNumber(rawData)).doubleValue());
                case BOOL:
                    return new BoolColumn(toBoolean(rawData));
                case DATE:
                    return new DateColumn(((Number) toNumber(rawData)).longValue());
                case BYTES:
                    return new BytesColumn(Base64.getDecoder().decode((String) rawData));
                case STRING:
                    return new StringColumn(rawData.toString());
                case BAD:
                    // 不存在 BadColumn 实现，保留原始负载为字符串
                    return new StringColumn(rawData.toString());
                default:
                    return new StringColumn(rawData.toString());
            }
        } catch (ClassCastException | IllegalArgumentException e) {
            throw corrupt(line, "第" + index + "列(" + typeName + ")的rawData无法解析: "
                    + e.getMessage());
        }
    }

    /**
     * fastjson2 在部分场景把数字解析为 String，这里统一转回 Number。
     */
    private Number toNumber(Object rawData) {
        if (rawData instanceof Number) {
            return (Number) rawData;
        }
        return Double.valueOf(rawData.toString());
    }

    private Boolean toBoolean(Object rawData) {
        if (rawData instanceof Boolean) {
            return (Boolean) rawData;
        }
        return Boolean.valueOf(rawData.toString());
    }

    private Column nullColumn(Column.Type type) {
        switch (type) {
            case INT:
            case LONG:
                return new LongColumn();
            case DOUBLE:
                return new DoubleColumn();
            case BOOL:
                return new BoolColumn();
            case DATE:
                return new DateColumn();
            case BYTES:
                return new BytesColumn();
            case STRING:
            case NULL:
            case BAD:
            default:
                // DataX 没有 NullColumn/BadColumn 实现，NULL 统一还原为 rawData=null 的空列
                return new StringColumn();
        }
    }

    private DataXException corrupt(JSONObject line, String reason) {
        return DataXException.asDataXException(FrameworkErrorCode.REPLAY_FILE_CORRUPT,
                "脏数据记录解析失败: " + reason + "，记录内容=" + line.toJSONString());
    }
}
