package com.alibaba.datax.core.statistics.plugin.task.sink;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.Record;
import org.apache.commons.lang3.StringUtils;

import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * 把一条脏数据 Record 序列化为 JSONL 行 Map。脱敏在序列化前完成，原始 Record 不被修改，
 * 原始敏感值不会进入返回结果。每个 SinkChannel 持有一个实例（id 信息固定）。
 */
public class DirtyRecordSerializer {

    private final DirtySinkContext context;

    private final int taskGroupId;

    private final int taskId;

    private final int attempt;

    private final PluginType pluginType;

    public DirtyRecordSerializer(DirtySinkContext context, int taskGroupId, int taskId,
                                 int attempt, PluginType pluginType) {
        this.context = context;
        this.taskGroupId = taskGroupId;
        this.taskId = taskId;
        this.attempt = attempt;
        this.pluginType = pluginType;
    }

    public Map<String, Object> toLine(Record record, Throwable throwable,
                                      String errorMessage, long sequence) {
        Map<String, Object> line = new LinkedHashMap<String, Object>();
        line.put(SinkConstants.FIELD_VERSION, SinkConstants.LINE_VERSION);
        line.put(SinkConstants.FIELD_JOB_ID, context.getJobId());
        line.put(SinkConstants.FIELD_TASK_GROUP_ID, taskGroupId);
        line.put(SinkConstants.FIELD_TASK_ID, taskId);
        line.put(SinkConstants.FIELD_ATTEMPT, attempt);
        line.put(SinkConstants.FIELD_PLUGIN_TYPE, pluginType.name());
        line.put(SinkConstants.FIELD_TIMESTAMP, System.currentTimeMillis());
        line.put(SinkConstants.FIELD_SEQUENCE, sequence);

        if (StringUtils.isNotBlank(errorMessage)) {
            line.put(SinkConstants.FIELD_ERROR_MESSAGE, errorMessage);
        }
        if (throwable != null) {
            line.put(SinkConstants.FIELD_EXCEPTION_CLASS, throwable.getClass().getName());
            if (StringUtils.isNotBlank(throwable.getMessage())) {
                line.put(SinkConstants.FIELD_EXCEPTION_MESSAGE, throwable.getMessage());
            }
        }
        if (record != null && record.getMeta() != null && !record.getMeta().isEmpty()) {
            line.put(SinkConstants.FIELD_META, record.getMeta());
        }
        if (context.getReplayOf() != null) {
            line.put(SinkConstants.FIELD_REPLAY_OF, context.getReplayOf());
            line.put(SinkConstants.FIELD_REPLAY_ATTEMPT, context.getReplayAttempt());
        }

        int columnCount = record == null ? 0 : record.getColumnNumber();
        line.put(SinkConstants.FIELD_COLUMN_COUNT, columnCount);
        java.util.List<Map<String, Object>> columns =
                new java.util.ArrayList<Map<String, Object>>(Math.max(columnCount, 0));
        if (record != null) {
            for (int i = 0; i < columnCount; i++) {
                columns.add(serializeColumn(record.getColumn(i), i));
            }
        }
        line.put(SinkConstants.FIELD_COLUMNS, columns);
        return line;
    }

    private Map<String, Object> serializeColumn(Column column, int index) {
        Map<String, Object> element = new LinkedHashMap<String, Object>();
        element.put(SinkConstants.COLUMN_INDEX, index);

        if (column == null || column.getType() == null || column.getType() == Column.Type.NULL
                || column.getRawData() == null) {
            element.put(SinkConstants.COLUMN_TYPE, Column.Type.NULL.name());
            element.put(SinkConstants.COLUMN_RAW_DATA, null);
            return element;
        }

        Column.Type type = column.getType();
        ColumnMasker masker = context.getMasker(index);
        if (masker != null) {
            String plain = toPlainString(column);
            String masked = masker.mask(plain);
            element.put(SinkConstants.COLUMN_TYPE, Column.Type.STRING.name());
            element.put(SinkConstants.COLUMN_RAW_DATA, masked);
            element.put(SinkConstants.COLUMN_MASKED, true);
            element.put(SinkConstants.COLUMN_MASK_STRATEGY, masker.getStrategy());
            return element;
        }

        element.put(SinkConstants.COLUMN_TYPE, type.name());
        switch (type) {
            case BYTES:
                byte[] bytes = (byte[]) column.getRawData();
                element.put(SinkConstants.COLUMN_RAW_DATA,
                        Base64.getEncoder().encodeToString(bytes));
                element.put(SinkConstants.COLUMN_ENCODING, SinkConstants.ENCODING_BASE64);
                break;
            case DATE:
                element.put(SinkConstants.COLUMN_RAW_DATA, column.getRawData());
                element.put(SinkConstants.COLUMN_ENCODING, SinkConstants.ENCODING_EPOCH_MILLIS);
                break;
            default:
                element.put(SinkConstants.COLUMN_RAW_DATA, column.getRawData());
                break;
        }
        return element;
    }

    /**
     * 计算用于脱敏的明文字符串。BYTES 先 Base64，其它类型优先 asString，失败退回 toString。
     */
    private String toPlainString(Column column) {
        if (column.getType() == Column.Type.BYTES) {
            return Base64.getEncoder().encodeToString((byte[]) column.getRawData());
        }
        try {
            String value = column.asString();
            return value == null ? "" : value;
        } catch (Exception e) {
            Object raw = column.getRawData();
            return raw == null ? "" : raw.toString();
        }
    }
}
