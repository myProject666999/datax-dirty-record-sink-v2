package com.alibaba.datax.core.statistics.plugin.task.sink;

/**
 * 脏数据落盘相关的常量：文件名、JSONL字段名、manifest字段名、策略枚举值等。
 * 配置路径常量见 {@link com.alibaba.datax.core.util.container.CoreConstant}。
 */
public final class SinkConstants {

    private SinkConstants() {
    }

    public static final String JSONL_SUFFIX = ".jsonl";

    /**
     * 活动文件（尚未滚动/关闭）的后缀，崩溃残留的 .writing 文件不会被 manifest 收录，
     * 回放时按损坏文件明确报错。
     */
    public static final String WRITING_SUFFIX = ".jsonl.writing";

    public static final String MANIFEST_FILE_NAME = "manifest.json";

    public static final String MANIFEST_TMP_SUFFIX = ".tmp";

    public static final String SUCCESS_MARKER = "_SUCCESS";

    public static final String FAILED_MARKER = "_FAILED";

    public static final String REPLAYED_MARKER = "_REPLAYED";

    public static final String OVER_CAP_POLICY_STOP_RECORD = "STOP_RECORD";

    public static final String OVER_CAP_POLICY_FAIL_JOB = "FAIL_JOB";

    public static final String MASK_STRATEGY_SHA256 = "sha256";

    public static final String MASK_STRATEGY_REPLACE = "replace";

    public static final String UNKNOWN_COLUMN_ERROR = "ERROR";

    public static final String UNKNOWN_COLUMN_IGNORE = "IGNORE";

    public static final int LINE_VERSION = 1;

    // ---------------- JSONL 行字段 ----------------
    public static final String FIELD_VERSION = "version";
    public static final String FIELD_JOB_ID = "jobId";
    public static final String FIELD_TASK_GROUP_ID = "taskGroupId";
    public static final String FIELD_TASK_ID = "taskId";
    public static final String FIELD_ATTEMPT = "attempt";
    public static final String FIELD_PLUGIN_TYPE = "pluginType";
    public static final String FIELD_TIMESTAMP = "timestamp";
    public static final String FIELD_SEQUENCE = "sequence";
    public static final String FIELD_ERROR_MESSAGE = "errorMessage";
    public static final String FIELD_EXCEPTION_CLASS = "exceptionClass";
    public static final String FIELD_EXCEPTION_MESSAGE = "exceptionMessage";
    public static final String FIELD_META = "meta";
    public static final String FIELD_COLUMN_COUNT = "columnCount";
    public static final String FIELD_COLUMNS = "columns";
    public static final String FIELD_REPLAY_OF = "replayOf";
    public static final String FIELD_REPLAY_ATTEMPT = "replayAttempt";

    // ---------------- column 元素字段 ----------------
    public static final String COLUMN_INDEX = "index";
    public static final String COLUMN_TYPE = "type";
    public static final String COLUMN_RAW_DATA = "rawData";
    public static final String COLUMN_MASKED = "masked";
    public static final String COLUMN_MASK_STRATEGY = "maskStrategy";
    public static final String COLUMN_ENCODING = "encoding";
    public static final String ENCODING_BASE64 = "base64";
    public static final String ENCODING_EPOCH_MILLIS = "epochMillis";

    // ---------------- manifest 字段 ----------------
    public static final String MANIFEST_SINK_NAME = "sinkName";
    public static final String MANIFEST_CREATED_AT = "createdAt";
    public static final String MANIFEST_CLOSED_AT = "closedAt";
    public static final String MANIFEST_JOB_STATUS = "jobStatus";
    public static final String MANIFEST_TOTAL_FILES = "totalFiles";
    public static final String MANIFEST_TOTAL_RECORDS = "totalRecords";
    public static final String MANIFEST_TOTAL_BYTES = "totalBytes";
    public static final String MANIFEST_TOTAL_SIZE_CAP = "totalSizeCapBytes";
    public static final String MANIFEST_CAP_REACHED = "capReached";
    public static final String MANIFEST_DROPPED_RECORDS = "droppedRecords";
    public static final String MANIFEST_MASKED_COLUMNS = "maskedColumns";
    public static final String MANIFEST_FILES = "files";
    public static final String MANIFEST_ENTRY_FILE_NAME = "fileName";
    public static final String MANIFEST_FILE_RECORDS = "records";
    public static final String MANIFEST_FILE_BYTES = "bytes";
    public static final String MANIFEST_FILE_SHA256 = "sha256";
    public static final String MANIFEST_FILE_FIRST_SEQ = "firstSequence";
    public static final String MANIFEST_FILE_LAST_SEQ = "lastSequence";

    public static final String JOB_STATUS_SUCCESS = "SUCCESS";
    public static final String JOB_STATUS_FAILED = "FAILED";
}
