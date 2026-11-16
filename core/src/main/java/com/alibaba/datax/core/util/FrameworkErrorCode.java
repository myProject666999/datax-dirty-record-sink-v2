package com.alibaba.datax.core.util;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * TODO: 根据现有日志数据分析各类错误，进行细化。
 * 
 * <p>请不要格式化本类代码</p>
 */
public enum FrameworkErrorCode implements ErrorCode {

	INSTALL_ERROR("Framework-00", "DataX引擎安装错误, 请联系您的运维解决 ."),
	ARGUMENT_ERROR("Framework-01", "DataX引擎运行错误，该问题通常是由于内部编程错误引起，请联系DataX开发团队解决 ."),
	RUNTIME_ERROR("Framework-02", "DataX引擎运行过程出错，具体原因请参看DataX运行结束时的错误诊断信息  ."),
	CONFIG_ERROR("Framework-03", "DataX引擎配置错误，该问题通常是由于DataX安装错误引起，请联系您的运维解决 ."),
    SECRET_ERROR("Framework-04", "DataX引擎加解密出错，该问题通常是由于DataX密钥配置错误引起，请联系您的运维解决 ."),
    HOOK_LOAD_ERROR("Framework-05", "加载外部Hook出现错误，通常是由于DataX安装引起的"),
    HOOK_FAIL_ERROR("Framework-06", "执行外部Hook出现错误"),

    PLUGIN_INSTALL_ERROR("Framework-10", "DataX插件安装错误, 该问题通常是由于DataX安装错误引起，请联系您的运维解决 ."),
    PLUGIN_NOT_FOUND("Framework-11", "DataX插件配置错误, 该问题通常是由于DataX安装错误引起，请联系您的运维解决 ."),
    PLUGIN_INIT_ERROR("Framework-12", "DataX插件初始化错误, 该问题通常是由于DataX安装错误引起，请联系您的运维解决 ."),
    PLUGIN_RUNTIME_ERROR("Framework-13", "DataX插件运行时出错, 具体原因请参看DataX运行结束时的错误诊断信息 ."),
    PLUGIN_DIRTY_DATA_LIMIT_EXCEED("Framework-14", "DataX传输脏数据超过用户预期，该错误通常是由于源端数据存在较多业务脏数据导致，请仔细检查DataX汇报的脏数据日志信息, 或者您可以适当调大脏数据阈值 ."),
    PLUGIN_SPLIT_ERROR("Framework-15", "DataX插件切分出错, 该问题通常是由于DataX各个插件编程错误引起，请联系DataX开发团队解决"),
    KILL_JOB_TIMEOUT_ERROR("Framework-16", "kill 任务超时，请联系PE解决"),
    START_TASKGROUP_ERROR("Framework-17", "taskGroup启动失败,请联系DataX开发团队解决"),
    CALL_DATAX_SERVICE_FAILED("Framework-18", "请求 DataX Service 出错."),
    CALL_REMOTE_FAILED("Framework-19", "远程调用失败"),

    DIRTY_RECORD_SINK_ERROR("Framework-20", "脏数据落盘过程出错，请检查落盘目录权限和磁盘空间 ."),
    DIRTY_RECORD_SINK_CAP_EXCEEDED("Framework-21", "脏数据落盘总量超过配置的totalSizeCap，且overCapPolicy=FAIL_JOB，任务失败 ."),
    DIRTY_RECORD_MASK_CONFIG_ERROR("Framework-22", "脏数据脱敏配置错误，请检查mask.rules中的列名/列索引与策略配置 ."),
    REPLAY_FILE_CORRUPT("Framework-23", "脏数据回放时发现落盘文件损坏或被截断，请根据报错中的文件名、行号和字节偏移检查 ."),
    REPLAY_DUPLICATE_SINK("Framework-24", "该脏数据落盘目录已经回放完成；如需重复回放请显式指定--force ."),
    REPLAY_MANIFEST_ERROR("Framework-25", "脏数据回放时manifest校验失败（文件缺失或sha256不一致） ."),
    KILLED_EXIT_VALUE("Framework-143", "Job 收到了 Kill 命令.");

    private final String code;

    private final String description;

    private FrameworkErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s]. ", this.code,
                this.description);
    }

    /**
     * 通过 "Framework-143" 来标示 任务是 Killed 状态
     */
    public int toExitValue() {
        if (this == FrameworkErrorCode.KILLED_EXIT_VALUE) {
            return 143;
        } else {
            return 1;
        }
    }

}
