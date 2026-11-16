package com.alibaba.datax.core.statistics.plugin.task.sink;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * 脏数据落盘的不可变配置。每个启用落盘的 job 构造一个，由 DirtyRecordSinkManager 持有。
 */
public class DirtySinkContext {

    private static final Logger LOG = LoggerFactory.getLogger(DirtySinkContext.class);

    public static final long DEFAULT_FILE_MAX_SIZE = 64L * 1024L * 1024L;

    public static final long DEFAULT_TOTAL_SIZE_CAP = 1024L * 1024L * 1024L;

    private final long jobId;

    private final File rootDir;

    private final File sinkDir;

    private final String sinkName;

    private final long maxFileBytes;

    private final long totalSizeCapBytes;

    /**
     * STOP_RECORD 或 FAIL_JOB
     */
    private final String overCapPolicy;

    private final boolean flushEveryRecord;

    private final Map<Integer, ColumnMasker> maskers;

    private final List<Integer> maskedColumns;

    /**
     * 回放场景标记，落盘行内附加 replayOf 等字段；正常同步为 null。
     */
    private final String replayOf;

    private final int replayAttempt;

    DirtySinkContext(long jobId, File rootDir, String sinkName, long maxFileBytes,
                     long totalSizeCapBytes, String overCapPolicy,
                     boolean flushEveryRecord,
                     Map<Integer, ColumnMasker> maskers,
                     String replayOf, int replayAttempt) {
        this.jobId = jobId;
        this.rootDir = rootDir;
        this.sinkName = sinkName;
        this.sinkDir = new File(rootDir, sinkName);
        this.maxFileBytes = maxFileBytes;
        this.totalSizeCapBytes = totalSizeCapBytes;
        this.overCapPolicy = overCapPolicy;
        this.flushEveryRecord = flushEveryRecord;
        this.maskers = maskers;
        List<Integer> columns = new ArrayList<Integer>(maskers.keySet());
        Collections.sort(columns);
        this.maskedColumns = Collections.unmodifiableList(columns);
        this.replayOf = replayOf;
        this.replayAttempt = replayAttempt;
    }

    public static boolean isEnabled(Configuration conf) {
        return conf.getBool(CoreConstant.DATAX_JOB_SETTING_DIRTY_RECORD_ENABLE, false);
    }

    /**
     * 从 job 配置构造落盘上下文。enable=false 时返回 null。
     */
    public static DirtySinkContext fromJob(Configuration conf) {
        if (!isEnabled(conf)) {
            return null;
        }

        long jobId = conf.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, 0L);

        String path = conf.getString(CoreConstant.DATAX_JOB_SETTING_DIRTY_RECORD_PATH);
        if (StringUtils.isBlank(path)) {
            throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,
                    "启用脏数据落盘时 job.setting.dirtyRecord.path 不能为空");
        }
        File rootDir = new File(path);

        String sinkName = conf.getString(
                CoreConstant.DATAX_JOB_SETTING_DIRTY_RECORD_SINK_NAME);
        if (StringUtils.isBlank(sinkName)) {
            sinkName = "sink-job" + jobId + "-"
                    + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date());
        }

        long maxFileBytes = ByteSizeParser.parse(
                conf.getString(CoreConstant.DATAX_JOB_SETTING_DIRTY_RECORD_FILE_MAX_SIZE),
                DEFAULT_FILE_MAX_SIZE);
        long totalSizeCap = ByteSizeParser.parse(
                conf.getString(CoreConstant.DATAX_JOB_SETTING_DIRTY_RECORD_TOTAL_SIZE_CAP),
                DEFAULT_TOTAL_SIZE_CAP);

        String policy = conf.getString(
                CoreConstant.DATAX_JOB_SETTING_DIRTY_RECORD_OVER_CAP_POLICY,
                SinkConstants.OVER_CAP_POLICY_STOP_RECORD);
        if (!SinkConstants.OVER_CAP_POLICY_STOP_RECORD.equalsIgnoreCase(policy)
                && !SinkConstants.OVER_CAP_POLICY_FAIL_JOB.equalsIgnoreCase(policy)) {
            throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,
                    "job.setting.dirtyRecord.overCapPolicy 仅支持 STOP_RECORD / FAIL_JOB，"
                            + "当前配置: " + policy);
        }

        boolean flushEveryRecord = conf.getBool(
                CoreConstant.DATAX_JOB_SETTING_DIRTY_RECORD_FLUSH_EVERY_RECORD, true);

        Map<Integer, ColumnMasker> maskers = resolveMaskers(conf);

        return new DirtySinkContext(jobId, rootDir, sinkName, maxFileBytes, totalSizeCap,
                policy.toUpperCase(), flushEveryRecord, maskers, null, 0);
    }

    private static Map<Integer, ColumnMasker> resolveMaskers(Configuration conf) {
        List<Configuration> ruleConfs = conf.getListConfiguration(
                CoreConstant.DATAX_JOB_SETTING_DIRTY_RECORD_MASK_RULES);
        if (ruleConfs == null || ruleConfs.isEmpty()) {
            return Collections.emptyMap();
        }
        List<MaskRule> rules = new ArrayList<MaskRule>(ruleConfs.size());
        for (Configuration ruleConf : ruleConfs) {
            MaskRule rule = MaskRule.from(ruleConf);
            if (StringUtils.isBlank(rule.getColumnName()) && rule.getColumnIndex() == null) {
                throw DataXException.asDataXException(
                        FrameworkErrorCode.DIRTY_RECORD_MASK_CONFIG_ERROR,
                        "每条脱敏规则必须配置 columnName 或 columnIndex");
            }
            rules.add(rule);
        }
        String unknownPolicy = conf.getString(
                CoreConstant.DATAX_JOB_SETTING_DIRTY_RECORD_MASK_UNKNOWN_COLUMN,
                SinkConstants.UNKNOWN_COLUMN_ERROR);
        boolean errorOnUnknown = !SinkConstants.UNKNOWN_COLUMN_IGNORE.equalsIgnoreCase(unknownPolicy);

        MaskRuleResolver.Resolution resolution = MaskRuleResolver.resolve(rules, conf, errorOnUnknown);
        return Collections.unmodifiableMap(resolution.getMaskers());
    }

    /**
     * 回放再脏数据场景：手工构造一个落盘上下文。
     */
    public static DirtySinkContext forReplay(long jobId, File rootDir, String sinkName,
                                             long maxFileBytes, long totalSizeCapBytes,
                                             String replayOf, int replayAttempt) {
        return new DirtySinkContext(jobId, rootDir, sinkName, maxFileBytes, totalSizeCapBytes,
                SinkConstants.OVER_CAP_POLICY_STOP_RECORD, true,
                Collections.<Integer, ColumnMasker>emptyMap(), replayOf, replayAttempt);
    }

    /**
     * 注册时调用：创建并校验目录，失败快速报错，避免任务跑起来后才发现无法落盘。
     */
    public void prepareDirectory() {
        if (!sinkDir.exists() && !sinkDir.mkdirs()) {
            throw DataXException.asDataXException(FrameworkErrorCode.DIRTY_RECORD_SINK_ERROR,
                    "脏数据落盘目录创建失败: " + sinkDir.getAbsolutePath());
        }
        if (!sinkDir.isDirectory() || !sinkDir.canWrite()) {
            throw DataXException.asDataXException(FrameworkErrorCode.DIRTY_RECORD_SINK_ERROR,
                    "脏数据落盘目录不可写: " + sinkDir.getAbsolutePath());
        }
        if (sinkDir.listFiles() != null && sinkDir.listFiles().length > 0) {
            LOG.warn("脏数据落盘目录[{}]非空，新文件将与已有文件共存，请确认sinkName没有重复",
                    sinkDir.getAbsolutePath());
        }
    }

    public long getJobId() {
        return jobId;
    }

    public File getRootDir() {
        return rootDir;
    }

    public File getSinkDir() {
        return sinkDir;
    }

    public String getSinkName() {
        return sinkName;
    }

    public long getMaxFileBytes() {
        return maxFileBytes;
    }

    public long getTotalSizeCapBytes() {
        return totalSizeCapBytes;
    }

    public String getOverCapPolicy() {
        return overCapPolicy;
    }

    public boolean isFlushEveryRecord() {
        return flushEveryRecord;
    }

    public Map<Integer, ColumnMasker> getMaskers() {
        return maskers;
    }

    public List<Integer> getMaskedColumns() {
        return maskedColumns;
    }

    public String getReplayOf() {
        return replayOf;
    }

    public int getReplayAttempt() {
        return replayAttempt;
    }

    public boolean isFailJobOnCapExceeded() {
        return SinkConstants.OVER_CAP_POLICY_FAIL_JOB.equals(overCapPolicy);
    }

    public ColumnMasker getMasker(int columnIndex) {
        return maskers.get(columnIndex);
    }
}
