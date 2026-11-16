package com.alibaba.datax.core.statistics.plugin.task;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.plugin.task.sink.DirtyRecordSinkManager;
import com.alibaba.datax.core.statistics.plugin.task.sink.DirtySinkContext;
import com.alibaba.datax.core.statistics.plugin.task.sink.SinkChannel;
import com.alibaba.datax.core.statistics.plugin.task.sink.TaskContextHolder;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 支持脏数据结构化落盘的收集器。
 *
 * 关闭（job.setting.dirtyRecord.enable 非 true）时：只有一个 boolean 判断，
 * 其余完全走 {@link StdoutPluginCollector} 的日志与计数逻辑，行为与历史版本一致。
 *
 * 开启时：在父类日志/计数之外，把每条脏数据以 JSONL 落盘；落盘失败默认让 job 失败，
 * 不静默丢数据。Communication 计数仍由父类负责，errorLimit 语义不变。
 */
public class FileSinkPluginCollector extends StdoutPluginCollector {

    private static final Logger LOG = LoggerFactory.getLogger(FileSinkPluginCollector.class);

    private final boolean sinkEnabled;

    private final long jobId;

    private final PluginType pluginType;

    /**
     * 首条脏数据时懒加载，关闭路径下永不触发任何文件操作。
     */
    private volatile SinkChannel sinkChannel;

    private volatile boolean lazyInitFailed;

    public FileSinkPluginCollector(Configuration configuration, Communication communication,
                                   PluginType type) {
        super(configuration, communication, type);
        this.pluginType = type;
        this.sinkEnabled = DirtySinkContext.isEnabled(configuration);
        this.jobId = configuration.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, 0L);
    }

    @Override
    public void collectDirtyRecord(Record dirtyRecord, Throwable t, String errorMessage) {
        if (sinkEnabled && dirtyRecord != null) {
            try {
                SinkChannel channel = getOrCreateChannel();
                if (channel != null) {
                    boolean written = channel.append(dirtyRecord, t, errorMessage);
                    if (!written) {
                        LOG.warn("脏数据落盘总量已达上限(overCapPolicy=STOP_RECORD)，"
                                + "该条脏数据仅计数与打日志，不再落盘。taskId 见日志上下文");
                    }
                }
            } catch (RuntimeException e) {
                // 落盘 IO 错误或 FAIL_JOB 超限：直接抛出使 task/job 失败，不静默丢数据
                LOG.error("脏数据落盘失败，任务将失败", e);
                throw e;
            }
        }

        // 无论是否开启落盘，日志和 Communication 计数行为都与历史版本完全一致
        super.collectDirtyRecord(dirtyRecord, t, errorMessage);
    }

    private SinkChannel getOrCreateChannel() {
        SinkChannel local = sinkChannel;
        if (local != null) {
            return local;
        }
        if (lazyInitFailed) {
            return null;
        }
        synchronized (this) {
            if (sinkChannel != null) {
                return sinkChannel;
            }
            DirtyRecordSinkManager manager = DirtyRecordSinkManager.getOrNull(jobId);
            if (manager == null) {
                // 管理器未注册（例如异常生命周期），退回纯日志模式并只告警一次
                lazyInitFailed = true;
                LOG.warn("未找到jobId={}的脏数据落盘管理器，本条脏数据只打日志不落盘", jobId);
                return null;
            }
            TaskContextHolder.TaskContext context = TaskContextHolder.get(this);
            int taskGroupId = context == null ? -1 : context.getTaskGroupId();
            int taskId = context == null ? -1 : context.getTaskId();
            int attempt = context == null ? 0 : context.getAttempt();
            sinkChannel = manager.acquire(taskGroupId, taskId, attempt, pluginType);
            return sinkChannel;
        }
    }
}
