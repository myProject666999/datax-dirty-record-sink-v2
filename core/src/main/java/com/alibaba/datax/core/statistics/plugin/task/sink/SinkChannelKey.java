package com.alibaba.datax.core.statistics.plugin.task.sink;

import com.alibaba.datax.common.constant.PluginType;

/**
 * 一个 collector 实例的唯一标识：taskGroup + task + failover attempt + 插件侧。
 */
public class SinkChannelKey {

    private final int taskGroupId;

    private final int taskId;

    private final int attempt;

    private final PluginType pluginType;

    public SinkChannelKey(int taskGroupId, int taskId, int attempt, PluginType pluginType) {
        this.taskGroupId = taskGroupId;
        this.taskId = taskId;
        this.attempt = attempt;
        this.pluginType = pluginType;
    }

    public int getTaskGroupId() {
        return taskGroupId;
    }

    public int getTaskId() {
        return taskId;
    }

    public int getAttempt() {
        return attempt;
    }

    public PluginType getPluginType() {
        return pluginType;
    }

    public String filePrefix(long jobId) {
        return String.format("dirty-j%d-tg%d-t%d-a%d-%s",
                jobId, taskGroupId, taskId, attempt, pluginType.name().toLowerCase());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SinkChannelKey)) {
            return false;
        }
        SinkChannelKey that = (SinkChannelKey) o;
        return taskGroupId == that.taskGroupId
                && taskId == that.taskId
                && attempt == that.attempt
                && pluginType == that.pluginType;
    }

    @Override
    public int hashCode() {
        int result = taskGroupId;
        result = 31 * result + taskId;
        result = 31 * result + attempt;
        result = 31 * result + pluginType.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "SinkChannelKey{tg=" + taskGroupId + ", task=" + taskId
                + ", attempt=" + attempt + ", type=" + pluginType + '}';
    }
}
