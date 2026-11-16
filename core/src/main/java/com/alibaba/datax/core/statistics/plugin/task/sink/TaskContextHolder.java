package com.alibaba.datax.core.statistics.plugin.task.sink;

import com.alibaba.datax.common.plugin.TaskPluginCollector;

import java.util.concurrent.ConcurrentHashMap;

/**
 * 收集器实例的身份注册表。收集器由固定签名 (Configuration, Communication, PluginType)
 * 反射构造，attempt 信息无法走构造参数，因此在 TaskExecutor 创建收集器后，
 * 以 collector 实例为 key 登记 (taskGroupId, taskId, attempt)。
 */
public final class TaskContextHolder {

    private static final ConcurrentHashMap<TaskPluginCollector, TaskContext> CONTEXTS =
            new ConcurrentHashMap<TaskPluginCollector, TaskContext>();

    private TaskContextHolder() {
    }

    public static void bind(TaskPluginCollector collector, int taskGroupId,
                            int taskId, int attempt) {
        CONTEXTS.put(collector, new TaskContext(taskGroupId, taskId, attempt));
    }

    public static TaskContext get(TaskPluginCollector collector) {
        return CONTEXTS.get(collector);
    }

    public static void unbind(TaskPluginCollector collector) {
        CONTEXTS.remove(collector);
    }

    public static class TaskContext {
        private final int taskGroupId;
        private final int taskId;
        private final int attempt;

        public TaskContext(int taskGroupId, int taskId, int attempt) {
            this.taskGroupId = taskGroupId;
            this.taskId = taskId;
            this.attempt = attempt;
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
    }
}
