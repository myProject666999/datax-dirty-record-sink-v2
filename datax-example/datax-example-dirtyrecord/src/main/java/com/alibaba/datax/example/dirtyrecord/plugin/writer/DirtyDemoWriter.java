package com.alibaba.datax.example.dirtyrecord.plugin.writer;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;

import java.util.ArrayList;
import java.util.List;

/**
 * 仅用于脏数据落盘/回放 E2E 测试的 writer 插件。
 * 参数：
 * - writerDirtyValues：第一列命中这些值的记录在 writer 侧上报为脏数据
 * - reDirtyValues：回放场景使用，命中这些值的记录再次上报为脏数据
 * - failOnValue：非负时，第一列等于该值的记录直接抛异常（模拟约束冲突等致命错误）
 * - collectOutputPath：非空时把成功写入的记录第一列逐行写到该文件，便于断言回放条数
 */
public class DirtyDemoWriter extends Writer {

    public static class Job extends Writer.Job {

        private Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> configs = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                configs.add(this.originalConfig.clone());
            }
            return configs;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }

    public static class Task extends Writer.Task {

        private List<Number> writerDirtyValues;

        private List<Number> reDirtyValues;

        private long failOnValue = -1;

        private String collectOutputPath;

        @Override
        public void init() {
            Configuration conf = super.getPluginJobConf();
            // fastjson 对小整数可能给出 Integer，统一按 Number 接收再 longValue 比较
            this.writerDirtyValues = conf.getList("writerDirtyValues",
                    new ArrayList<Number>(), Number.class);
            this.reDirtyValues = conf.getList("reDirtyValues",
                    new ArrayList<Number>(), Number.class);
            this.failOnValue = conf.getLong("failOnValue", -1L);
            this.collectOutputPath = conf.getString("collectOutputPath", null);
        }

        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            java.io.BufferedWriter output = null;
            try {
                if (collectOutputPath != null) {
                    output = new java.io.BufferedWriter(new java.io.OutputStreamWriter(
                            new java.io.FileOutputStream(collectOutputPath, true), "UTF-8"));
                }
                Record record;
                while ((record = lineReceiver.getFromReader()) != null) {
                    // LongColumn 的 rawData 内部是 BigInteger，统一按 long 比较
                    long firstValue = record.getColumn(0).asLong();

                    if (containsLong(writerDirtyValues, firstValue)
                            || containsLong(reDirtyValues, firstValue)) {
                        getTaskPluginCollector().collectDirtyRecord(record,
                                new java.sql.SQLException("constraint violation for " + firstValue),
                                "writer侧约束冲突");
                        continue;
                    }
                    if (firstValue == failOnValue) {
                        throw new RuntimeException("模拟writer致命错误，value=" + firstValue);
                    }

                    if (output != null) {
                        output.write(String.valueOf(firstValue));
                        output.newLine();
                    }
                }
                if (output != null) {
                    output.flush();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            } finally {
                if (output != null) {
                    try {
                        output.close();
                    } catch (Exception ignored) {
                        // ignore
                    }
                }
            }
        }

        private boolean containsLong(List<Number> values, long target) {
            for (Number value : values) {
                if (value != null && value.longValue() == target) {
                    return true;
                }
            }
            return false;
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }
}
