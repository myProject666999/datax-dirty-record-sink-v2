package com.alibaba.datax.example.dirtyrecord.plugin.reader;

import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.element.StringColumn;
import com.alibaba.datax.common.plugin.RecordSender;
import com.alibaba.datax.common.spi.Reader;
import com.alibaba.datax.common.util.Configuration;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 仅用于脏数据落盘 E2E 测试的 reader 插件。
 * 参数：
 * - totalRecords：发送记录数
 * - readerDirtyIndexes：这些下标的记录在 reader 侧主动上报为脏数据（0 基）
 * - metaFilePath：非空时给每条记录设置 filePath meta，验证来源信息落盘
 * - oversizeIndex：非负时，该下标发送一条超大字符串记录（触发 exchanger 的单条超限脏数据）
 */
public class DirtyDemoReader extends Reader {

    public static class Job extends Reader.Job {

        private Configuration originalConfig;

        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
        }

        @Override
        public List<Configuration> split(int adviceNumber) {
            List<Configuration> configs = new ArrayList<Configuration>();
            for (int i = 0; i < adviceNumber; i++) {
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

    public static class Task extends Reader.Task {

        private int totalRecords;

        private List<Integer> readerDirtyIndexes;

        private String metaFilePath;

        private int oversizeIndex;

        @Override
        public void init() {
            Configuration conf = super.getPluginJobConf();
            this.totalRecords = conf.getInt("totalRecords", 10);
            this.readerDirtyIndexes = conf.getList("readerDirtyIndexes",
                    new ArrayList<Integer>(), Integer.class);
            this.metaFilePath = conf.getString("metaFilePath", null);
            this.oversizeIndex = conf.getInt("oversizeIndex", -1);
        }

        @Override
        public void startRead(RecordSender recordSender) {
            for (int i = 0; i < totalRecords; i++) {
                Record record = recordSender.createRecord();
                record.addColumn(new LongColumn(i));
                if (i == oversizeIndex) {
                    StringBuilder huge = new StringBuilder();
                    for (int j = 0; j < 1024 * 1024; j++) {
                        huge.append('X');
                    }
                    record.addColumn(new StringColumn(huge.toString()));
                } else {
                    record.addColumn(new StringColumn("name-" + i));
                }
                if (metaFilePath != null) {
                    record.setMeta(Collections.singletonMap("filePath", metaFilePath));
                }

                if (readerDirtyIndexes.contains(i)) {
                    getTaskPluginCollector().collectDirtyRecord(record,
                            new RuntimeException("reader-boom-" + i),
                            "reader侧主动上报脏数据");
                    continue;
                }
                recordSender.sendToWriter(record);
            }
        }

        @Override
        public void post() {
        }

        @Override
        public void destroy() {
        }
    }
}
