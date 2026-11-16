package com.alibaba.datax.core.replay;

import com.alibaba.datax.common.constant.PluginType;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.AbstractTaskPlugin;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.statistics.communication.Communication;
import com.alibaba.datax.core.statistics.plugin.task.FileSinkPluginCollector;
import com.alibaba.datax.core.statistics.plugin.task.sink.DirtyRecordSinkManager;
import com.alibaba.datax.core.statistics.plugin.task.sink.DirtySinkContext;
import com.alibaba.datax.core.statistics.plugin.task.sink.SinkConstants;
import com.alibaba.datax.core.statistics.plugin.task.sink.TaskContextHolder;
import com.alibaba.datax.core.util.ConfigParser;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.datax.core.util.container.ClassLoaderSwapper;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;
import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * 脏数据回放入口。只加载原 job 的 writer 插件，把 sink 目录里的 jsonl 记录顺序送回 writer，
 * 不启动 reader、不建 Channel。回放中再次变脏的记录通过同一个 FileSinkPluginCollector
 * 重新落盘到独立的 re-dirty 目录，不会丢失。
 */
public class DirtyRecordReplayEngine {

    private static final Logger LOG = LoggerFactory.getLogger(DirtyRecordReplayEngine.class);

    public static void main(String[] args) throws Exception {
        int exitCode = 0;
        try {
            ReplayConfig config = parseArgs(args);
            new DirtyRecordReplayEngine().replay(config);
            LOG.info("脏数据回放完成");
        } catch (Throwable e) {
            exitCode = 1;
            LOG.error("脏数据回放失败: {}", e.getMessage(), e);
        } finally {
            System.exit(exitCode);
        }
    }

    public static ReplayConfig parseArgs(String[] args) throws Exception {
        Options options = new Options();
        options.addOption(null, "sink", true, "脏数据落盘目录（必填）");
        options.addOption(null, "job", true, "原始同步job的json配置（必填，只使用其writer块）");
        options.addOption(null, "force", false, "忽略_REPLAYED标记，强制重复回放");
        options.addOption(null, "allow-manifest-missing", false,
                "manifest缺失时允许降级直接按jsonl回放");
        options.addOption(null, "re-dirty-sink", true,
                "回放再脏数据的输出根目录，默认 <sink>/replay-redirty-<时间戳>");
        CommandLine cl = new BasicParser().parse(options, args);

        ReplayConfig config = new ReplayConfig();
        String sink = cl.getOptionValue("sink");
        String job = cl.getOptionValue("job");
        if (StringUtils.isBlank(sink) || StringUtils.isBlank(job)) {
            throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,
                    "回放必须指定 --sink 和 --job");
        }
        config.setSinkDir(new File(sink));
        config.setJobFile(new File(job));
        config.setForce(cl.hasOption("force"));
        config.setAllowManifestMissing(cl.hasOption("allow-manifest-missing"));
        if (cl.hasOption("re-dirty-sink")) {
            config.setReDirtySinkDir(new File(cl.getOptionValue("re-dirty-sink")));
        }
        return config;
    }

    public void replay(ReplayConfig config) {
        // 1. sink 目录校验（重复回放、manifest、sha256、损坏文件）
        ReplayManifestReader.ReplayPlan plan = new ReplayManifestReader()
                .prepare(config.getSinkDir(), config.isForce(), config.isAllowManifestMissing());

        // 2. 解析原 job，只注册 writer 插件
        Configuration jobConf = ConfigParser.parseJobConfig(config.getJobFile().getAbsolutePath());
        String writerName = jobConf.getString(CoreConstant.DATAX_JOB_CONTENT_WRITER_NAME);
        Configuration writerParameter = jobConf.getConfiguration(
                CoreConstant.DATAX_JOB_CONTENT_WRITER_PARAMETER);
        if (StringUtils.isBlank(writerName) || writerParameter == null) {
            throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,
                    "原始job配置缺少 job.content[0].writer.name/parameter");
        }
        Configuration allConf = Configuration.newDefault();
        allConf.merge(Configuration.from(new File(CoreConstant.DATAX_CONF_PATH)), false);
        allConf.merge(loadOnlyWriterPlugin(writerName), false);
        allConf.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, 0L);
        LoadUtil.bind(allConf);

        // 3. 准备回放再脏数据的 sink
        File reDirtyRoot = config.getReDirtySinkDir() != null
                ? config.getReDirtySinkDir()
                : new File(config.getSinkDir(), "replay-redirty-"
                        + new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()));
        String reSinkName = config.getSinkDir().getName() + "-replay";
        long replayJobId = allConf.getLong(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID);
        DirtySinkContext reSinkContext = DirtySinkContext.forReplay(
                replayJobId,
                reDirtyRoot, reSinkName,
                DirtySinkContext.DEFAULT_FILE_MAX_SIZE,
                DirtySinkContext.DEFAULT_TOTAL_SIZE_CAP,
                config.getSinkDir().getName(), 1);
        DirtyRecordSinkManager.register(reSinkContext);

        ClassLoaderSwapper swapper = ClassLoaderSwapper.newCurrentThreadClassLoaderSwapper();
        JsonlRecordReceiver receiver = new JsonlRecordReceiver(plan.getOrderedFiles());
        try {
            swapper.setCurrentThreadClassLoader(
                    LoadUtil.getJarLoader(PluginType.WRITER, writerName));

            com.alibaba.datax.common.spi.Writer.Job writerJob =
                    (com.alibaba.datax.common.spi.Writer.Job)
                            LoadUtil.loadJobPlugin(PluginType.WRITER, writerName);
            writerJob.setPluginJobConf(writerParameter);
            writerJob.setPeerPluginJobConf(writerParameter);
            writerJob.setPeerPluginName(writerName);
            writerJob.init();
            writerJob.prepare();
            List<Configuration> slices = writerJob.split(1);
            if (slices == null || slices.isEmpty()) {
                throw DataXException.asDataXException(FrameworkErrorCode.PLUGIN_SPLIT_ERROR,
                        "writer切分结果为空，无法回放");
            }
            if (slices.size() > 1) {
                LOG.warn("writer切分出{}个slice，回放按顺序逐个执行", slices.size());
            }

            // 4. 逐个 slice 顺序回放（无 reader、无 Channel）
            for (int i = 0; i < slices.size(); i++) {
                runWriterTask(writerName, slices.get(i), receiver, reSinkContext, replayJobId, i);
            }

            writerJob.post();
            writerJob.destroy();
        } finally {
            swapper.restoreCurrentThreadClassLoader();
            receiver.shutdown();
            DirtyRecordSinkManager manager = DirtyRecordSinkManager.getOrNull(
                    reSinkContext.getJobId());
            if (manager != null) {
                manager.close(true);
            }
        }

        // 5. 回放成功后写 _REPLAYED 标记
        writeReplayedMarker(config.getSinkDir());
        LOG.info("回放再脏数据目录: {}", reSinkContext.getSinkDir().getAbsolutePath());
    }

    private void runWriterTask(String writerName, Configuration sliceConf,
                               JsonlRecordReceiver receiver, DirtySinkContext reSinkContext,
                               long replayJobId, int taskIndex) {
        AbstractTaskPlugin taskPlugin = LoadUtil.loadTaskPlugin(PluginType.WRITER, writerName);
        taskPlugin.setPluginJobConf(sliceConf);
        taskPlugin.setTaskGroupId(0);
        taskPlugin.setTaskId(taskIndex);

        // 再脏数据收集器：复用正常落盘链路，写到独立 re-dirty 目录
        Configuration sinkEnabledConf = Configuration.newDefault();
        sinkEnabledConf.set(CoreConstant.DATAX_JOB_SETTING_DIRTY_RECORD_ENABLE, true);
        sinkEnabledConf.set(CoreConstant.DATAX_CORE_CONTAINER_JOB_ID, replayJobId);
        FileSinkPluginCollector collector = new FileSinkPluginCollector(
                sinkEnabledConf, new Communication(), PluginType.WRITER);
        TaskContextHolder.bind(collector, 0, taskIndex, 1);
        taskPlugin.setTaskPluginCollector(collector);

        taskPlugin.init();
        taskPlugin.prepare();
        try {
            ((com.alibaba.datax.common.spi.Writer.Task) taskPlugin).startWrite(receiver);
            taskPlugin.post();
        } finally {
            taskPlugin.destroy();
        }
    }

    /**
     * 扫描插件注册目录，只保留指定 writer 插件的注册信息，reader 不参与回放。
     */
    private Configuration loadOnlyWriterPlugin(String writerName) {
        Configuration full = ConfigParser.parsePluginConfig(new ArrayList<String>());
        Configuration writerConf = full.getConfiguration("plugin.writer." + writerName);
        if (writerConf == null) {
            throw DataXException.asDataXException(FrameworkErrorCode.PLUGIN_NOT_FOUND,
                    "在 " + CoreConstant.DATAX_PLUGIN_WRITER_HOME + " 下找不到 writer 插件: "
                            + writerName);
        }
        Configuration result = Configuration.newDefault();
        result.set("plugin.writer." + writerName, writerConf.getInternal());
        return result;
    }

    private void writeReplayedMarker(File sinkDir) {
        File marker = new File(sinkDir, SinkConstants.REPLAYED_MARKER);
        try {
            Files.write(marker.toPath(),
                    String.valueOf(System.currentTimeMillis()).getBytes(StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw DataXException.asDataXException(FrameworkErrorCode.DIRTY_RECORD_SINK_ERROR,
                    "_REPLAYED 标记写入失败: " + marker.getAbsolutePath(), e);
        }
    }
}
