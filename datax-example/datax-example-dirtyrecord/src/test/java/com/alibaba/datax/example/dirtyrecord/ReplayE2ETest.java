package com.alibaba.datax.example.dirtyrecord;

import com.alibaba.datax.core.replay.DirtyRecordReplayEngine;
import com.alibaba.datax.core.replay.ReplayConfig;
import com.alibaba.datax.core.replay.ReplayManifestReader;
import com.alibaba.datax.core.statistics.plugin.task.sink.SinkConstants;
import com.alibaba.datax.core.util.container.CoreConstant;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * 回放全场景：正常回放、损坏文件报错、重复回放拦截与 --force、回放再脏数据再次落盘。
 * 测试内搭建临时 DATAX_HOME（conf/core.json + plugin/writer/dirtydemowriter）。
 */
public class ReplayE2ETest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private String oldDataxHome;

    private String oldDataxHomeField;

    private String oldConfPath;

    private String oldWriterHome;

    private String oldReaderHome;

    private File dataxHome;

    private File pluginClassesDir;

    @Before
    public void setUp() throws Exception {
        oldDataxHome = System.getProperty("datax.home");
        oldDataxHomeField = CoreConstant.DATAX_HOME;
        oldConfPath = CoreConstant.DATAX_CONF_PATH;
        oldWriterHome = CoreConstant.DATAX_PLUGIN_WRITER_HOME;
        oldReaderHome = CoreConstant.DATAX_PLUGIN_READER_HOME;
        dataxHome = folder.newFolder("fake-datax-home");
        new File(dataxHome, "conf").mkdirs();
        File confTemplate = new File(DirtyE2EHelper.resourcePath("example-core.json"));
        Files.copy(confTemplate.toPath(),
                new File(dataxHome, "conf/core.json").toPath(),
                StandardCopyOption.REPLACE_EXISTING);

        File writerPluginDir = new File(dataxHome, "plugin/writer/dirtydemowriter");
        writerPluginDir.mkdirs();
        pluginClassesDir = locatePluginClassesDir();
        // 拷贝 plugin.json；JarLoader 以插件目录为 path，测试插件类在测试 classpath 上可被父加载器加载
        Files.copy(new File(pluginClassesDir, "plugin2/plugin.json").toPath(),
                new File(writerPluginDir, "plugin.json").toPath(),
                StandardCopyOption.REPLACE_EXISTING);

        System.setProperty("datax.home", dataxHome.getAbsolutePath());
        CoreConstant.DATAX_HOME = dataxHome.getAbsolutePath();
        CoreConstant.DATAX_CONF_PATH = new File(dataxHome, "conf/core.json").getAbsolutePath();
        CoreConstant.DATAX_PLUGIN_WRITER_HOME =
                new File(dataxHome, "plugin/writer").getAbsolutePath() + File.separator;
        CoreConstant.DATAX_PLUGIN_READER_HOME =
                new File(dataxHome, "plugin/reader").getAbsolutePath() + File.separator;
    }

    @After
    public void tearDown() {
        if (oldDataxHome == null) {
            System.clearProperty("datax.home");
        } else {
            System.setProperty("datax.home", oldDataxHome);
        }
        CoreConstant.DATAX_HOME = oldDataxHomeField;
        CoreConstant.DATAX_CONF_PATH = oldConfPath;
        CoreConstant.DATAX_PLUGIN_WRITER_HOME = oldWriterHome;
        CoreConstant.DATAX_PLUGIN_READER_HOME = oldReaderHome;
    }

    /**
     * 找到本模块 target/classes（其中含 plugin/plugin.json 与插件类）。
     */
    private File locatePluginClassesDir() {
        // 测试工作目录通常为模块根目录
        File candidate = new File("target/classes");
        if (new File(candidate, "plugin2/plugin.json").exists()) {
            return candidate.getAbsoluteFile();
        }
        throw new IllegalStateException("未找到 target/classes/plugin2/plugin.json，请先编译测试模块");
    }

    private File runSourceJobAndGetSink() throws Exception {
        String sinkName = "replay-source";
        File outputFile = folder.newFile("writer-output.txt");
        File template = new File(DirtyE2EHelper.resourcePath("replay-source-job.json"));
        String content = new String(Files.readAllBytes(template.toPath()), StandardCharsets.UTF_8)
                .replace("${SINK_ROOT}", folder.getRoot().getAbsolutePath().replace("\\", "/"))
                .replace("${SINK_NAME}", sinkName)
                .replace("${OUTPUT_FILE}", outputFile.getAbsolutePath().replace("\\", "/"));
        File jobFile = folder.newFile("source-job.json");
        Files.write(jobFile.toPath(), content.getBytes(StandardCharsets.UTF_8));
        DirtyE2EHelper.runJob(jobFile.getAbsolutePath());

        File sinkDir = DirtyE2EHelper.sinkDir(folder.getRoot(), sinkName);
        assertTrue(new File(sinkDir, SinkConstants.MANIFEST_FILE_NAME).exists());
        return sinkDir;
    }

    @Test
    public void testNormalReplayFeedsOnlyDirtyRecords() throws Exception {
        File sinkDir = runSourceJobAndGetSink();
        File replayOutput = folder.newFile("replay-output.txt");
        File replayJob = buildReplayJob(replayOutput, java.util.Collections.<Long>emptyList());

        ReplayConfig config = new ReplayConfig();
        config.setSinkDir(sinkDir);
        config.setJobFile(replayJob);
        new DirtyRecordReplayEngine().replay(config);

        // 原始 writer 落盘的脏数据是 id=2 和 id=4，回放后应只写入这两条
        List<String> lines = Files.readAllLines(replayOutput.toPath(), StandardCharsets.UTF_8);
        assertEquals(2, lines.size());
        assertTrue(lines.contains("2"));
        assertTrue(lines.contains("4"));
        assertTrue(new File(sinkDir, SinkConstants.REPLAYED_MARKER).exists());
    }

    @Test
    public void testCorruptFileReportedWithLocation() throws Exception {
        File sinkDir = runSourceJobAndGetSink();
        // 找一个 jsonl 文件追加截断的坏行，并移除 manifest 以进入严格解析路径
        // （manifest 存在时损坏会先被 sha256 校验拦截，这里验证 JSONL 解析器自身的定位能力）
        File target = DirtyE2EHelper.jsonlFiles(sinkDir).get(0);
        Files.write(target.toPath(),
                "garbage-not-json".getBytes(StandardCharsets.UTF_8),
                java.nio.file.StandardOpenOption.APPEND);
        assertTrue(new File(sinkDir, SinkConstants.MANIFEST_FILE_NAME).delete());

        File replayOutput = folder.newFile("corrupt-output.txt");
        File replayJob = buildReplayJob(replayOutput, java.util.Collections.<Long>emptyList());
        ReplayConfig config = new ReplayConfig();
        config.setSinkDir(sinkDir);
        config.setJobFile(replayJob);
        config.setAllowManifestMissing(true);
        try {
            new DirtyRecordReplayEngine().replay(config);
            fail("损坏文件应报错");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(target.getName()));
            assertTrue(e.getMessage().contains("line="));
            assertTrue(e.getMessage().contains("byteOffset="));
        }
    }

    @Test
    public void testDuplicateReplayBlockedThenForce() throws Exception {
        File sinkDir = runSourceJobAndGetSink();
        File replayOutput = folder.newFile("dup-output.txt");
        File replayJob = buildReplayJob(replayOutput, java.util.Collections.<Long>emptyList());

        ReplayConfig first = new ReplayConfig();
        first.setSinkDir(sinkDir);
        first.setJobFile(replayJob);
        new DirtyRecordReplayEngine().replay(first);

        ReplayConfig second = new ReplayConfig();
        second.setSinkDir(sinkDir);
        second.setJobFile(replayJob);
        try {
            new DirtyRecordReplayEngine().replay(second);
            fail("第二次回放无 --force 应被拦截");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains(SinkConstants.REPLAYED_MARKER));
        }

        second.setForce(true);
        second.setAllowManifestMissing(true); // 标记已存在但 manifest 仍在，这里仅验证 force 不报错
        new DirtyRecordReplayEngine().replay(second);
    }

    @Test
    public void testReDirtyRecordsReSinkToNewDirectory() throws Exception {
        File sinkDir = runSourceJobAndGetSink();
        File replayOutput = folder.newFile("redirty-output.txt");
        // 回放时让 id=2 再次变脏，id=4 修复后成功
        File replayJob = buildReplayJob(replayOutput, java.util.Arrays.asList(2L));

        ReplayConfig config = new ReplayConfig();
        config.setSinkDir(sinkDir);
        config.setJobFile(replayJob);
        new DirtyRecordReplayEngine().replay(config);

        // 只有 id=4 成功写入 writer
        List<String> lines = Files.readAllLines(replayOutput.toPath(), StandardCharsets.UTF_8);
        assertEquals(1, lines.size());
        assertEquals("4", lines.get(0));

        // 再脏数据进入独立的 replay-redirty 目录，且带 replayOf 字段
        File[] redirtyDirs = sinkDir.listFiles(new java.io.FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.isDirectory() && pathname.getName().startsWith("replay-redirty-");
            }
        });
        assertEquals(1, redirtyDirs.length);
        File reSinkDir = new File(redirtyDirs[0], sinkDir.getName() + "-replay");
        assertTrue(new File(reSinkDir, SinkConstants.MANIFEST_FILE_NAME).exists());
        boolean sawReplayOf = false;
        for (String line : DirtyE2EHelper.allJsonlLines(reSinkDir)) {
            com.alibaba.fastjson2.JSONObject obj = com.alibaba.fastjson2.JSON.parseObject(line);
            if (sinkDir.getName().equals(obj.getString(SinkConstants.FIELD_REPLAY_OF))) {
                sawReplayOf = true;
            }
        }
        assertTrue("再脏数据行应带 replayOf 字段", sawReplayOf);
    }

    private File buildReplayJob(File outputFile, List<Long> reDirtyValues) throws Exception {
        String jobJson = "{"
                + "\"job\":{\"content\":[{\"reader\":{\"name\":\"dirtydemoreader\",\"parameter\":{}},"
                + "\"writer\":{\"name\":\"dirtydemowriter\",\"parameter\":{"
                + "\"collectOutputPath\":\"" + outputFile.getAbsolutePath().replace("\\", "/") + "\","
                + "\"reDirtyValues\":" + com.alibaba.fastjson2.JSON.toJSONString(reDirtyValues)
                + "}}}]}}" ;
        File jobFile = folder.newFile("replay-job-" + System.nanoTime() + ".json");
        Files.write(jobFile.toPath(), jobJson.getBytes(StandardCharsets.UTF_8));
        return jobFile;
    }

    @Test
    public void testManifestReaderRejectsUnknownSinkDir() {
        try {
            new ReplayManifestReader().prepare(
                    new File(folder.getRoot(), "not-exist"), false, false);
            fail("不存在的 sink 目录应报错");
        } catch (Exception e) {
            // 期望异常
            assertFalse(e.getMessage() == null);
        }
    }
}
