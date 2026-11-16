package com.alibaba.datax.example.dirtyrecord;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.example.ExampleContainer;
import com.alibaba.datax.example.util.PathUtil;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

/**
 * E2E 测试辅助：构造作业配置、定位 sink 文件、读取 jsonl 行。
 */
public final class DirtyE2EHelper {

    private DirtyE2EHelper() {
    }

    public static String resourcePath(String name) {
        return PathUtil.getAbsolutePathFromClassPath("/" + name);
    }

    /**
     * 读取 classpath 上的作业模板并做占位符替换后写到临时文件，返回可直接运行的作业路径。
     */
    public static String materializeJob(String templateName, String sinkRoot, String sinkName,
                                        String extraReplacementsJson) throws IOException {
        String template = new String(Files.readAllBytes(
                new File(resourcePath(templateName)).toPath()), StandardCharsets.UTF_8);
        String content = template
                .replace("${SINK_ROOT}", sinkRoot.replace("\\", "/"))
                .replace("${SINK_NAME}", sinkName);
        if (extraReplacementsJson != null) {
            content = content.replace("${EXTRA}", extraReplacementsJson);
        }
        File jobFile = File.createTempFile("dirty-e2e-job-", ".json");
        jobFile.deleteOnExit();
        Files.write(jobFile.toPath(), content.getBytes(StandardCharsets.UTF_8));
        return jobFile.getAbsolutePath();
    }

    public static void runJob(String jobPath) {
        ExampleContainer.start(jobPath);
    }

    public static File sinkDir(File root, String sinkName) {
        return new File(root, sinkName);
    }

    public static List<File> jsonlFiles(File sinkDir) {
        List<File> result = new ArrayList<File>();
        File[] files = sinkDir.listFiles(new java.io.FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.isFile()
                        && pathname.getName().endsWith(".jsonl");
            }
        });
        if (files != null) {
            for (File f : files) {
                result.add(f);
            }
        }
        result.sort(java.util.Comparator.comparing(File::getName));
        return result;
    }

    public static List<String> allJsonlLines(File sinkDir) throws IOException {
        List<String> lines = new ArrayList<String>();
        for (File f : jsonlFiles(sinkDir)) {
            lines.addAll(Files.readAllLines(f.toPath(), StandardCharsets.UTF_8));
        }
        return lines;
    }

    public static boolean anyLineContains(File sinkDir, String token) throws IOException {
        for (String line : allJsonlLines(sinkDir)) {
            if (line.contains(token)) {
                return true;
            }
        }
        return false;
    }

    public static Configuration parseJob(String jobPath) {
        return Configuration.from(new File(jobPath));
    }
}
