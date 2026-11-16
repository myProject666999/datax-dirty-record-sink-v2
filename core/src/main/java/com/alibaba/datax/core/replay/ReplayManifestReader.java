package com.alibaba.datax.core.replay;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.statistics.plugin.task.sink.SinkConstants;
import com.alibaba.datax.core.statistics.plugin.task.sink.SinkFileEntry;
import com.alibaba.datax.core.statistics.plugin.task.sink.SinkManifest;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 回放前的 sink 目录校验：重复回放标记、manifest 校验和、文件清单一致性、
 * .writing 残留检测。产出按文件名排序的待回放 jsonl 文件列表。
 */
public class ReplayManifestReader {

    private static final Logger LOG = LoggerFactory.getLogger(ReplayManifestReader.class);

    public static class ReplayPlan {
        private final List<File> orderedFiles;
        private final SinkManifest manifest;

        ReplayPlan(List<File> orderedFiles, SinkManifest manifest) {
            this.orderedFiles = orderedFiles;
            this.manifest = manifest;
        }

        public List<File> getOrderedFiles() {
            return orderedFiles;
        }

        public SinkManifest getManifest() {
            return manifest;
        }
    }

    public ReplayPlan prepare(File sinkDir, boolean force, boolean allowManifestMissing) {
        if (!sinkDir.exists() || !sinkDir.isDirectory()) {
            throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,
                    "脏数据落盘目录不存在或不是目录: " + sinkDir.getAbsolutePath());
        }

        // 1. 重复回放防护
        File replayedMarker = new File(sinkDir, SinkConstants.REPLAYED_MARKER);
        if (replayedMarker.exists() && !force) {
            throw DataXException.asDataXException(FrameworkErrorCode.REPLAY_DUPLICATE_SINK,
                    "sink目录[" + sinkDir.getAbsolutePath() + "]已存在 " + SinkConstants.REPLAYED_MARKER
                            + " 标记，说明已成功回放；确认需要重复回放请加 --force");
        }

        // 2. .writing 残留：崩溃留下的不完整文件，严格模式下直接报错
        File[] writingFiles = sinkDir.listFiles(new java.io.FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().endsWith(SinkConstants.WRITING_SUFFIX);
            }
        });
        if (writingFiles != null && writingFiles.length > 0) {
            throw DataXException.asDataXException(FrameworkErrorCode.REPLAY_FILE_CORRUPT,
                    "发现未正常关闭的活动文件（可能存在写了一半的记录），请人工检查后删除: "
                            + Arrays.toString(writingFiles));
        }

        File manifestFile = new File(sinkDir, SinkConstants.MANIFEST_FILE_NAME);
        File[] jsonlOnDisk = listJsonlFiles(sinkDir);

        if (!manifestFile.exists()) {
            if (!allowManifestMissing) {
                throw DataXException.asDataXException(FrameworkErrorCode.REPLAY_MANIFEST_ERROR,
                        "sink目录缺少 manifest.json，无法证明文件完整性；如确认要直接按jsonl回放，"
                                + "请显式加 --allow-manifest-missing。目录: "
                                + sinkDir.getAbsolutePath());
            }
            LOG.warn("manifest.json 缺失，已按 --allow-manifest-missing 降级回放，"
                    + "跳过sha256与记录数校验，请人工确认 jsonl 文件完整: {}", sinkDir.getAbsolutePath());
            return new ReplayPlan(sortFiles(jsonlOnDisk), null);
        }

        SinkManifest manifest = SinkManifest.read(manifestFile);

        // 3. 清单与磁盘文件一致性
        Set<String> manifestNames = new HashSet<String>();
        for (SinkFileEntry entry : manifest.getFiles()) {
            manifestNames.add(entry.getFileName());
        }
        Set<String> diskNames = new HashSet<String>();
        for (File f : jsonlOnDisk) {
            diskNames.add(f.getName());
        }
        if (!manifestNames.equals(diskNames)) {
            Set<String> onlyManifest = new HashSet<String>(manifestNames);
            onlyManifest.removeAll(diskNames);
            Set<String> onlyDisk = new HashSet<String>(diskNames);
            onlyDisk.removeAll(manifestNames);
            throw DataXException.asDataXException(FrameworkErrorCode.REPLAY_MANIFEST_ERROR,
                    String.format("manifest与磁盘文件清单不一致。仅manifest有:%s，仅磁盘有:%s",
                            onlyManifest, onlyDisk));
        }

        // 4. 逐文件校验大小与 sha256（回放前失败，避免回放到一半才发现损坏）
        for (SinkFileEntry entry : manifest.getFiles()) {
            File file = new File(sinkDir, entry.getFileName());
            if (file.length() != entry.getBytes()) {
                throw DataXException.asDataXException(FrameworkErrorCode.REPLAY_MANIFEST_ERROR,
                        String.format("文件[%s]大小不一致: manifest=%d, 实际=%d",
                                entry.getFileName(), entry.getBytes(), file.length()));
            }
            String actualSha = sha256Hex(file);
            if (!actualSha.equalsIgnoreCase(entry.getSha256())) {
                throw DataXException.asDataXException(FrameworkErrorCode.REPLAY_MANIFEST_ERROR,
                        String.format("文件[%s]sha256校验失败: manifest=%s, 实际=%s",
                                entry.getFileName(), entry.getSha256(), actualSha));
            }
        }
        LOG.info("sink目录[{}]校验通过，待回放文件{}个，记录{}条",
                sinkDir.getAbsolutePath(), manifest.getFiles().size(), manifest.getTotalRecords());
        return new ReplayPlan(sortFiles(jsonlOnDisk), manifest);
    }

    private File[] listJsonlFiles(File sinkDir) {
        File[] files = sinkDir.listFiles(new java.io.FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.isFile()
                        && pathname.getName().endsWith(SinkConstants.JSONL_SUFFIX);
            }
        });
        return files == null ? new File[0] : files;
    }

    private List<File> sortFiles(File[] files) {
        List<File> list = new ArrayList<File>(Arrays.asList(files));
        list.sort(Comparator.comparing(File::getName));
        return list;
    }

    private String sha256Hex(File file) {
        try (InputStream in = new FileInputStream(file)) {
            return DigestUtils.sha256Hex(in);
        } catch (IOException e) {
            throw DataXException.asDataXException(FrameworkErrorCode.REPLAY_MANIFEST_ERROR,
                    "计算文件sha256失败: " + file.getAbsolutePath(), e);
        }
    }
}
