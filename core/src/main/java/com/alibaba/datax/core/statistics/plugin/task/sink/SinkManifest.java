package com.alibaba.datax.core.statistics.plugin.task.sink;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * manifest 的内存模型与原子读写。manifest 只收录 finalize 后的完整 jsonl 文件。
 */
public class SinkManifest {

    private static final Logger LOG = LoggerFactory.getLogger(SinkManifest.class);

    private int version = SinkConstants.LINE_VERSION;

    private long jobId;

    private String sinkName;

    private long createdAt;

    private long closedAt;

    private String jobStatus;

    private long totalSizeCapBytes;

    private boolean capReached;

    private long droppedRecords;

    private List<Integer> maskedColumns = new ArrayList<Integer>();

    private List<SinkFileEntry> files = new ArrayList<SinkFileEntry>();

    public int getVersion() {
        return version;
    }

    public void setJobId(long jobId) {
        this.jobId = jobId;
    }

    public void setSinkName(String sinkName) {
        this.sinkName = sinkName;
    }

    public void setCreatedAt(long createdAt) {
        this.createdAt = createdAt;
    }

    public void setClosedAt(long closedAt) {
        this.closedAt = closedAt;
    }

    public void setJobStatus(String jobStatus) {
        this.jobStatus = jobStatus;
    }

    public void setTotalSizeCapBytes(long totalSizeCapBytes) {
        this.totalSizeCapBytes = totalSizeCapBytes;
    }

    public void setCapReached(boolean capReached) {
        this.capReached = capReached;
    }

    public void setDroppedRecords(long droppedRecords) {
        this.droppedRecords = droppedRecords;
    }

    public void setMaskedColumns(List<Integer> maskedColumns) {
        this.maskedColumns = maskedColumns;
    }

    public synchronized void addFileEntry(SinkFileEntry entry) {
        files.add(entry);
    }

    public synchronized List<SinkFileEntry> getFiles() {
        return new ArrayList<SinkFileEntry>(files);
    }

    public long getTotalRecords() {
        long total = 0;
        for (SinkFileEntry entry : files) {
            total += entry.getRecords();
        }
        return total;
    }

    public long getTotalBytes() {
        long total = 0;
        for (SinkFileEntry entry : files) {
            total += entry.getBytes();
        }
        return total;
    }

    public String getJobStatus() {
        return jobStatus;
    }

    public boolean isCapReached() {
        return capReached;
    }

    public long getDroppedRecords() {
        return droppedRecords;
    }

    private Map<String, Object> toMap() {
        Map<String, Object> map = new LinkedHashMap<String, Object>();
        map.put(SinkConstants.FIELD_VERSION, version);
        map.put(SinkConstants.FIELD_JOB_ID, jobId);
        map.put(SinkConstants.MANIFEST_SINK_NAME, sinkName);
        map.put(SinkConstants.MANIFEST_CREATED_AT, createdAt);
        map.put(SinkConstants.MANIFEST_CLOSED_AT, closedAt);
        map.put(SinkConstants.MANIFEST_JOB_STATUS, jobStatus);
        map.put(SinkConstants.MANIFEST_TOTAL_FILES, files.size());
        map.put(SinkConstants.MANIFEST_TOTAL_RECORDS, getTotalRecords());
        map.put(SinkConstants.MANIFEST_TOTAL_BYTES, getTotalBytes());
        map.put(SinkConstants.MANIFEST_TOTAL_SIZE_CAP, totalSizeCapBytes);
        map.put(SinkConstants.MANIFEST_CAP_REACHED, capReached);
        map.put(SinkConstants.MANIFEST_DROPPED_RECORDS, droppedRecords);
        map.put(SinkConstants.MANIFEST_MASKED_COLUMNS, maskedColumns);

        List<Map<String, Object>> fileList = new ArrayList<Map<String, Object>>();
        for (SinkFileEntry entry : files) {
            Map<String, Object> f = new LinkedHashMap<String, Object>();
            f.put(SinkConstants.MANIFEST_ENTRY_FILE_NAME, entry.getFileName());
            f.put(SinkConstants.MANIFEST_FILE_RECORDS, entry.getRecords());
            f.put(SinkConstants.MANIFEST_FILE_BYTES, entry.getBytes());
            f.put(SinkConstants.MANIFEST_FILE_SHA256, entry.getSha256());
            f.put(SinkConstants.MANIFEST_FILE_FIRST_SEQ, entry.getFirstSequence());
            f.put(SinkConstants.MANIFEST_FILE_LAST_SEQ, entry.getLastSequence());
            fileList.add(f);
        }
        map.put(SinkConstants.MANIFEST_FILES, fileList);
        return map;
    }

    /**
     * 原子写：先写 manifest.json.tmp 再同目录 rename，避免留下写了一半的 manifest。
     */
    public void writeAtomically(File sinkDir) {
        File target = new File(sinkDir, SinkConstants.MANIFEST_FILE_NAME);
        File tmp = new File(sinkDir,
                SinkConstants.MANIFEST_FILE_NAME + SinkConstants.MANIFEST_TMP_SUFFIX);
        byte[] payload = JSON.toJSONString(toMap()).getBytes(StandardCharsets.UTF_8);
        try {
            Files.write(tmp.toPath(), payload);
            try {
                Files.move(tmp.toPath(), target.toPath(),
                        java.nio.file.StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException moveError) {
                deleteQuietly(tmp);
                throw moveError;
            }
        } catch (IOException e) {
            throw DataXException.asDataXException(FrameworkErrorCode.DIRTY_RECORD_SINK_ERROR,
                    "manifest.json 写入失败: " + target.getAbsolutePath(), e);
        }
    }

    public static SinkManifest read(File manifestFile) {
        try {
            JSONObject obj = JSON.parseObject(new String(
                    Files.readAllBytes(manifestFile.toPath()), StandardCharsets.UTF_8));
            SinkManifest manifest = new SinkManifest();
            manifest.version = obj.getIntValue(SinkConstants.FIELD_VERSION);
            manifest.jobId = obj.getLongValue(SinkConstants.FIELD_JOB_ID);
            manifest.sinkName = obj.getString(SinkConstants.MANIFEST_SINK_NAME);
            manifest.createdAt = obj.getLongValue(SinkConstants.MANIFEST_CREATED_AT);
            manifest.closedAt = obj.getLongValue(SinkConstants.MANIFEST_CLOSED_AT);
            manifest.jobStatus = obj.getString(SinkConstants.MANIFEST_JOB_STATUS);
            manifest.totalSizeCapBytes = obj.getLongValue(SinkConstants.MANIFEST_TOTAL_SIZE_CAP);
            manifest.capReached = obj.getBooleanValue(SinkConstants.MANIFEST_CAP_REACHED);
            manifest.droppedRecords = obj.getLongValue(SinkConstants.MANIFEST_DROPPED_RECORDS);
            manifest.maskedColumns = obj.getJSONArray(SinkConstants.MANIFEST_MASKED_COLUMNS)
                    .toJavaList(Integer.class);
            List<JSONObject> fileObjs = obj.getJSONArray(SinkConstants.MANIFEST_FILES)
                    .toJavaList(JSONObject.class);
            for (JSONObject f : fileObjs) {
                manifest.files.add(new SinkFileEntry(
                        f.getString(SinkConstants.MANIFEST_ENTRY_FILE_NAME),
                        f.getLongValue(SinkConstants.MANIFEST_FILE_RECORDS),
                        f.getLongValue(SinkConstants.MANIFEST_FILE_BYTES),
                        f.getString(SinkConstants.MANIFEST_FILE_SHA256),
                        f.getLongValue(SinkConstants.MANIFEST_FILE_FIRST_SEQ),
                        f.getLongValue(SinkConstants.MANIFEST_FILE_LAST_SEQ)));
            }
            return manifest;
        } catch (IOException e) {
            throw DataXException.asDataXException(FrameworkErrorCode.REPLAY_MANIFEST_ERROR,
                    "manifest.json 读取失败: " + manifestFile.getAbsolutePath(), e);
        } catch (Exception e) {
            throw DataXException.asDataXException(FrameworkErrorCode.REPLAY_MANIFEST_ERROR,
                    "manifest.json 内容损坏: " + manifestFile.getAbsolutePath(), e);
        }
    }

    private static void deleteQuietly(File file) {
        try {
            Files.deleteIfExists(file.toPath());
        } catch (IOException e) {
            LOG.warn("临时文件删除失败: {}", file.getAbsolutePath());
        }
    }
}
