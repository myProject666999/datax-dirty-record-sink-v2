package com.alibaba.datax.core.statistics.plugin.task.sink;

/**
 * 一个已 finalize 的 jsonl 文件的清单条目。
 */
public class SinkFileEntry {

    private final String fileName;

    private final long records;

    private final long bytes;

    private final String sha256;

    private final long firstSequence;

    private final long lastSequence;

    public SinkFileEntry(String fileName, long records, long bytes, String sha256,
                         long firstSequence, long lastSequence) {
        this.fileName = fileName;
        this.records = records;
        this.bytes = bytes;
        this.sha256 = sha256;
        this.firstSequence = firstSequence;
        this.lastSequence = lastSequence;
    }

    public String getFileName() {
        return fileName;
    }

    public long getRecords() {
        return records;
    }

    public long getBytes() {
        return bytes;
    }

    public String getSha256() {
        return sha256;
    }

    public long getFirstSequence() {
        return firstSequence;
    }

    public long getLastSequence() {
        return lastSequence;
    }
}
