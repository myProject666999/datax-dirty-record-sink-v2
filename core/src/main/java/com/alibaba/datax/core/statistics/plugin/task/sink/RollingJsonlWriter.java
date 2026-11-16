package com.alibaba.datax.core.statistics.plugin.task.sink;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * 单个 SinkChannel 专用的滚动 JSONL 写入器。每个实例只被一个 collector 对应的线程使用，
 * 不同 channel 写不同文件，因此并发 task 之间不会互相冲突。
 *
 * 文件约定：活动文件带 .writing 后缀；滚动/关闭时原子 rename 为最终 .jsonl 文件名。
 * manifest 只收录 finalize 后的文件，保证对外可见的 jsonl 都是完整文件。
 */
public class RollingJsonlWriter {

    private static final Logger LOG = LoggerFactory.getLogger(RollingJsonlWriter.class);

    private final File sinkDir;

    private final String filePrefix;

    private final long maxFileBytes;

    private final boolean flushEveryRecord;

    private int fileIndex = 0;

    private File currentWritingFile;

    private BufferedWriter currentWriter;

    private long currentFileBytes;

    private long currentFileRecords;

    private long currentFirstSequence = -1;

    private long currentLastSequence = -1;

    private MessageDigest digest;

    public RollingJsonlWriter(File sinkDir, String filePrefix, long maxFileBytes,
                              boolean flushEveryRecord) {
        this.sinkDir = sinkDir;
        this.filePrefix = filePrefix;
        this.maxFileBytes = maxFileBytes;
        this.flushEveryRecord = flushEveryRecord;
    }

    /**
     * 写入一行（不含换行符，方法内部补 \n），返回写入的字节数（UTF-8，含换行）。
     * 写完若达到文件大小阈值则滚动。
     */
    public synchronized long writeLine(String line, long sequence) {
        ensureOpen();
        byte[] payload = line.getBytes(StandardCharsets.UTF_8);
        try {
            currentWriter.write(line);
            currentWriter.newLine();
            if (flushEveryRecord) {
                currentWriter.flush();
            }
        } catch (IOException e) {
            throw DataXException.asDataXException(FrameworkErrorCode.DIRTY_RECORD_SINK_ERROR,
                    "脏数据落盘写入失败: " + currentWritingFile.getAbsolutePath(), e);
        }
        digest.update(payload);
        digest.update((byte) '\n');

        long bytes = payload.length + 1L;
        currentFileBytes += bytes;
        currentFileRecords++;
        if (currentFirstSequence < 0) {
            currentFirstSequence = sequence;
        }
        currentLastSequence = sequence;

        // 单条记录大于 maxFileBytes 时也保证它独占一个文件：先写后滚，不拆行
        if (currentFileBytes >= maxFileBytes) {
            rollover();
        }
        return bytes;
    }

    public synchronized void flush() {
        if (currentWriter == null) {
            return;
        }
        try {
            currentWriter.flush();
        } catch (IOException e) {
            throw DataXException.asDataXException(FrameworkErrorCode.DIRTY_RECORD_SINK_ERROR,
                    "脏数据落盘flush失败: " + currentWritingFile.getAbsolutePath(), e);
        }
    }

    /**
     * 关闭当前活动文件并 finalize（rename 为 .jsonl），返回清单条目；空文件返回 null。
     */
    public synchronized SinkFileEntry closeCurrentFile() {
        if (currentWriter == null) {
            return null;
        }
        try {
            currentWriter.flush();
            currentWriter.close();
        } catch (IOException e) {
            throw DataXException.asDataXException(FrameworkErrorCode.DIRTY_RECORD_SINK_ERROR,
                    "脏数据文件关闭失败: " + currentWritingFile.getAbsolutePath(), e);
        }

        if (currentFileRecords == 0) {
            // 空文件直接删除，不产生清单条目
            deleteQuietly(currentWritingFile);
            resetCurrent();
            return null;
        }

        String finalName = finalFileName(fileIndex);
        File finalFile = new File(sinkDir, finalName);
        try {
            Files.move(currentWritingFile.toPath(), finalFile.toPath());
        } catch (IOException e) {
            throw DataXException.asDataXException(FrameworkErrorCode.DIRTY_RECORD_SINK_ERROR,
                    String.format("脏数据文件重命名失败: %s -> %s",
                            currentWritingFile.getAbsolutePath(), finalFile.getAbsolutePath()), e);
        }

        SinkFileEntry entry = new SinkFileEntry(finalName, currentFileRecords,
                currentFileBytes, hexDigest(), currentFirstSequence, currentLastSequence);
        LOG.info("脏数据文件finalize: {}, records={}, bytes={}", finalName,
                currentFileRecords, currentFileBytes);
        resetCurrent();
        return entry;
    }

    private void rollover() {
        SinkFileEntry entry = closeCurrentFile();
        if (entry != null) {
            // 通过回调由 channel 转交给 manager
            if (closedFileConsumer != null) {
                closedFileConsumer.accept(entry);
            }
        }
    }

    private java.util.function.Consumer<SinkFileEntry> closedFileConsumer;

    public void setClosedFileConsumer(java.util.function.Consumer<SinkFileEntry> consumer) {
        this.closedFileConsumer = consumer;
    }

    private void ensureOpen() {
        if (currentWriter != null) {
            return;
        }
        fileIndex++;
        currentWritingFile = new File(sinkDir, writingFileName(fileIndex));
        try {
            this.currentWriter = new BufferedWriter(new OutputStreamWriter(
                    Files.newOutputStream(currentWritingFile.toPath(),
                            StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE),
                    StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw DataXException.asDataXException(FrameworkErrorCode.DIRTY_RECORD_SINK_ERROR,
                    "脏数据文件创建失败: " + currentWritingFile.getAbsolutePath(), e);
        }
        currentFileBytes = 0;
        currentFileRecords = 0;
        currentFirstSequence = -1;
        currentLastSequence = -1;
        try {
            this.digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw DataXException.asDataXException(FrameworkErrorCode.RUNTIME_ERROR, e);
        }
    }

    private void resetCurrent() {
        currentWriter = null;
        currentWritingFile = null;
        digest = null;
        currentFileBytes = 0;
        currentFileRecords = 0;
        currentFirstSequence = -1;
        currentLastSequence = -1;
    }

    private String writingFileName(int index) {
        return filePrefix + "." + sixDigits(index) + SinkConstants.WRITING_SUFFIX;
    }

    private String finalFileName(int index) {
        return filePrefix + "." + sixDigits(index) + SinkConstants.JSONL_SUFFIX;
    }

    private static String sixDigits(int index) {
        return String.format("%06d", index);
    }

    private String hexDigest() {
        StringBuilder sb = new StringBuilder();
        for (byte b : digest.digest()) {
            sb.append(String.format("%02x", b & 0xff));
        }
        return sb.toString();
    }

    private static void deleteQuietly(File file) {
        if (file != null && file.exists() && !file.delete()) {
            LOG.warn("空脏数据文件删除失败: {}", file.getAbsolutePath());
        }
    }
}
