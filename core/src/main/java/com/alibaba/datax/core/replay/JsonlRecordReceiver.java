package com.alibaba.datax.core.replay;

import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.core.statistics.plugin.task.sink.SinkConstants;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;

/**
 * 顺序读取 sink 目录下已 finalize 的 jsonl 文件并还原为 Record。
 * 任何解析/字段错误都抛出 REPLAY_FILE_CORRUPT，并给出 文件名+行号+字节偏移 精确定位；
 * 被截断的末行也会报错，绝不静默跳过。
 */
public class JsonlRecordReceiver implements RecordReceiver {

    private final List<File> files;

    private final JsonlRecordCodec codec = new JsonlRecordCodec();

    private int fileIndex = -1;

    private BufferedReader currentReader;

    private File currentFile;

    private int currentLineNo;

    private long currentLineStartOffset;

    private long currentFileOffset;

    public JsonlRecordReceiver(List<File> orderedFiles) {
        this.files = orderedFiles;
    }

    @Override
    public Record getFromReader() {
        while (true) {
            if (currentReader == null) {
                if (!openNextFile()) {
                    return null;
                }
            }
            String line;
            try {
                line = currentReader.readLine();
            } catch (IOException e) {
                throw corrupt("读取IO异常: " + e.getMessage());
            }
            if (line == null) {
                closeCurrentReader();
                continue;
            }
            long lineStart = currentFileOffset;
            int lineNo = currentLineNo + 1;
            currentLineNo = lineNo;
            currentLineStartOffset = lineStart;
            currentFileOffset += line.getBytes(StandardCharsets.UTF_8).length + 1L;

            if (StringUtils.isBlank(line)) {
                throw corrupt("第" + lineNo + "行为空行，JSONL不允许空行");
            }

            JSONObject jsonLine;
            try {
                jsonLine = JSON.parseObject(line);
            } catch (Exception e) {
                throw corrupt("第" + lineNo + "行不是合法JSON（文件可能被截断或损坏）: "
                        + e.getMessage());
            }
            validateRequiredFields(jsonLine, lineNo);
            try {
                return codec.toRecord(jsonLine);
            } catch (DataXException e) {
                throw corrupt(e.getMessage());
            }
        }
    }

    private void validateRequiredFields(JSONObject jsonLine, int lineNo) {
        if (jsonLine.get(SinkConstants.FIELD_VERSION) == null) {
            throw corrupt("第" + lineNo + "行缺少 version 字段");
        }
        if (jsonLine.get(SinkConstants.FIELD_COLUMNS) == null) {
            throw corrupt("第" + lineNo + "行缺少 columns 字段");
        }
        if (StringUtils.isBlank(jsonLine.getString(SinkConstants.FIELD_PLUGIN_TYPE))) {
            throw corrupt("第" + lineNo + "行缺少 pluginType 字段");
        }
    }

    private boolean openNextFile() {
        closeCurrentReader();
        fileIndex++;
        if (fileIndex >= files.size()) {
            return false;
        }
        currentFile = files.get(fileIndex);
        currentLineNo = 0;
        currentFileOffset = 0;
        currentLineStartOffset = 0;
        try {
            currentReader = new BufferedReader(new InputStreamReader(
                    Files.newInputStream(currentFile.toPath()), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw DataXException.asDataXException(FrameworkErrorCode.REPLAY_FILE_CORRUPT,
                    "无法打开脏数据文件: " + currentFile.getAbsolutePath(), e);
        }
        return true;
    }

    private void closeCurrentReader() {
        if (currentReader != null) {
            try {
                currentReader.close();
            } catch (IOException ignored) {
                // 关闭异常不影响后续文件
            }
            currentReader = null;
        }
    }

    private DataXException corrupt(String reason) {
        return DataXException.asDataXException(FrameworkErrorCode.REPLAY_FILE_CORRUPT,
                String.format("脏数据文件损坏: file=%s, line=%d, byteOffset=%d, reason=%s",
                        currentFile == null ? "?" : currentFile.getAbsolutePath(),
                        currentLineNo, currentLineStartOffset, reason));
    }

    @Override
    public void shutdown() {
        closeCurrentReader();
    }
}
