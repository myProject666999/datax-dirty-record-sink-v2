package com.alibaba.datax.core.statistics.plugin.task.sink;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import org.apache.commons.lang3.StringUtils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 解析 "64MB"、"1GB"、"1024" 这类字节大小配置。
 */
public final class ByteSizeParser {

    private static final Pattern SIZE_PATTERN = Pattern.compile(
            "^\\s*(\\d+(?:\\.\\d+)?)\\s*(B|KB|MB|GB|TB)?\\s*$", Pattern.CASE_INSENSITIVE);

    private ByteSizeParser() {
    }

    public static long parse(String text, long defaultValue) {
        if (StringUtils.isBlank(text)) {
            return defaultValue;
        }
        return parse(text);
    }

    public static long parse(String text) {
        Matcher matcher = SIZE_PATTERN.matcher(text);
        if (!matcher.matches()) {
            throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,
                    String.format("非法的大小配置[%s]，示例: 64MB、1GB、1024", text));
        }
        double value = Double.parseDouble(matcher.group(1));
        String unit = matcher.group(2);
        double factor = 1L;
        if (unit != null) {
            switch (unit.toUpperCase()) {
                case "B":
                    factor = 1L;
                    break;
                case "KB":
                    factor = 1024L;
                    break;
                case "MB":
                    factor = 1024L * 1024L;
                    break;
                case "GB":
                    factor = 1024L * 1024L * 1024L;
                    break;
                case "TB":
                    factor = 1024L * 1024L * 1024L * 1024L;
                    break;
                default:
                    break;
            }
        }
        long result = (long) (value * factor);
        if (result <= 0) {
            throw DataXException.asDataXException(FrameworkErrorCode.CONFIG_ERROR,
                    String.format("大小配置[%s]必须为正数", text));
        }
        return result;
    }
}
