package com.alibaba.datax.core.statistics.plugin.task.sink;

/**
 * 固定值替换脱敏，原值被完全丢弃，不可逆。
 */
public class ReplaceColumnMasker implements ColumnMasker {

    public static final String DEFAULT_REPLACEMENT = "***";

    private final String replacement;

    public ReplaceColumnMasker(String replacement) {
        this.replacement = replacement == null ? DEFAULT_REPLACEMENT : replacement;
    }

    @Override
    public String getStrategy() {
        return SinkConstants.MASK_STRATEGY_REPLACE;
    }

    @Override
    public String mask(String original) {
        return replacement;
    }
}
