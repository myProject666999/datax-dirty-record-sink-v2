package com.alibaba.datax.core.statistics.plugin.task.sink;

import com.alibaba.datax.common.util.Configuration;

import java.io.Serializable;

/**
 * 一条脱敏规则。columnName 与 columnIndex 至少配置一个：
 * 优先按列名解析列下标，解析不到时若显式配置了 columnIndex 则使用下标。
 */
public class MaskRule implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String columnName;

    private final Integer columnIndex;

    private final String strategy;

    private final String replacement;

    private final String salt;

    public MaskRule(String columnName, Integer columnIndex, String strategy,
                    String replacement, String salt) {
        this.columnName = columnName;
        this.columnIndex = columnIndex;
        this.strategy = strategy;
        this.replacement = replacement;
        this.salt = salt;
    }

    public static MaskRule from(Configuration ruleConf) {
        String columnName = ruleConf.getString("columnName");
        Integer columnIndex = ruleConf.get("columnIndex", Integer.class);
        String strategy = ruleConf.getString("strategy");
        String replacement = ruleConf.getString("replacement");
        String salt = ruleConf.getString("salt");
        return new MaskRule(columnName, columnIndex, strategy, replacement, salt);
    }

    public String getColumnName() {
        return columnName;
    }

    public Integer getColumnIndex() {
        return columnIndex;
    }

    public String getStrategy() {
        return strategy;
    }

    public String getReplacement() {
        return replacement;
    }

    public String getSalt() {
        return salt;
    }
}
