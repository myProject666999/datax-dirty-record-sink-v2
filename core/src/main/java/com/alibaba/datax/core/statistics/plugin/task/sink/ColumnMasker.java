package com.alibaba.datax.core.statistics.plugin.task.sink;

/**
 * 列脱敏器。脱敏在脏数据序列化之前执行，原始值不会进入任何落盘字节。
 * 实现必须保证不可逆：不能保留可还原原值的密钥或映射。
 */
public interface ColumnMasker {

    /**
     * @return 脱敏策略名，写入 JSONL 的 maskStrategy 字段
     */
    String getStrategy();

    /**
     * 对原始值做脱敏。
     *
     * @param original 原始列值（null 由调用方保证不会传入，null 列保持 NULL 语义不脱敏）
     * @return 脱敏后的字符串值，落盘为 STRING 列
     */
    String mask(String original);
}
