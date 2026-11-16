package com.alibaba.datax.core.statistics.plugin.task.sink;

import org.apache.commons.codec.digest.DigestUtils;

/**
 * SHA-256 单向哈希脱敏。落盘内容只有摘要，原值在计算上不可还原；
 * 可选静态盐（只存在于 job 配置中，绝不写入 sink）用于抵御低基数列的预计算表攻击。
 */
public class Sha256ColumnMasker implements ColumnMasker {

    private final String salt;

    public Sha256ColumnMasker(String salt) {
        this.salt = salt;
    }

    @Override
    public String getStrategy() {
        return SinkConstants.MASK_STRATEGY_SHA256;
    }

    @Override
    public String mask(String original) {
        String material = (salt == null ? "" : salt) + original;
        return DigestUtils.sha256Hex(material);
    }
}
