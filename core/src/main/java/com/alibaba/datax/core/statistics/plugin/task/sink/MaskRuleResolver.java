package com.alibaba.datax.core.statistics.plugin.task.sink;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.util.FrameworkErrorCode;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 把脱敏规则解析为 列下标 -> ColumnMasker 的映射。
 * 列名来源于 job.content 下 reader/writer parameter.column 配置，兼容四种形态：
 * String 列名、{"name":"x"}、{"index":1,"name":"x"}、仅 {"index":1}（不贡献列名）。
 */
public final class MaskRuleResolver {

    private static final Logger LOG = LoggerFactory.getLogger(MaskRuleResolver.class);

    private MaskRuleResolver() {
    }

    public static class Resolution {
        private final Map<Integer, ColumnMasker> maskers;
        private final List<String> unresolvedRules;

        Resolution(Map<Integer, ColumnMasker> maskers, List<String> unresolvedRules) {
            this.maskers = maskers;
            this.unresolvedRules = unresolvedRules;
        }

        public Map<Integer, ColumnMasker> getMaskers() {
            return maskers;
        }

        public List<String> getUnresolvedRules() {
            return unresolvedRules;
        }
    }

    public static Resolution resolve(List<MaskRule> rules, Configuration fullConf,
                                     boolean errorOnUnknown) {
        Map<Integer, ColumnMasker> maskers = new LinkedHashMap<Integer, ColumnMasker>();
        if (rules == null || rules.isEmpty()) {
            return new Resolution(maskers, new ArrayList<String>());
        }

        Map<String, Integer> nameToIndex = buildNameToIndex(fullConf);
        List<String> unresolved = new ArrayList<String>();

        for (MaskRule rule : rules) {
            Integer index = null;
            if (StringUtils.isNotBlank(rule.getColumnName())) {
                index = nameToIndex.get(rule.getColumnName());
            }
            if (index == null) {
                // 列名解析不到时，显式下标兜底
                index = rule.getColumnIndex();
            }

            if (index == null || index < 0) {
                String desc = StringUtils.isNotBlank(rule.getColumnName())
                        ? "columnName=" + rule.getColumnName()
                        : "columnIndex=" + rule.getColumnIndex();
                unresolved.add(desc);
                continue;
            }

            maskers.put(index, buildMasker(rule));
        }

        if (!unresolved.isEmpty()) {
            String message = "以下脱敏规则无法定位到数据列: " + unresolved
                    + "，请检查job.content中reader/writer的column配置或改用columnIndex";
            if (errorOnUnknown) {
                throw DataXException.asDataXException(
                        FrameworkErrorCode.DIRTY_RECORD_MASK_CONFIG_ERROR, message);
            }
            LOG.warn(message);
        }

        return new Resolution(maskers, unresolved);
    }

    private static ColumnMasker buildMasker(MaskRule rule) {
        String strategy = rule.getStrategy();
        if (SinkConstants.MASK_STRATEGY_SHA256.equalsIgnoreCase(strategy)) {
            return new Sha256ColumnMasker(rule.getSalt());
        }
        if (SinkConstants.MASK_STRATEGY_REPLACE.equalsIgnoreCase(strategy)) {
            return new ReplaceColumnMasker(rule.getReplacement());
        }
        throw DataXException.asDataXException(
                FrameworkErrorCode.DIRTY_RECORD_MASK_CONFIG_ERROR,
                String.format("不支持的脱敏策略[%s]，仅支持 sha256 / replace", strategy));
    }

    /**
     * 扫描所有 task 配置中的 reader/writer column 列表，合并出 列名 -> 下标。
     * writer 与 reader 同名冲突时 writer 优先（与数据最终落库列序对齐）。
     */
    private static Map<String, Integer> buildNameToIndex(Configuration fullConf) {
        Map<String, Integer> readerMap = new HashMap<String, Integer>();
        Map<String, Integer> writerMap = new HashMap<String, Integer>();

        List<Configuration> contents = fullConf.getListConfiguration("job.content");
        if (contents == null) {
            return writerMap;
        }
        for (Configuration content : contents) {
            collectColumnNames(content.getConfiguration("reader.parameter"), readerMap);
            collectColumnNames(content.getConfiguration("writer.parameter"), writerMap);
        }

        Map<String, Integer> merged = new HashMap<String, Integer>(readerMap);
        merged.putAll(writerMap);
        return merged;
    }

    private static void collectColumnNames(Configuration parameter, Map<String, Integer> target) {
        if (parameter == null) {
            return;
        }
        List<Object> columns = parameter.getList("column");
        if (columns == null) {
            return;
        }
        for (int i = 0; i < columns.size(); i++) {
            Object element = columns.get(i);
            if (element == null) {
                continue;
            }
            if (element instanceof String) {
                String name = (String) element;
                if (StringUtils.isNotBlank(name) && !"*".equals(name)) {
                    target.put(name, i);
                }
            } else if (element instanceof Map) {
                Map<?, ?> map = (Map<?, ?>) element;
                Object name = map.get("name");
                if (name != null && StringUtils.isNotBlank(name.toString())) {
                    Object explicitIndex = map.get("index");
                    int index = explicitIndex instanceof Number
                            ? ((Number) explicitIndex).intValue() : i;
                    target.put(name.toString(), index);
                }
            }
        }
    }
}
