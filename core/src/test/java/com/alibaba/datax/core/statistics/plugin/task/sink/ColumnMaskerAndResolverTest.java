package com.alibaba.datax.core.statistics.plugin.task.sink;

import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ColumnMaskerAndResolverTest {

    @Test
    public void testSha256KnownVector() {
        Sha256ColumnMasker masker = new Sha256ColumnMasker(null);
        assertEquals(DigestUtils.sha256Hex("abc"), masker.mask("abc"));
        assertEquals("sha256", masker.getStrategy());
    }

    @Test
    public void testSaltChangesDigest() {
        String plain = "13800000000";
        String noSalt = new Sha256ColumnMasker(null).mask(plain);
        String withSalt = new Sha256ColumnMasker("static-salt").mask(plain);
        assertFalse(noSalt.equals(withSalt));
        assertEquals(DigestUtils.sha256Hex("static-salt" + plain), withSalt);
        // 不可逆：输出不含原文
        assertFalse(withSalt.contains(plain));
    }

    @Test
    public void testReplaceIsIrreversible() {
        ReplaceColumnMasker masker = new ReplaceColumnMasker("***");
        assertEquals("***", masker.mask("anything"));
        assertEquals("replace", masker.getStrategy());
        assertFalse(masker.mask("secret").contains("secret"));
    }

    @Test
    public void testReplaceDefaultValue() {
        assertEquals("***", new ReplaceColumnMasker(null).mask("x"));
    }

    private Configuration jobWithColumns(String readerColumns, String writerColumns) {
        String json = "{\"job\":{\"content\":[{\"reader\":{\"name\":\"r\",\"parameter\":"
                + "{\"column\":" + readerColumns + "}},\"writer\":{\"name\":\"w\",\"parameter\":"
                + "{\"column\":" + writerColumns + "}}}]}}";
        return Configuration.from(json);
    }

    private List<MaskRule> rule(String name, Integer index, String strategy) {
        List<MaskRule> rules = new ArrayList<MaskRule>();
        rules.add(new MaskRule(name, index, strategy, "***", null));
        return rules;
    }

    @Test
    public void testResolveStringAndMapColumnForms() {
        Configuration conf = jobWithColumns(
                "[\"id\",\"name\",{\"index\":2,\"name\":\"phone\"}]",
                "[{\"name\":\"id\"},{\"name\":\"name\"},{\"name\":\"phone\"}]");
        Map<Integer, ColumnMasker> maskers = MaskRuleResolver.resolve(
                rule("phone", null, "sha256"), conf, true).getMaskers();
        assertEquals(1, maskers.size());
        assertTrue(maskers.get(2) instanceof Sha256ColumnMasker);
    }

    @Test
    public void testExplicitIndexFallback() {
        Configuration conf = jobWithColumns("[{\"index\":0,\"type\":\"string\"}]", "[]");
        Map<Integer, ColumnMasker> maskers = MaskRuleResolver.resolve(
                rule(null, 0, "replace"), conf, true).getMaskers();
        assertTrue(maskers.get(0) instanceof ReplaceColumnMasker);
    }

    @Test
    public void testUnknownColumnErrorByDefault() {
        Configuration conf = jobWithColumns("[\"a\"]", "[\"a\"]");
        try {
            MaskRuleResolver.resolve(rule("not_exist", null, "sha256"), conf, true);
            fail("应因未知列名抛异常");
        } catch (DataXException e) {
            assertTrue(e.getMessage().contains("not_exist"));
        }
    }

    @Test
    public void testUnknownColumnIgnore() {
        Configuration conf = jobWithColumns("[\"a\"]", "[\"a\"]");
        MaskRuleResolver.Resolution resolution = MaskRuleResolver.resolve(
                rule("not_exist", null, "sha256"), conf, false);
        assertTrue(resolution.getMaskers().isEmpty());
        assertEquals(1, resolution.getUnresolvedRules().size());
    }

    @Test
    public void testUnsupportedStrategy() {
        Configuration conf = jobWithColumns("[\"a\"]", "[\"a\"]");
        try {
            MaskRuleResolver.resolve(rule("a", null, "unknown-strategy"), conf, true);
            fail("应因不支持的脱敏策略抛异常");
        } catch (DataXException e) {
            assertTrue(e.getMessage().contains("unknown-strategy"));
        }
    }
}
