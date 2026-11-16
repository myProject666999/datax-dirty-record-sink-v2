package com.alibaba.datax.core.statistics.plugin.task.sink;

import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.codec.digest.DigestUtils;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class RollingJsonlWriterTest {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    private RollingJsonlWriter newWriter(long maxFileBytes) {
        return new RollingJsonlWriter(folder.getRoot(), "dirty-j0-tg0-t0-a0-writer",
                maxFileBytes, true);
    }

    @Test
    public void testRolloverBySize() throws Exception {
        RollingJsonlWriter writer = newWriter(50L);
        final List<SinkFileEntry> entries = new ArrayList<SinkFileEntry>();
        writer.setClosedFileConsumer(new java.util.function.Consumer<SinkFileEntry>() {
            @Override
            public void accept(SinkFileEntry entry) {
                entries.add(entry);
            }
        });

        for (int i = 0; i < 10; i++) {
            writer.writeLine(String.format("{\"seq\":%d,\"v\":\"%s\"}", i, "payload-" + i), i + 1);
        }
        SinkFileEntry last = writer.closeCurrentFile();
        if (last != null) {
            entries.add(last);
        }

        assertTrue("应产生多个滚动文件", entries.size() >= 2);
        File[] jsonl = folder.getRoot().listFiles(new java.io.FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().endsWith(SinkConstants.JSONL_SUFFIX);
            }
        });
        // 活动文件已全部 finalize，没有 .writing 残留
        assertEquals(entries.size(), jsonl.length);
        File[] writing = folder.getRoot().listFiles(new java.io.FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().endsWith(SinkConstants.WRITING_SUFFIX);
            }
        });
        assertEquals(0, writing.length);

        // 序号按 000001、000002 递增
        assertEquals("dirty-j0-tg0-t0-a0-writer.000001.jsonl", entries.get(0).getFileName());

        // sha256 与独立计算一致
        for (SinkFileEntry entry : entries) {
            File f = new File(folder.getRoot(), entry.getFileName());
            try (FileInputStream in = new FileInputStream(f)) {
                assertEquals(DigestUtils.sha256Hex(in), entry.getSha256());
            }
            assertEquals(f.length(), entry.getBytes());
        }
    }

    @Test
    public void testEveryLineDurableBeforeClose() throws Exception {
        RollingJsonlWriter writer = newWriter(1024L * 1024L);
        writer.writeLine("{\"v\":\"durable\"}", 1);
        writer.flush();
        // 未关闭时活动文件以 .writing 存在，且内容已可读出
        File[] writing = folder.getRoot().listFiles(new java.io.FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().endsWith(SinkConstants.WRITING_SUFFIX);
            }
        });
        assertEquals(1, writing.length);
        String content = new String(Files.readAllBytes(writing[0].toPath()), StandardCharsets.UTF_8);
        assertTrue(content.contains("durable"));

        SinkFileEntry entry = writer.closeCurrentFile();
        assertNotNull(entry);
        assertFalse(writing[0].exists());
        assertTrue(new File(folder.getRoot(), entry.getFileName()).exists());
    }

    @Test
    public void testEmptyWriterProducesNoFile() {
        RollingJsonlWriter writer = newWriter(1024L);
        assertEquals(null, writer.closeCurrentFile());
        assertEquals(0, folder.getRoot().listFiles().length);
    }

    @Test
    public void testSingleOversizeLineGetsOwnFile() {
        final List<SinkFileEntry> entries = new ArrayList<SinkFileEntry>();
        RollingJsonlWriter writer = newWriter(10L);
        writer.setClosedFileConsumer(new java.util.function.Consumer<SinkFileEntry>() {
            @Override
            public void accept(SinkFileEntry entry) {
                entries.add(entry);
            }
        });
        // 该行远超 maxFileBytes：写入后立即滚动并 finalize 为独立文件，不拆行
        writer.writeLine("{\"v\":\"this line is much longer than the max file size\"}", 1);
        SinkFileEntry entry = writer.closeCurrentFile();
        if (entry != null) {
            entries.add(entry);
        }
        assertEquals(1, entries.size());
        assertEquals(1, entries.get(0).getRecords());
        File[] jsonl = folder.getRoot().listFiles(new java.io.FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().endsWith(SinkConstants.JSONL_SUFFIX);
            }
        });
        assertEquals(1, jsonl.length);
    }

    @Test
    public void testByteSizeParser() {
        assertEquals(1024L, ByteSizeParser.parse("1KB"));
        assertEquals(1024L * 1024L, ByteSizeParser.parse("1MB"));
        assertEquals(1024L * 1024L * 1024L, ByteSizeParser.parse("1GB"));
        assertEquals(2048L, ByteSizeParser.parse("2048"));
        try {
            ByteSizeParser.parse("bad-size");
            throw new AssertionError("应拒绝非法大小配置");
        } catch (DataXException expected) {
            // ok
        }
        assertArrayEquals(new byte[0], new byte[0]);
    }
}
