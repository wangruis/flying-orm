package com.flying.orm.core.internal.hash;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.lang.management.ManagementFactory;
import java.util.HexFormat;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StableDigestTest {

    @Test
    void submitsCommonSegmentHeadersTogetherWithoutChangingTheirBytes() throws Exception {
        MessageDigest expected = MessageDigest.getInstance("SHA-256");
        CountingDigest digest = new CountingDigest();
        StableEncoder actual = new StableEncoder(digest);
        String[] tags = {"TERM", "FIELD", "OPERATOR", "GROUP_START", "GROUP_END", "ARITY"};
        for (String tag : tags) {
            segment(expected, tag, null);
            actual.nullableText(tag, null);
        }
        assertEquals(HexFormat.of().formatHex(expected.digest()), actual.finishHex());
        assertEquals(tags.length, digest.updates, "Each common header needs only one digest update");
    }

    private static final class CountingDigest extends MessageDigest {
        private final MessageDigest delegate = MessageDigest.getInstance("SHA-256");
        private int updates;

        private CountingDigest() throws Exception { super("SHA-256"); }
        @Override protected void engineUpdate(byte input) { updates++; delegate.update(input); }
        @Override protected void engineUpdate(byte[] input, int offset, int length) {
            updates++;
            delegate.update(input, offset, length);
        }
        @Override protected byte[] engineDigest() { return delegate.digest(); }
        @Override protected void engineReset() { delegate.reset(); }
    }

    @Test
    void isolatesDomainsAndPreservesSegmentBoundaries() {
        String firstDomain = StableDigest.sha256(StableDigest.domain("form"))
                                         .text("part", "ab")
                                         .text("part", "c")
                                         .finishHex();
        String secondDomain = StableDigest.sha256(StableDigest.domain("batch"))
                                          .text("part", "ab")
                                          .text("part", "c")
                                          .finishHex();
        String differentBoundary = StableDigest.sha256(StableDigest.domain("form"))
                                               .text("part", "a")
                                               .text("part", "bc")
                                               .finishHex();

        assertNotEquals(firstDomain, secondDomain);
        assertNotEquals(firstDomain, differentBoundary);
    }

    @Test
    void distinguishesTagsNullsAndValueTypes() {
        String text = StableDigest.sha256(StableDigest.domain("value")).text("text", "1").finishHex();
        String integer = StableDigest.sha256(StableDigest.domain("value")).integer("number", 1).finishHex();
        String nullValue = StableDigest.sha256(StableDigest.domain("value")).nullableText("text", null).finishHex();
        String literalNull = StableDigest.sha256(StableDigest.domain("value")).text("text", "null").finishHex();

        assertNotEquals(text, integer);
        assertNotEquals(nullValue, literalNull);
        assertNotEquals(text, StableDigest.sha256(StableDigest.domain("value")).text("other", "1").finishHex());
    }

    @Test
    void consumesTheArrayBeforeCallerMutation() {
        byte[] source = {2, 3};
        StableDigest.Domain domain = StableDigest.domain("bytes");
        StableEncoder encoder = StableDigest.sha256(domain).bytes("value", source);
        source[0] = 9;
        String expected = StableDigest.sha256(domain).bytes("value", new byte[]{2, 3}).finishHex();
        assertEquals(expected, encoder.finishHex());
    }

    @Test
    void rejectsReuseAfterFinishing() {
        StableEncoder encoder = StableDigest.sha256(StableDigest.domain("one-shot")).text("value", "ready");

        encoder.finishHex();

        assertThrows(IllegalStateException.class, () -> encoder.text("value", "late"));
        assertThrows(IllegalStateException.class, encoder::finishHex);
    }

    @Test
    void safelyReusesAPreinitializedDomain() {
        StableDigest.Domain domain = StableDigest.domain("reusable");

        String first = StableDigest.sha256(domain).text("value", "same").finishHex();
        String second = StableDigest.sha256(domain).text("value", "same").finishHex();

        assertEquals(first, second);
        assertThrows(IllegalArgumentException.class, () -> StableDigest.domain(" "));
    }

    @Test
    void preservesUtf8TagProtocolAndMixedSegmentBoundaries() throws Exception {
        MessageDigest expected = MessageDigest.getInstance("SHA-256");
        StableEncoder actual = new StableEncoder(MessageDigest.getInstance("SHA-256"));
        for (int length = 1; length <= 129; length++) {
            checkSegments("a".repeat(length), expected, actual);
        }
        for (char character : new char[]{0, 127, 128, 'é', '中', '\ud800', '\udc00'}) {
            for (int position = 0; position < 18; position++) {
                checkSegments("A".repeat(position) + character + "tail", expected, actual);
            }
        }
        checkSegments("pair-\ud83d\ude00-end", expected, actual);
        assertEquals(HexFormat.of().formatHex(expected.digest()), actual.finishHex());
    }

    @Test
    void asciiTagsDoNotAllocateAnEncodingArrayPerSegment() throws Exception {
        com.sun.management.ThreadMXBean bean =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        bean.setThreadAllocatedMemoryEnabled(true);
        StableEncoder encoder = new StableEncoder(MessageDigest.getInstance("SHA-256"));
        String[] tags = {"TERM", "FIELD", "OPERATOR", "GROUP_START"};
        for (int index = 0; index < 50_000; index++) encoder.integer(tags[index & 3], index);
        long thread = Thread.currentThread().threadId();
        long before = bean.getThreadAllocatedBytes(thread);
        for (int index = 0; index < 20_000; index++) encoder.integer(tags[index & 3], index);
        long allocated = bean.getThreadAllocatedBytes(thread) - before;
        assertEquals(64, encoder.finishHex().length());
        assertTrue(allocated < 20_000L * 8,
                () -> "ASCII tag encoding still allocates per segment: " + allocated);
    }

    @Test
    void preservesTextEncodingAcrossShortAsciiAndUtf8Fallbacks() throws Exception {
        MessageDigest expected = MessageDigest.getInstance("SHA-256");
        StableEncoder actual = new StableEncoder(MessageDigest.getInstance("SHA-256"));
        for (int length = 0; length <= 129; length++) {
            String text = "A".repeat(length);
            segment(expected, "FIELD", text.getBytes(StandardCharsets.UTF_8));
            actual.text("FIELD", text);
        }
        for (char character : new char[]{0, 127, 128, 'é', '中', '\ud800', '\udc00'}) {
            for (int position = 0; position < 10; position++) {
                String text = "A".repeat(position) + character;
                segment(expected, "值", text.getBytes(StandardCharsets.UTF_8));
                actual.text("值", text);
            }
        }
        segment(expected, "FIELD", "\ud83d\ude00".getBytes(StandardCharsets.UTF_8));
        actual.text("FIELD", "\ud83d\ude00");
        assertEquals(HexFormat.of().formatHex(expected.digest()), actual.finishHex());
    }

    @Test
    void shortAsciiValuesDoNotAllocateEncodingArrays() throws Exception {
        com.sun.management.ThreadMXBean bean =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        bean.setThreadAllocatedMemoryEnabled(true);
        StableEncoder encoder = new StableEncoder(MessageDigest.getInstance("SHA-256"));
        String[] values = {"", "id", ">=", "OPERATOR"};
        for (int index = 0; index < 50_000; index++) encoder.text("FIELD", values[index & 3]);
        long thread = Thread.currentThread().threadId();
        long before = bean.getThreadAllocatedBytes(thread);
        for (int index = 0; index < 20_000; index++) encoder.text("FIELD", values[index & 3]);
        long allocated = bean.getThreadAllocatedBytes(thread) - before;
        assertEquals(64, encoder.finishHex().length());
        assertTrue(allocated < 20_000L * 8, "Short text encoding bytes: " + allocated);
    }

    private static void checkSegments(String tag, MessageDigest expected, StableEncoder actual) {
        segment(expected, tag, new byte[0]);
        segment(expected, tag, ByteBuffer.allocate(Long.BYTES).putLong(Long.MIN_VALUE + 17).array());
        segment(expected, tag, "值-\ud83d\ude00".getBytes(StandardCharsets.UTF_8));
        segment(expected, tag, new byte[]{1});
        segment(expected, tag, null);
        segment(expected, tag, new byte[]{3, 4});
        actual.marker(tag).integer(tag, Long.MIN_VALUE + 17).text(tag, "值-\ud83d\ude00")
                .bool(tag, true).nullableText(tag, null).bytes(tag, new byte[]{3, 4});
    }

    private static void segment(MessageDigest digest, String tag, byte[] value) {
        byte[] encoded = tag.getBytes(StandardCharsets.UTF_8);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(encoded.length).array());
        digest.update(encoded);
        digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(value == null ? -1 : value.length).array());
        if (value != null) digest.update(value);
    }
}
