package com.flying.orm.core.internal.hash;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class StableDigestTest {

    @Test
    void isolatesDomainsAndPreservesSegmentBoundaries() {
        String firstDomain = StableDigest.sha256("form")
                                         .text("part", "ab")
                                         .text("part", "c")
                                         .finishHex();
        String secondDomain = StableDigest.sha256("batch")
                                          .text("part", "ab")
                                          .text("part", "c")
                                          .finishHex();
        String differentBoundary = StableDigest.sha256("form")
                                               .text("part", "a")
                                               .text("part", "bc")
                                               .finishHex();

        assertNotEquals(firstDomain, secondDomain);
        assertNotEquals(firstDomain, differentBoundary);
    }

    @Test
    void distinguishesTagsNullsAndValueTypes() {
        String text = StableDigest.sha256("value").text("text", "1").finishHex();
        String integer = StableDigest.sha256("value").integer("number", 1).finishHex();
        String nullValue = StableDigest.sha256("value").nullableText("text", null).finishHex();
        String literalNull = StableDigest.sha256("value").text("text", "null").finishHex();

        assertNotEquals(text, integer);
        assertNotEquals(nullValue, literalNull);
        assertNotEquals(text, StableDigest.sha256("value").text("other", "1").finishHex());
    }

    @Test
    void readsOnlyTheRemainingBufferWithoutChangingCallerState() {
        ByteBuffer source = ByteBuffer.wrap(new byte[]{1, 2, 3, 4});
        source.position(1);
        source.limit(3);
        int position = source.position();
        int limit = source.limit();

        String bufferHash = StableDigest.sha256("bytes").bytes("value", source).finishHex();
        String arrayHash = StableDigest.sha256("bytes").bytes("value", new byte[]{2, 3}).finishHex();

        assertEquals(arrayHash, bufferHash);
        assertEquals(position, source.position());
        assertEquals(limit, source.limit());
    }

    @Test
    void rejectsReuseAfterFinishing() {
        StableEncoder encoder = StableDigest.sha256("one-shot").text("value", "ready");

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
}
