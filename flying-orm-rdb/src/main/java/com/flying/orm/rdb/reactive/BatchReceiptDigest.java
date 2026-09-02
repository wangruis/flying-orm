package com.flying.orm.rdb.reactive;

import com.flying.orm.core.internal.hash.StableDigest;
import com.flying.orm.core.internal.hash.StableEncoder;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** 单次批量回执摘要。 */
final class BatchReceiptDigest {

    private static final byte[] EMPTY = new byte[0];

    private final StableEncoder encoder;

    private BatchReceiptDigest(StableEncoder encoder) {
        this.encoder = encoder;
    }

    static BatchReceiptDigest create(StableDigest.Domain domain) {
        return new BatchReceiptDigest(StableDigest.sha256(domain));
    }

    BatchReceiptDigest marker(String tag) {
        return bytes(tag, EMPTY);
    }

    BatchReceiptDigest text(String tag, String value) {
        return bytes(tag, Objects.requireNonNull(value, "batch receipt text must not be null")
                                 .getBytes(StandardCharsets.UTF_8));
    }

    BatchReceiptDigest bool(String tag, boolean value) {
        encoder.bool(tag, value);
        return this;
    }

    BatchReceiptDigest integer(String tag, long value) {
        encoder.integer(tag, value);
        return this;
    }

    BatchReceiptDigest uuid(long mostSignificantBits, long leastSignificantBits) {
        encoder.marker("UUID")
               .integer("UUID_MOST", mostSignificantBits)
               .integer("UUID_LEAST", leastSignificantBits);
        return this;
    }

    BatchReceiptDigest bytes(String tag, byte[] value) {
        encoder.bytes(tag, Objects.requireNonNull(value, "batch receipt bytes must not be null"));
        return this;
    }

    BatchReceiptDigest bytes(String tag, ByteBuffer value) {
        encoder.bytes(tag, Objects.requireNonNull(value, "batch receipt buffer must not be null")
                                  .asReadOnlyBuffer());
        return this;
    }

    String finish() {
        return encoder.finishHex();
    }
}
