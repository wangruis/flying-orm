package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequests;
import com.flying.orm.rdb.internal.binding.SqlNullParameter;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BatchPayloadHasherStableTest {

    private final BatchPayloadHasher hasher = new BatchPayloadHasher();

    @Test
    void bindsReceiptIdentityToBothChunkAndRowBudgets() {
        BatchWriteOptions options = BatchWriteOptions.independent(500, 4)
                .withMemoryLimits(0, 4096, 16).withMaxRowBytes(128);

        assertNotEquals(planHash(options), planHash(options.withMaxRowBytes(256)));
        assertNotEquals(planHash(options), planHash(options.withMemoryLimits(0, 8192, 16)
                .withMaxRowBytes(128)));
        assertEquals(planHash(options), planHash(options.withMaxRows(100)
                .withTimeout(Duration.ofSeconds(1)).withReceipt("same-plan")));
    }

    private String planHash(BatchWriteOptions options) {
        return hasher.hashPlan(BatchWriteRequests.request("INSERT INTO sample (value) VALUES (?)",
                1, List.of(byte[].class), SqlBindMarkerStyle.CANONICAL,
                Flux.empty(), options));
    }

    @Test
    void preservesRowsAndValueBoundaries() {
        String twoValues = hasher.hashRows(List.<Object[]>of(new Object[]{"ab", "c"}));
        String otherBoundary = hasher.hashRows(List.<Object[]>of(new Object[]{"a", "bc"}));
        String twoRows = hasher.hashRows(List.of(new Object[]{"ab"}, new Object[]{"c"}));

        assertNotEquals(twoValues, otherBoundary);
        assertNotEquals(twoValues, twoRows);
    }

    @Test
    void distinguishesNumericTypesAndContainerShapes() {
        assertNotEquals(hash((byte) 1), hash(1));
        assertNotEquals(hash(1), hash(1L));
        assertNotEquals(hash(List.of("a", "b")), hash(new String[]{"a", "b"}));
    }

    @Test
    void hashesTypedNullsWithTheirBindingType() {
        assertDoesNotThrow(() -> hash(new SqlNullParameter(String.class)));
        assertNotEquals(hash(new SqlNullParameter(String.class)),
                        hash(new SqlNullParameter(Long.class)));
        assertNotEquals(hash(null), hash(new SqlNullParameter(String.class)));
    }

    @Test
    void streamingRowsProduceTheSameIdentityAsMaterializedRows() {
        List<Object[]> rows = List.of(new Object[]{1, "first"}, new Object[]{2, "second"});
        BatchReceiptDigest digest = hasher.newPayloadEncoder();

        rows.forEach(row -> hasher.updateRow(digest, row));

        assertEquals(hasher.hashRows(rows), hasher.finish(digest));
    }

    @Test
    void readsByteBufferWithoutChangingCallerState() {
        ByteBuffer value = ByteBuffer.wrap(new byte[]{1, 2, 3, 4});
        value.position(1);
        value.limit(3);

        String bufferHash = hash(value);

        assertEquals(1, value.position());
        assertEquals(3, value.limit());
        assertNotEquals(bufferHash, hash(ByteBuffer.wrap(new byte[]{1, 2})));
    }

    @Test
    void rejectsCyclicAndExcessivelyDeepPayloads() {
        List<Object> cyclic = new ArrayList<>();
        cyclic.add(cyclic);
        Object nested = "value";
        for (int depth = 0; depth < 66; depth++) {
            nested = List.of(nested);
        }
        Object tooDeep = nested;

        assertThrows(IllegalArgumentException.class, () -> hash(cyclic));
        assertThrows(IllegalArgumentException.class, () -> hash(tooDeep));
    }

    @Test
    void keepsTheCurrentBatchReceiptEncodingProtocolStable() {
        Object[] row = {
                null, "text", true, (byte) 1, (short) 2, 3, 4L,
                new BigInteger("5"), new BigDecimal("6.70"), 8.5F, 9.25D,
                UUID.fromString("00000000-0000-0001-0000-000000000002"),
                LocalDate.of(2026, 8, 24),
                OffsetDateTime.parse("2026-08-24T10:11:12+08:00"),
                Timestamp.from(Instant.parse("2026-08-24T02:11:12.123456789Z")),
                java.sql.Date.valueOf(LocalDate.of(2026, 8, 24)),
                Time.valueOf(LocalTime.of(10, 11, 12)),
                new java.util.Date(1_700_000_000_000L),
                new byte[]{1, 2}, ByteBuffer.wrap(new byte[]{3, 4}),
                new int[]{5, 6}, List.of("nested", 7)
        };

        assertEquals("aad814b5ff76b28f1e23e98976e195934356ea616887132d1d62ce148a7e2b93",
                 hasher.hashRows(List.<Object[]>of(row)));
    }

    private String hash(Object value) {
        return hasher.hashRows(List.<Object[]>of(new Object[]{value}));
    }
}
