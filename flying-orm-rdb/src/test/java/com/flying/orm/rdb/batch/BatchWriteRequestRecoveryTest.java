package com.flying.orm.rdb.batch;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BatchWriteRequestRecoveryTest {

    @Test
    void rejectsReceiptRecoveryWhenDatabaseGeneratedKeysCannotBeReplayed() {
        BatchWriteOptions options = BatchWriteOptions.defaults()
                .withReceipt("insert-users", Duration.ofSeconds(1));
        BatchGeneratedKeys generatedKeys = BatchGeneratedKeys.required("id", (offset, row) -> { });

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> com.flying.orm.rdb.batch.BatchWriteRequests.request(
                        "INSERT INTO users(name) VALUES (?)",
                        1,
                        List.of(String.class),
                        SqlBindMarkerStyle.CANONICAL,
                        Flux.<Object[]>just(new Object[]{"alice"}),
                        options,
                        BatchRowCountPolicy.ANY,
                        generatedKeys,
                        BatchWriteCompletion.noop()));

        assertEquals("batch receipt recovery does not support database-generated keys", failure.getMessage());
    }

    @Test
    void keepsReceiptRecoveryAvailableForOrdinaryBatchWrites() {
        BatchWriteOptions options = BatchWriteOptions.defaults()
                .withReceipt("update-users", Duration.ofSeconds(1));

        assertDoesNotThrow(() -> request(options, BatchGeneratedKeys.none()));
    }

    @Test
    void keepsDatabaseGeneratedKeysAvailableWithoutReceiptRecovery() {
        BatchGeneratedKeys generatedKeys = BatchGeneratedKeys.required("id", (offset, row) -> { });

        assertDoesNotThrow(() -> request(BatchWriteOptions.defaults(), generatedKeys));
    }

    private static BatchWriteRequest request(BatchWriteOptions options, BatchGeneratedKeys generatedKeys) {
        return com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "INSERT INTO users(name) VALUES (?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(new Object[]{"alice"}),
                options,
                BatchRowCountPolicy.ANY,
                generatedKeys,
                BatchWriteCompletion.noop());
    }
}
