package com.flying.orm.rdb.batch;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class BatchWriteRequestRecoveryTest {

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
                generatedKeys);
    }
}
