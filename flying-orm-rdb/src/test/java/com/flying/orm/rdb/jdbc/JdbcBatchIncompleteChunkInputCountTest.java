package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JdbcBatchIncompleteChunkInputCountTest {

    @Test
    void independentReportsRowsAcceptedBeforeAnIncompleteChunkInputFailure() {
        IllegalArgumentException inputFailure = new IllegalArgumentException("batch input failed");

        BatchWriteException failure = assertThrows(
                BatchWriteException.class,
                () -> JdbcBatchWriter.create(dataSource("independent_incomplete_input"))
                        .writeBatch(request(BatchWriteOptions.independent(2, 1), inputFailure)));

        assertSame(inputFailure, failure.getCause());
        assertIncompleteInput(failure);
    }

    @Test
    void atomicReportsRowsAcceptedBeforeAnIncompleteChunkInputFailure() {
        IllegalArgumentException inputFailure = new IllegalArgumentException("batch input failed");

        BatchWriteException failure = assertThrows(
                BatchWriteException.class,
                () -> JdbcBatchWriter.create(dataSource("atomic_incomplete_input"))
                        .writeBatch(request(BatchWriteOptions.atomic(2), inputFailure)));

        assertSame(inputFailure, failure.getCause());
        assertEquals(com.flying.orm.rdb.batch.BatchWriteResult.Status.ROLLED_BACK,
                     failure.result().status());
        assertIncompleteInput(failure);
    }

    private static void assertIncompleteInput(BatchWriteException failure) {
        assertEquals(1, failure.result().inputCount());
        assertEquals(1, failure.result().chunks().size());
        BatchChunkResult chunk = failure.result().chunks().getFirst();
        assertEquals(0, chunk.startOffset());
        assertEquals(1, chunk.inputCount());
        assertEquals(BatchChunkResult.Status.FAILED, chunk.status());
        assertFalse(chunk.failure().message().contains("batch input failed"));
    }

    private static BatchWriteRequest request(BatchWriteOptions options,
                                             IllegalArgumentException inputFailure) {
        return com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into batch_people(name_col) values (?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.concat(Flux.<Object[]>just(new Object[]{"name-0"}), Flux.error(inputFailure)),
                options);
    }

    private static JdbcDataSource dataSource(String name) {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        dataSource.setPassword("");
        return dataSource;
    }
}
