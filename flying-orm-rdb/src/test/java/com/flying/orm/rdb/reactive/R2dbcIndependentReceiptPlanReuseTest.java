package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.dialect.RdbDialect;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteRequests;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class R2dbcIndependentReceiptPlanReuseTest {

    @Test
    void emptyWriteBatchDoesNotAcquireConnections() {
        emptyInputDoesNotAcquireConnections(false);
    }

    @Test
    void emptyWriteBatchEvidenceDoesNotAcquireConnections() {
        emptyInputDoesNotAcquireConnections(true);
    }

    private static void emptyInputDoesNotAcquireConnections(boolean streaming) {
        AtomicInteger acquires = new AtomicInteger();
        ConnectionFactory connectionFactory = new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                acquires.incrementAndGet();
                return Mono.error(new AssertionError("empty input must not acquire a connection"));
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "H2";
            }
        };
        BatchWriteRequest request = BatchWriteRequests.request(
                "insert into samples(value_col) values (?)", 1,
                List.of(Integer.class), SqlBindMarkerStyle.CANONICAL,
                Flux.empty(), BatchWriteOptions.of(1));
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(com.flying.orm.rdb.execution.ConnectionAccessTestSupport.reactive(connectionFactory), RdbDialect.h2());
        Mono<BatchExecutionEvidence> execution = streaming
                ? executor.writeBatchEvidence(request)
                : executor.writeBatch(request);

        BatchExecutionEvidence result = execution.block(Duration.ofSeconds(2));

        assertEquals(0, result.inputCount());
        assertEquals(0, result.successfulCount());
        assertEquals(0, acquires.get());
    }
}
