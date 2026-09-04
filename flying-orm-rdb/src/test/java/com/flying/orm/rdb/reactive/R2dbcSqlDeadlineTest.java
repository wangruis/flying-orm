package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipant;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import static org.junit.jupiter.api.Assertions.assertSame;

class R2dbcSqlDeadlineTest {

    @Test
    void doesNotWrapPublishersWhenSqlTimeoutIsDisabled() {
        R2dbcSqlDeadline deadline = R2dbcSqlDeadline.start(SqlExecutionOptions.safeDefaults());
        Mono<Integer> mono = Mono.just(1);
        Flux<Integer> flux = Flux.just(1);

        assertSame(mono, deadline.bind(mono));
        assertSame(flux, deadline.bind(flux));
        assertSame(mono, deadline.protectExecution(mono));
        assertSame(flux, deadline.protectExecution(flux));
    }

    @Test
    void executionSessionDoesNotAddDeadlineContextWhenAllQueryProtectionIsDisabled() {
        ConnectionFactory connectionFactory = new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return Mono.error(new AssertionError("connection must not be acquired"));
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "PostgreSQL";
            }
        };
        R2dbcExecutionSession session = new R2dbcExecutionSession(
                connectionFactory,
                R2dbcBindMarkers.from(connectionFactory),
                SqlExecutionObserver.noop(),
                R2dbcTransactionParticipant.none());
        Flux<DynamicRow> rows = Flux.never();

        assertSame(rows, session.protectRows(rows, "select 1", SqlExecutionOptions.unlimited()));
    }
}
