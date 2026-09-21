package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.dialect.RdbDialect;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.result.DynamicRow;
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
    void executionSessionReusesPublisherWhenAllQueryProtectionIsDisabled() {
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
                com.flying.orm.rdb.execution.ConnectionAccessTestSupport.reactive(connectionFactory),
                R2dbcBindMarkers.from(RdbDialect.postgresql()),
                SqlExecutionObserver.noop(),
                null);
        Flux<DynamicRow> rows = Flux.never();

        assertSame(rows, session.protectRows(rows, "select 1", SqlExecutionOptions.unlimited()));
    }
}
