package com.flying.orm.rdb.schema;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemaTransactionBoundaryTest {
    @Test
    void executionSpiHasNoTransactionProtocol() {
        for (Class<?> type : List.of(SyncSqlExecutor.class, ReactiveSqlExecutor.class)) {
            org.junit.jupiter.api.Assertions.assertFalse(java.util.Arrays.stream(type.getMethods())
                    .anyMatch(method -> method.getName().toLowerCase().contains("transaction")));
        }
    }

    private static final List<SqlRequest> REQUESTS = List.of(
            new SqlRequest("create table accounts (id integer)", List.of()));

    @Test
    void jdbcExecutesAndInvalidatesWithoutTransactionInspection() {
        AtomicInteger executions = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        SyncSqlExecutor sql = new SyncSqlExecutor() {
            public List<DynamicRow> query(SqlRequest request) { return List.of(); }
            public long rowsUpdated(SqlRequest request) { executions.incrementAndGet(); return 0L; }
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
                throw new UnsupportedOperationException();
            }
        };
        new JdbcSchemaMigrationExecutor(sql, SchemaMigrationObserver.noop(), ignored -> { })
                .executeWithInvalidation(REQUESTS, List.of("accounts"),
                        ignored -> invalidations.incrementAndGet(), SqlExecutionOptions.safeDefaults());
        assertEquals(1, invalidations.get());
        assertEquals(1, executions.get());
    }

    @Test
    void reactiveExecutesAndInvalidatesWithoutTransactionInspection() {
        AtomicInteger executions = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        ReactiveSqlExecutor sql = new ReactiveSqlExecutor() {
            public Flux<DynamicRow> query(SqlRequest request) { return Flux.empty(); }
            public Mono<Long> rowsUpdated(SqlRequest request) { return Mono.fromSupplier(() -> { executions.incrementAndGet(); return 0L; }); }
        };
        Mono<Long> execution = new SchemaMigrationExecutor(sql, SchemaMigrationObserver.noop())
                .executeWithInvalidation(REQUESTS, List.of("accounts"),
                        ignored -> invalidations.incrementAndGet(), SqlExecutionOptions.safeDefaults());
        assertEquals(0, executions.get());
        assertEquals(0, invalidations.get());
        assertEquals(0L, execution.block());
        assertEquals(1, invalidations.get());
        assertEquals(1, executions.get());
    }
}
