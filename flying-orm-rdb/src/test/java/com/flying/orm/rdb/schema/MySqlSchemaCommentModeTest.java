package com.flying.orm.rdb.schema;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MySqlSchemaCommentModeTest {

    private static final SqlRequest GUARDED_DDL = new SqlRequest(
            "create table paths (value varchar(32) comment "
                    + "/*flying-orm:mysql-comment-no-backslash-escapes*/ 'C:\\data')",
            List.of());

    @Test
    void jdbcRejectsBackslashCommentBeforeDdlWhenRequiredModeIsMissing() {
        AtomicInteger ddlExecutions = new AtomicInteger();
        AtomicInteger modeQueries = new AtomicInteger();
        SyncSqlExecutor sqlExecutor = syncExecutor("STRICT_TRANS_TABLES", modeQueries, ddlExecutions);

        SchemaMigrationRejectedException failure = assertThrows(
                SchemaMigrationRejectedException.class,
                () -> jdbcExecutor(sqlExecutor).execute(List.of(GUARDED_DDL), SqlExecutionOptions.safeDefaults()));

        assertEquals(SchemaMigrationFailureCode.EXECUTOR_CAPABILITY_REQUIRED, failure.failureCode());
        assertTrue(failure.getMessage().contains("NO_BACKSLASH_ESCAPES"));
        assertEquals(1, modeQueries.get());
        assertEquals(0, ddlExecutions.get());
    }

    @Test
    void reactiveRejectsBackslashCommentBeforeDdlWhenRequiredModeIsMissing() {
        AtomicInteger ddlExecutions = new AtomicInteger();
        AtomicInteger modeQueries = new AtomicInteger();
        ReactiveSqlExecutor sqlExecutor = reactiveExecutor("STRICT_TRANS_TABLES", modeQueries, ddlExecutions);

        SchemaMigrationRejectedException failure = assertThrows(
                SchemaMigrationRejectedException.class,
                () -> reactiveExecutor(sqlExecutor).execute(List.of(GUARDED_DDL)).block());

        assertEquals(SchemaMigrationFailureCode.EXECUTOR_CAPABILITY_REQUIRED, failure.failureCode());
        assertTrue(failure.getMessage().contains("NO_BACKSLASH_ESCAPES"));
        assertEquals(1, modeQueries.get());
        assertEquals(0, ddlExecutions.get());
    }

    @Test
    void jdbcExecutesBackslashCommentWhenRequiredModeIsConfigured() {
        AtomicInteger modeQueries = new AtomicInteger();
        AtomicInteger ddlExecutions = new AtomicInteger();
        SyncSqlExecutor sqlExecutor = syncExecutor(
                "STRICT_TRANS_TABLES,NO_BACKSLASH_ESCAPES", modeQueries, ddlExecutions);

        long rows = jdbcExecutor(sqlExecutor).execute(
                List.of(GUARDED_DDL, GUARDED_DDL), SqlExecutionOptions.safeDefaults());

        assertEquals(0L, rows);
        assertEquals(1, modeQueries.get());
        assertEquals(2, ddlExecutions.get());
    }

    @Test
    void reactiveExecutesBackslashCommentWhenRequiredModeIsConfigured() {
        AtomicInteger modeQueries = new AtomicInteger();
        AtomicInteger ddlExecutions = new AtomicInteger();
        ReactiveSqlExecutor sqlExecutor = reactiveExecutor(
                "NO_BACKSLASH_ESCAPES,STRICT_TRANS_TABLES", modeQueries, ddlExecutions);

        long rows = reactiveExecutor(sqlExecutor).execute(List.of(GUARDED_DDL, GUARDED_DDL)).block();

        assertEquals(0L, rows);
        assertEquals(1, modeQueries.get());
        assertEquals(2, ddlExecutions.get());
    }

    @Test
    void ordinaryMysqlDdlDoesNotQueryTheSessionMode() {
        SqlRequest ordinaryDdl = new SqlRequest("create table paths (value varchar(32) comment 'plain')", List.of());
        AtomicInteger jdbcExecutions = new AtomicInteger();
        SyncSqlExecutor jdbc = new SyncSqlExecutor() {
            @Override
            public List<DynamicRow> query(SqlRequest request) {
                throw new AssertionError("ordinary MySQL DDL must not query sql_mode");
            }

            @Override
            public long rowsUpdated(SqlRequest request) {
                jdbcExecutions.incrementAndGet();
                return 0L;
            }

            @Override
            public com.flying.orm.rdb.execution.SqlWriteResult rowsUpdatedReturningKeys(
                    SqlRequest request, SqlExecutionOptions options) {
                throw new AssertionError("generated keys must not be requested");
            }
        };
        AtomicInteger reactiveExecutions = new AtomicInteger();
        ReactiveSqlExecutor reactive = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.error(new AssertionError("ordinary MySQL DDL must not query sql_mode"));
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                reactiveExecutions.incrementAndGet();
                return Mono.just(0L);
            }
        };

        jdbcExecutor(jdbc).execute(List.of(ordinaryDdl), SqlExecutionOptions.safeDefaults());
        reactiveExecutor(reactive).execute(List.of(ordinaryDdl)).block();

        assertEquals(1, jdbcExecutions.get());
        assertEquals(1, reactiveExecutions.get());
    }

    private static SyncSqlExecutor syncExecutor(
            String modes, AtomicInteger modeQueries, AtomicInteger ddlExecutions) {
        return new SyncSqlExecutor() {
            private boolean inScope;

            @Override
            public <T> T withConnection(SqlRequest first, Function<SyncSqlExecutor, T> work) {
                assertEquals("select @@SESSION.sql_mode as sql_mode", first.sql());
                assertFalse(inScope);
                inScope = true;
                try {
                    return work.apply(this);
                } finally {
                    inScope = false;
                }
            }

            @Override
            public List<DynamicRow> query(SqlRequest request) {
                assertTrue(inScope, "mode validation must run in the shared connection scope");
                assertEquals("select @@SESSION.sql_mode as sql_mode", request.sql());
                modeQueries.incrementAndGet();
                return List.of(DynamicRow.copyOf(Map.of("sql_mode", modes)));
            }

            @Override
            public long rowsUpdated(SqlRequest request) {
                assertTrue(inScope, "guarded DDL must use the validated connection scope");
                assertEquals(GUARDED_DDL.sql(), request.sql());
                ddlExecutions.incrementAndGet();
                return 0L;
            }

            @Override
            public com.flying.orm.rdb.execution.SqlWriteResult rowsUpdatedReturningKeys(
                    SqlRequest request, SqlExecutionOptions options) {
                throw new AssertionError("generated keys must not be requested");
            }
        };
    }

    private static ReactiveSqlExecutor reactiveExecutor(
            String modes, AtomicInteger modeQueries, AtomicInteger ddlExecutions) {
        return new ReactiveSqlExecutor() {
            private boolean inScope;

            @Override
            public <T> Mono<T> withConnection(SqlRequest first, Function<ReactiveSqlExecutor, Mono<T>> work) {
                return Mono.defer(() -> {
                    assertEquals("select @@SESSION.sql_mode as sql_mode", first.sql());
                    assertFalse(inScope);
                    inScope = true;
                    return Mono.defer(() -> work.apply(this)).doFinally(ignored -> inScope = false);
                });
            }

            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                assertTrue(inScope, "mode validation must run in the shared connection scope");
                assertEquals("select @@SESSION.sql_mode as sql_mode", request.sql());
                modeQueries.incrementAndGet();
                return Flux.just(DynamicRow.copyOf(Map.of("sql_mode", modes)));
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                assertTrue(inScope, "guarded DDL must use the validated connection scope");
                assertEquals(GUARDED_DDL.sql(), request.sql());
                ddlExecutions.incrementAndGet();
                return Mono.just(0L);
            }
        };
    }

    private static JdbcSchemaMigrationExecutor jdbcExecutor(SyncSqlExecutor executor) {
        return new JdbcSchemaMigrationExecutor(
                executor,
                SchemaMigrationObserver.noop(),
                ignored -> { });
    }

    private static SchemaMigrationExecutor reactiveExecutor(ReactiveSqlExecutor executor) {
        return new SchemaMigrationExecutor(
                executor,
                SchemaMigrationObserver.noop());
    }
}
