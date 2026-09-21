package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
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
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReviewedSchemaMigrationRejectionFingerprintTest {
    @Test
    void bindsReactiveSqlPreconditionRejectionToReviewedPlan() {
        ReviewedSchemaMigrationPlan plan = reviewedPlan();
        AtomicInteger scopes = new AtomicInteger();
        ReactiveSqlExecutor sql = new ReactiveSqlExecutor() {
            public <T> Mono<T> withConnection(SqlRequest first, Function<ReactiveSqlExecutor, Mono<T>> work) {
                return Mono.defer(() -> {
                    assertEquals("select @@SESSION.sql_mode as sql_mode", first.sql());
                    scopes.incrementAndGet();
                    return work.apply(this);
                });
            }
            public Flux<DynamicRow> query(SqlRequest request) {
                assertEquals(1, scopes.get(), "mode precondition must run inside the connection scope");
                assertEquals("select @@SESSION.sql_mode as sql_mode", request.sql());
                return Flux.empty();
            }
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.error(new AssertionError("DDL must be rejected before execution"));
            }
        };
        SchemaMigrationExecutor executor = new SchemaMigrationExecutor(sql, SchemaMigrationObserver.noop());
        SchemaMigrationRejectedException failure = assertThrows(SchemaMigrationRejectedException.class,
                () -> executor.executeReviewed(plan, List.of("orders"), ignored -> { },
                        SchemaMigrationExecutionOptions.defaults()).block());
        assertPlanBinding(plan, failure);
        assertEquals(1, scopes.get());
    }

    @Test
    void bindsJdbcSqlPreconditionRejectionToReviewedPlan() {
        ReviewedSchemaMigrationPlan plan = reviewedPlan();
        AtomicInteger scopes = new AtomicInteger();
        SyncSqlExecutor sql = new SyncSqlExecutor() {
            public <T> T withConnection(SqlRequest first, Function<SyncSqlExecutor, T> work) {
                assertEquals("select @@SESSION.sql_mode as sql_mode", first.sql());
                scopes.incrementAndGet();
                return work.apply(this);
            }
            public List<DynamicRow> query(SqlRequest request) {
                assertEquals(1, scopes.get(), "mode precondition must run inside the connection scope");
                assertEquals("select @@SESSION.sql_mode as sql_mode", request.sql());
                return List.of();
            }
            public long rowsUpdated(SqlRequest request) {
                throw new AssertionError("DDL must be rejected before execution");
            }
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
                throw new AssertionError("generated keys must not execute");
            }
        };
        JdbcSchemaMigrationExecutor executor = new JdbcSchemaMigrationExecutor(
                sql, SchemaMigrationObserver.noop(), ignored -> { });
        SchemaMigrationRejectedException failure = assertThrows(SchemaMigrationRejectedException.class,
                () -> executor.executeReviewed(plan, List.of("orders"), SchemaMigrationExecutionOptions.defaults()));
        assertPlanBinding(plan, failure);
        assertEquals(1, scopes.get());
    }

    private static void assertPlanBinding(ReviewedSchemaMigrationPlan plan, SchemaMigrationRejectedException failure) {
        assertEquals(SchemaMigrationFailureCode.EXECUTOR_CAPABILITY_REQUIRED, failure.failureCode());
        assertEquals(plan.fingerprint(), failure.planFingerprint());
        assertEquals(plan.fingerprint(), failure.toErrorReport().resource());
    }

    private static ReviewedSchemaMigrationPlan reviewedPlan() {
        DynamicForm target = DynamicForm.builder("orders", "orders")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        SchemaMigrationPlan migration = new SchemaMigrationPlan(target, List.of(), List.of(), true,
                List.of(new SqlRequest("alter table orders comment "
                        + "/*flying-orm:mysql-comment-no-backslash-escapes*/ 'C:\\data'", List.of())), List.of());
        return new ReviewedSchemaMigrationPlan(migration, new SchemaRollbackPlan(List.of(), List.of()),
                new OnlineDdlReview(OnlineDdlMode.ALLOW_BLOCKING, List.of()));
    }
}
