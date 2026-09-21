package com.flying.orm.rdb.schema;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SchemaCommentConnectionScopeTest {
    private static final SqlRequest COMMENT = new SqlRequest(
            "alter table events comment /*flying-orm:mysql-comment-no-backslash-escapes*/ 'C:\\data'", List.of());
    private static final SqlExecutionOptions OPTIONS = SqlExecutionOptions.safeDefaults();

    @Test
    void jdbcValidationAndDdlUseSameBoundExecutor() {
        var events = new ArrayList<String>();
        var executor = new JdbcSchemaMigrationExecutor(jdbc(events, false), SchemaMigrationObserver.noop(), ignored -> { });
        assertEquals(1L, executor.execute(List.of(COMMENT), OPTIONS));
        assertEquals(List.of("scope", "bound query", "bound ddl"), events);
    }

    @Test
    void jdbcInvalidationOccursAfterScopedDdl() {
        var events = new ArrayList<String>();
        var executor = new JdbcSchemaMigrationExecutor(jdbc(events, false), SchemaMigrationObserver.noop(), ignored -> { });
        assertEquals(1L, executor.executeWithInvalidation(List.of(COMMENT), List.of("events"),
                                                        ignored -> events.add("invalidate"), OPTIONS));
        assertEquals(List.of("scope", "bound query", "bound ddl", "invalidate"), events);
    }

    @Test
    void reactiveValidationAndDdlUseSameBoundExecutor() {
        var events = new ArrayList<String>();
        var executor = new SchemaMigrationExecutor(reactive(events, false), SchemaMigrationObserver.noop());
        Mono<Long> result = executor.execute(List.of(COMMENT));
        assertEquals(List.of(), events);
        assertEquals(1L, result.block());
        assertEquals(List.of("scope", "bound query", "bound ddl"), events);
    }

    @Test
    void reactiveInvalidationOccursAfterScopedDdl() {
        var events = new ArrayList<String>();
        var executor = new SchemaMigrationExecutor(reactive(events, false), SchemaMigrationObserver.noop());
        assertEquals(1L, executor.executeWithInvalidation(List.of(COMMENT), List.of("events"),
                                                        ignored -> events.add("invalidate"), OPTIONS).block());
        assertEquals(List.of("scope", "bound query", "bound ddl", "invalidate"), events);
    }

    @Test
    void reviewedJdbcMigrationUsesSameConnection() {
        var events = new ArrayList<String>();
        var executor = new JdbcSchemaMigrationExecutor(jdbc(events, false), SchemaMigrationObserver.noop(),
                                                      ignored -> events.add("invalidate"));
        var result = executor.executeReviewed(reviewedMigration(), List.of("events"),
                                              SchemaMigrationExecutionOptions.defaults());
        assertEquals(1, result.steps().size());
        assertEquals(List.of("scope", "bound query", "bound ddl", "invalidate"), events);
    }

    @Test
    void reviewedReactiveMigrationUsesSameConnection() {
        var events = new ArrayList<String>();
        var executor = new SchemaMigrationExecutor(reactive(events, false), SchemaMigrationObserver.noop());
        var result = executor.executeReviewed(reviewedMigration(), List.of("events"),
                ignored -> events.add("invalidate"), SchemaMigrationExecutionOptions.defaults()).block();
        assertEquals(1, result.steps().size());
        assertEquals(List.of("scope", "bound query", "bound ddl", "invalidate"), events);
    }

    @Test
    void reviewedRelationalJdbcMigrationUsesSameConnection() {
        assertRelationalScope(false, false);
    }

    @Test
    void reviewedRelationalReactiveMigrationUsesSameConnection() {
        assertRelationalScope(true, false);
    }

    @Test
    void jdbcScopeReleaseFailureKeepsCompletedDdlFact() {
        assertRelationalScope(false, true);
    }

    @Test
    void reactiveScopeReleaseFailureKeepsCompletedDdlFact() {
        assertRelationalScope(true, true);
    }

    private static void assertRelationalScope(boolean reactive, boolean releaseFailure) {
        var events = new ArrayList<String>();
        var before = SchemaSnapshot.present(VerifiedSchemaPlanFixtures.table(VerifiedSchemaPlanFixtures.ID));
        var desired = VerifiedSchemaPlanFixtures.table(VerifiedSchemaPlanFixtures.ID, VerifiedSchemaPlanFixtures.NOTE);
        var plan = VerifiedSchemaPlanFixtures.plan(before, desired, COMMENT);
        AtomicInteger reads = new AtomicInteger();
        java.util.function.Supplier<SchemaSnapshot> reader = () -> reads.getAndIncrement() == 0
                ? before : SchemaSnapshot.present(desired);
        SchemaExecutionReport result = reactive
                ? new SchemaMigrationExecutor(reactive(events, false, releaseFailure), SchemaMigrationObserver.noop())
                    .executeReviewed(plan, () -> Mono.fromSupplier(reader), SchemaSnapshotCoverage::complete,
                                     () -> events.add("invalidate"), SchemaMigrationExecutionOptions.defaults()).block()
                : new JdbcSchemaMigrationExecutor(jdbc(events, false, releaseFailure),
                                                  SchemaMigrationObserver.noop(), ignored -> { })
                    .executeReviewed(plan, reader, SchemaSnapshotCoverage::complete,
                                     () -> events.add("invalidate"), SchemaMigrationExecutionOptions.defaults());
        assertEquals(releaseFailure ? SchemaExecutionStatus.UNKNOWN : SchemaExecutionStatus.SUCCESS, result.status());
        assertEquals(1, result.steps().size());
        assertEquals(SchemaExecutionStatus.SUCCESS, result.steps().getFirst().status());
        assertEquals(List.of("scope", "bound query", "bound ddl", "invalidate"), events);
    }

    private static ReviewedSchemaMigrationPlan reviewedMigration() {
        DynamicForm form = DynamicForm.builder("events", "events")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        return new ReviewedSchemaMigrationPlan(
                new SchemaMigrationPlan(form, List.of(), List.of(), true, List.of(COMMENT), List.of()),
                new SchemaRollbackPlan(List.of(), List.of()), new OnlineDdlReview(OnlineDdlMode.ALLOW_BLOCKING, List.of()));
    }

    private static SyncSqlExecutor jdbc(List<String> events, boolean bound) {
        return jdbc(events, bound, false);
    }

    @SuppressWarnings("unchecked")
    private static SyncSqlExecutor jdbc(List<String> events, boolean bound, boolean releaseFailure) {
        return (SyncSqlExecutor) Proxy.newProxyInstance(SyncSqlExecutor.class.getClassLoader(),
                new Class<?>[]{SyncSqlExecutor.class}, (proxy, method, arguments) -> {
                    switch (method.getName()) {
                        case "withConnection":
                            assertEquals("select @@SESSION.sql_mode as sql_mode", ((SqlRequest) arguments[0]).sql());
                            events.add("scope");
                            Object result = ((Function<SyncSqlExecutor, ?>) arguments[1]).apply(jdbc(events, true));
                            if (releaseFailure) throw new IllegalStateException("upper release failed");
                            return result;
                        case "query":
                            events.add((bound ? "bound" : "unbound") + " query");
                            return List.of(DynamicRow.copyOf(Map.of("sql_mode", "NO_BACKSLASH_ESCAPES")));
                        case "rowsUpdated":
                            events.add((bound ? "bound" : "unbound") + " ddl");
                            return 1L;
                        default: throw new AssertionError(method);
                    }
                });
    }

    private static ReactiveSqlExecutor reactive(List<String> events, boolean bound) {
        return reactive(events, bound, false);
    }

    @SuppressWarnings("unchecked")
    private static ReactiveSqlExecutor reactive(List<String> events, boolean bound, boolean releaseFailure) {
        return (ReactiveSqlExecutor) Proxy.newProxyInstance(ReactiveSqlExecutor.class.getClassLoader(),
                new Class<?>[]{ReactiveSqlExecutor.class}, (proxy, method, arguments) -> {
                    switch (method.getName()) {
                        case "withConnection":
                            return Mono.defer(() -> {
                                assertEquals("select @@SESSION.sql_mode as sql_mode", ((SqlRequest) arguments[0]).sql());
                                events.add("scope");
                                Mono<?> result = ((Function<ReactiveSqlExecutor, Mono<?>>) arguments[1])
                                        .apply(reactive(events, true));
                                return releaseFailure ? result.then(Mono.error(new IllegalStateException("upper release failed")))
                                        : result;
                            });
                        case "query":
                            return Flux.defer(() -> {
                                events.add((bound ? "bound" : "unbound") + " query");
                                return Flux.just(DynamicRow.copyOf(Map.of("sql_mode", "NO_BACKSLASH_ESCAPES")));
                            });
                        case "rowsUpdated":
                            return Mono.fromSupplier(() -> {
                                events.add((bound ? "bound" : "unbound") + " ddl");
                                return 1L;
                            });
                        default: throw new AssertionError(method);
                    }
                });
    }
}
