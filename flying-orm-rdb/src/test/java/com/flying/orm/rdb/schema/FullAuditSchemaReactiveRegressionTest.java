package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FullAuditSchemaReactiveRegressionTest {

    private static final SqlRequest FIRST =
            new SqlRequest("alter table orders add column note varchar(64)", List.of());
    private static final SqlRequest SECOND =
            new SqlRequest("alter table orders add column description varchar(64)", List.of());

    @Test
    void reviewedMigrationFailureObservesCompletedFirstStepAndPropagatesOriginalError() {
        IllegalStateException failure = new IllegalStateException("second DDL failed");
        List<SqlRequest> executed = new ArrayList<>();
        List<SchemaMigrationObservation> observations = new ArrayList<>();
        SchemaMigrationExecutor executor = executor(Mono.error(failure), executed, observations);

        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> executeReviewed(executor).block());

        assertSame(failure, thrown);
        assertEquals(List.of(FIRST, SECOND), executed);
        SchemaMigrationObservation event = assertObservation(
                observations, SqlExecutionStatus.ERROR, 1, 7L);
        assertSame(failure, event.error());
    }

    @Test
    void reviewedMigrationCancellationObservesCompletedFirstStepAndCancelsPendingSecondStep() {
        AtomicInteger cancelled = new AtomicInteger();
        List<SqlRequest> executed = new ArrayList<>();
        List<SchemaMigrationObservation> observations = new ArrayList<>();
        SchemaMigrationExecutor executor = executor(
                Mono.<Long>never().doOnCancel(cancelled::incrementAndGet), executed, observations);

        Disposable subscription = executeReviewed(executor).subscribe();
        try {
            assertEquals(List.of(FIRST, SECOND), executed);
            assertTrue(observations.isEmpty());
        } finally {
            subscription.dispose();
        }

        assertEquals(1, cancelled.get());
        SchemaMigrationObservation event = assertObservation(
                observations, SqlExecutionStatus.CANCELLED, 1, 7L);
        assertNull(event.error());
    }

    @Test
    void reviewedMigrationSuccessReportsAllExecutedStepsAndTotalFacts() {
        List<SqlRequest> executed = new ArrayList<>();
        List<SchemaMigrationObservation> observations = new ArrayList<>();
        SchemaMigrationExecutor executor = executor(Mono.just(11L), executed, observations);

        SchemaMigrationResult result = executeReviewed(executor).block();

        assertNotNull(result);
        assertEquals(List.of(FIRST, SECOND), executed);
        assertEquals(18L, result.rowsUpdated());
        assertEquals(List.of(FIRST, SECOND), result.steps().stream().map(
                com.flying.orm.rdb.execution.SqlExecutionStepResult::request).toList());
        assertEquals(List.of(7L, 11L), result.steps().stream().map(
                com.flying.orm.rdb.execution.SqlExecutionStepResult::rowsUpdated).toList());
        SchemaMigrationObservation event = assertObservation(
                observations, SqlExecutionStatus.SUCCESS, 2, 18L);
        assertNull(event.error());
    }

    private static SchemaMigrationExecutor executor(
            Mono<Long> secondResult,
            List<SqlRequest> executed,
            List<SchemaMigrationObservation> observations) {
        ReactiveSqlExecutor sqlExecutor = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.error(new AssertionError("reviewed DDL must not query: " + request.sql()));
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.defer(() -> {
                    executed.add(request);
                    return request == FIRST ? Mono.just(7L) : secondResult;
                });
            }
        };
        return new SchemaMigrationExecutor(
                sqlExecutor, observations::add);
    }

    private static Mono<SchemaMigrationResult> executeReviewed(SchemaMigrationExecutor executor) {
        DynamicForm target = DynamicForm.builder("orders", "orders")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .build();
        SchemaMigrationPlan migration = new SchemaMigrationPlan(
                target, List.of(), List.of(), true, List.of(FIRST, SECOND), List.of());
        ReviewedSchemaMigrationPlan reviewed = new ReviewedSchemaMigrationPlan(
                migration,
                new SchemaRollbackPlan(List.of(), List.of()),
                new OnlineDdlReview(OnlineDdlMode.ALLOW_BLOCKING, List.of()));
        return executor.executeReviewed(
                reviewed, List.of("orders"), ignored -> {}, SchemaMigrationExecutionOptions.defaults());
    }

    private static SchemaMigrationObservation assertObservation(
            List<SchemaMigrationObservation> observations,
            SqlExecutionStatus status,
            int completedSteps,
            long rowsUpdated) {
        assertEquals(1, observations.size());
        SchemaMigrationObservation event = observations.get(0);
        assertEquals(status, event.status());
        assertEquals(2, event.plannedSteps());
        assertEquals(completedSteps, event.completedSteps());
        assertEquals(rowsUpdated, event.rowsUpdated());
        return event;
    }
}
