package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionPhase;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JdbcSchemaMigrationExecutorFailurePriorityTest {

    @Test
    void keepsFirstWorkFailureWithNoopObserver() {
        assertWorkFailure(0, false, false);
    }

    @Test
    void keepsSecondWorkFailureWithNoopObserver() {
        assertWorkFailure(1, false, false);
    }

    @Test
    void observesFirstWorkFailure() {
        assertWorkFailure(0, true, false);
    }

    @Test
    void observesSecondWorkFailureAfterCompletedWork() {
        assertWorkFailure(1, true, false);
    }

    @Test
    void observesFirstWorkFailureWhenInvalidationAlsoFails() {
        assertWorkFailure(0, true, true);
    }

    @Test
    void observesSecondWorkFailureWhenInvalidationAlsoFails() {
        assertWorkFailure(1, true, true);
    }

    @Test
    void keepsFirstWorkFailurePrimaryWhenInvalidationAlsoFails() {
        assertWorkFailure(0, false, true);
    }

    @Test
    void keepsSecondWorkFailurePrimaryWhenInvalidationAlsoFails() {
        assertWorkFailure(1, false, true);
    }

    private static void assertWorkFailure(int failedIndex,
                                          boolean observe,
                                          boolean invalidationAlsoFails) {
        SQLException driverFailure = new SQLException("DDL rejected", "42601");
        RdbException primary = new RdbException(RdbErrorKind.BAD_SQL, "DDL rejected", "42601", 0,
                                               driverFailure);
        RuntimeException cleanup = new IllegalStateException("metadata invalidation failed");
        List<SqlRequest> work = List.of(
                new SqlRequest("alter table orders add column note varchar(20)", List.of()),
                new SqlRequest("alter table orders add column tag varchar(20)", List.of()));
        List<SqlRequest> calls = new ArrayList<>();
        SyncSqlExecutor sqlExecutor = new SyncSqlExecutor() {
            @Override
            public List<DynamicRow> query(SqlRequest request) {
                throw new AssertionError("query must not execute");
            }

            @Override
            public long rowsUpdated(SqlRequest request) {
                calls.add(request);
                int index = work.indexOf(request);
                if (index < 0) {
                    throw new AssertionError("unexpected SQL: " + request.sql());
                }
                if (index == failedIndex) {
                    throw primary;
                }
                return 1L;
            }

            @Override
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
                throw new AssertionError("generated keys must not execute");
            }
        };
        List<SchemaMigrationObservation> observations = new ArrayList<>();
        AtomicInteger invalidations = new AtomicInteger();
        JdbcSchemaMigrationExecutor executor = new JdbcSchemaMigrationExecutor(
                sqlExecutor,
                observe ? observations::add : SchemaMigrationObserver.noop(),
                ignored -> {
                    invalidations.incrementAndGet();
                    if (invalidationAlsoFails) {
                        throw cleanup;
                    }
                });
        ReviewedSchemaMigrationPlan plan = reviewedPlan(work);

        RdbException failure = assertThrows(RdbException.class,
                () -> executor.executeReviewed(plan, List.of("orders"),
                        SchemaMigrationExecutionOptions.defaults()));

        assertSame(primary, failure);
        assertSame(driverFailure, failure.getCause());
        assertEquals(work.subList(0, failedIndex + 1), calls);
        assertEquals(invalidationAlsoFails ? 1 : 0, failure.getSuppressed().length);
        if (invalidationAlsoFails) {
            RdbException suppressed = assertInstanceOf(RdbException.class, failure.getSuppressed()[0]);
            assertSame(cleanup, suppressed.getCause());
        }
        assertEquals(observe ? 1 : 0, observations.size());
        if (observe) {
            SchemaMigrationObservation observation = observations.getFirst();
            assertEquals(plan.fingerprint(), observation.planFingerprint());
            assertEquals(SqlExecutionStatus.ERROR, observation.status());
            assertEquals(SqlExecutionPhase.WORK, observation.failedPhase());
            assertEquals(failedIndex, observation.failedStepIndex());
            assertEquals(SchemaMigrationFailureCode.BAD_SQL, observation.failureCode());
            assertEquals(work.size(), observation.plannedSteps());
            assertEquals(failedIndex, observation.completedSteps());
            assertEquals(failedIndex, observation.rowsUpdated());
            assertSame(failure, observation.error());
        }
        assertEquals(1, invalidations.get());
    }

    private static ReviewedSchemaMigrationPlan reviewedPlan(List<SqlRequest> requests) {
        DynamicForm target = DynamicForm.builder("orders", "orders")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .build();
        SchemaMigrationPlan migration = new SchemaMigrationPlan(
                target,
                List.of(),
                List.of(),
                true,
                requests,
                List.of());
        return new ReviewedSchemaMigrationPlan(
                migration,
                new SchemaRollbackPlan(List.of(), List.of()),
                new OnlineDdlReview(OnlineDdlMode.ALLOW_BLOCKING, List.of()));
    }
}
