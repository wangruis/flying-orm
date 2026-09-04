package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionPhase;
import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;
import com.flying.orm.rdb.transaction.JdbcTransactionCompletion;
import com.flying.orm.rdb.transaction.JdbcTransactionParticipant;
import com.flying.orm.rdb.transaction.TransactionOutcome;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JdbcSchemaMigrationExecutorFailurePriorityTest {

    @Test
    void keepsSetupFailureWithNoopObserver() {
        assertSessionFailure(SqlExecutionPhase.SETUP, 0, false, false);
    }

    @Test
    void keepsCleanupFailureWithNoopObserver() {
        assertSessionFailure(SqlExecutionPhase.CLEANUP, 0, false, false);
    }

    @Test
    void observesSetupFailureBeforeCleanup() {
        assertSessionFailure(SqlExecutionPhase.SETUP, 0, true, false);
    }

    @Test
    void observesCleanupFailureAfterCompletedWork() {
        assertSessionFailure(SqlExecutionPhase.CLEANUP, 0, true, false);
    }

    @Test
    void observesSecondWorkFailureAfterCleanup() {
        assertSessionFailure(SqlExecutionPhase.WORK, 1, true, false);
    }

    @Test
    void observesSetupFailureWhenCleanupAlsoFails() {
        assertSessionFailure(SqlExecutionPhase.SETUP, 0, true, true);
    }

    @Test
    void observesSecondWorkFailureWhenCleanupAlsoFails() {
        assertSessionFailure(SqlExecutionPhase.WORK, 1, true, true);
    }

    private static void assertSessionFailure(SqlExecutionPhase failedPhase,
                                             int failedIndex,
                                             boolean observe,
                                             boolean cleanupAlsoFails) {
        SQLException driverFailure = new SQLException("DDL rejected", "42601");
        RdbException primary = new RdbException(RdbErrorKind.BAD_SQL, "DDL rejected", "42601", 0,
                                               driverFailure);
        RuntimeException cleanup = new IllegalStateException("lock timeout reset failed");
        List<SqlRequest> work = List.of(
                new SqlRequest("alter table orders add column note varchar(20)", List.of()),
                new SqlRequest("alter table orders add column tag varchar(20)", List.of()));
        List<String> calls = new ArrayList<>();
        SyncSqlExecutor sqlExecutor = new SyncSqlExecutor() {
            @Override
            public List<DynamicRow> query(SqlRequest request) {
                throw new AssertionError("query must not execute");
            }

            @Override
            public long rowsUpdated(SqlRequest request) {
                calls.add(request.sql());
                SqlExecutionPhase phase;
                int index;
                if (request.sql().startsWith("set lock_timeout")) {
                    phase = SqlExecutionPhase.SETUP;
                    index = 0;
                } else if (request.sql().equals("reset lock_timeout")) {
                    phase = SqlExecutionPhase.CLEANUP;
                    index = 0;
                } else {
                    phase = SqlExecutionPhase.WORK;
                    index = work.indexOf(request);
                    if (index < 0) {
                        throw new AssertionError("unexpected SQL: " + request.sql());
                    }
                }
                if (phase == failedPhase && index == failedIndex) {
                    throw primary;
                }
                if (phase == SqlExecutionPhase.CLEANUP && cleanupAlsoFails) {
                    throw cleanup;
                }
                return phase == SqlExecutionPhase.WORK ? 1L : 0L;
            }

            @Override
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
                throw new AssertionError("generated keys must not execute");
            }
        };
        AtomicReference<JdbcTransactionCompletion.Listener> completion = new AtomicReference<>();
        JdbcTransactionContext transaction = JdbcTransactionContext.external(
                transactionParticipant().currentTransaction().orElseThrow().connection(),
                listener -> completion.compareAndSet(null, listener));
        List<SchemaMigrationObservation> observations = new ArrayList<>();
        AtomicInteger invalidations = new AtomicInteger();
        JdbcSchemaMigrationExecutor executor = new JdbcSchemaMigrationExecutor(
                sqlExecutor, FormSchemaSqlRenderer.create(RdbDialect.postgresql()),
                observe ? observations::add : SchemaMigrationObserver.noop(),
                SchemaDdlTransactionSupport.TRANSACTIONAL, () -> Optional.of(transaction),
                ignored -> invalidations.incrementAndGet());
        ReviewedSchemaMigrationPlan plan = reviewedPlan(work);

        SqlExecutionSequenceException failure = assertThrows(SqlExecutionSequenceException.class,
                () -> executor.executeReviewed(plan, List.of("orders"),
                        SchemaMigrationExecutionOptions.defaults().withLockTimeout(Duration.ofSeconds(1))));

        assertEquals(failedPhase, failure.phase());
        assertEquals(failedIndex, failure.stepIndex());
        assertSame(primary, failure.getCause());
        assertSame(driverFailure, failure.getCause().getCause());
        int completed = failedPhase == SqlExecutionPhase.SETUP ? 0
                : failedPhase == SqlExecutionPhase.WORK ? failedIndex : work.size();
        assertEquals(completed, failure.completedWorkSteps().size());
        assertEquals("reset lock_timeout", calls.getLast());
        assertEquals(2 + (failedPhase == SqlExecutionPhase.WORK ? failedIndex + 1 : completed), calls.size());
        assertEquals(cleanupAlsoFails ? 1 : 0, failure.getSuppressed().length);
        if (cleanupAlsoFails) {
            SqlExecutionSequenceException suppressed = assertInstanceOf(SqlExecutionSequenceException.class,
                                                                         failure.getSuppressed()[0]);
            assertEquals(SqlExecutionPhase.CLEANUP, suppressed.phase());
            assertEquals(0, suppressed.stepIndex());
            assertSame(cleanup, suppressed.getCause());
        }
        assertEquals(observe ? 1 : 0, observations.size());
        if (observe) {
            SchemaMigrationObservation observation = observations.getFirst();
            assertEquals(plan.fingerprint(), observation.planFingerprint());
            assertEquals(SqlExecutionStatus.ERROR, observation.status());
            assertEquals(failedPhase, observation.failedPhase());
            assertEquals(failedIndex, observation.failedStepIndex());
            assertEquals(failedPhase == SqlExecutionPhase.CLEANUP
                    ? SchemaMigrationFailureCode.CLEANUP_FAILED : SchemaMigrationFailureCode.BAD_SQL,
                    observation.failureCode());
            assertEquals(completed, observation.completedSteps());
            assertEquals(completed, observation.rowsUpdated());
            assertSame(failure, observation.error());
        }
        assertEquals(0, invalidations.get());
        Mono.from(completion.get().afterCompletion(TransactionOutcome.ROLLED_BACK)).block();
        assertEquals(1, invalidations.get());
    }

    @Test
    void keepsWorkFailurePrimaryWhenCleanupAlsoFails() {
        RuntimeException workFailure = new IllegalStateException("work failed");
        RuntimeException cleanupFailure = new IllegalArgumentException("cleanup failed");
        SequencedFailingExecutor sqlExecutor = new SequencedFailingExecutor(workFailure, cleanupFailure);
        FormSchemaSqlRenderer renderer = FormSchemaSqlRenderer.create(RdbDialect.postgresql());
        JdbcTransactionParticipant transactionParticipant = transactionParticipant();
        JdbcSchemaMigrationExecutor executor = new JdbcSchemaMigrationExecutor(
                sqlExecutor,
                renderer,
                SchemaMigrationObserver.noop(),
                SchemaDdlTransactionSupport.TRANSACTIONAL,
                transactionParticipant,
                ignored -> {
                });
        SchemaMigrationExecutionOptions options = SchemaMigrationExecutionOptions.defaults()
                                                                                  .withLockTimeout(
                                                                                          Duration.ofSeconds(1));

        SqlExecutionSequenceException failure = assertThrows(
                SqlExecutionSequenceException.class,
                () -> executor.executeReviewed(reviewedPlan(), List.of("orders"), options));

        assertEquals(SqlExecutionPhase.WORK, failure.phase());
        assertSame(workFailure, failure.getCause());
        assertEquals(1, failure.getSuppressed().length);
        SqlExecutionSequenceException suppressedCleanup = assertInstanceOf(
                SqlExecutionSequenceException.class,
                failure.getSuppressed()[0]);
        assertEquals(SqlExecutionPhase.CLEANUP, suppressedCleanup.phase());
        assertSame(cleanupFailure, suppressedCleanup.getCause());
        assertEquals(3, sqlExecutor.calls());
    }

    private static ReviewedSchemaMigrationPlan reviewedPlan() {
        return reviewedPlan(List.of(new SqlRequest("alter table orders add column note varchar(20)", List.of())));
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

    private static JdbcTransactionParticipant transactionParticipant() {
        Connection connection = (Connection) Proxy.newProxyInstance(
                JdbcSchemaMigrationExecutorFailurePriorityTest.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (ignored, method, arguments) -> null);
        JdbcTransactionContext context = new JdbcTransactionContext(connection, ignored -> true);
        return () -> Optional.of(context);
    }

    private static final class SequencedFailingExecutor implements SyncSqlExecutor {
        private final RuntimeException workFailure;
        private final RuntimeException cleanupFailure;
        private int calls;

        private SequencedFailingExecutor(RuntimeException workFailure, RuntimeException cleanupFailure) {
            this.workFailure = workFailure;
            this.cleanupFailure = cleanupFailure;
        }

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            calls++;
            return switch (calls) {
                case 1 -> 0L;
                case 2 -> throw workFailure;
                case 3 -> throw cleanupFailure;
                default -> throw new AssertionError("unexpected SQL execution: " + request.sql());
            };
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            throw new UnsupportedOperationException();
        }

        private int calls() {
            return calls;
        }
    }
}
