package com.flying.orm.rdb.migration;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReactiveDataMigrationTest {

    @Test
    void compensationPreservesTheOriginalVirtualMachineError() {
        VirtualMachineError fatal = new VirtualMachineError("rollback fatal") { };
        ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.empty();
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return switch (request.sql()) {
                    case "forward-2" -> Mono.error(new IllegalStateException("forward failed"));
                    case "rollback-1" -> Mono.error(fatal);
                    default -> Mono.just(1L);
                };
            }
        };
        DataMigrationPlan plan = DataMigrationPlan.builder("migration")
                .step("first", request("forward-1"), request("rollback-1"))
                .step("second", request("forward-2"), request("rollback-2"))
                .build();
        AtomicReference<Throwable> failure = new AtomicReference<>();

        ReactiveDataMigration.create(executor).execute(plan).subscribe(ignored -> { }, failure::set);

        assertSame(fatal, failure.get());
    }

    @Test
    void cancellationDuringFailureCompensationCompletesRemainingRollbackWithoutReplay() {
        List<String> executions = new ArrayList<>();
        Sinks.One<Long> pendingRollback = Sinks.one();
        ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.empty();
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.defer(() -> {
                    executions.add(request.sql());
                    return switch (request.sql()) {
                        case "forward-3" -> Mono.error(new IllegalStateException("forward failed"));
                        case "rollback-2" -> pendingRollback.asMono();
                        default -> Mono.just(1L);
                    };
                });
            }
        };
        DataMigrationPlan plan = DataMigrationPlan.builder("migration")
                .step("first", request("forward-1"), request("rollback-1"))
                .step("second", request("forward-2"), request("rollback-2"))
                .step("third", request("forward-3"), request("rollback-3"))
                .build();

        Disposable subscription = ReactiveDataMigration.create(executor).execute(plan).subscribe();
        assertEquals(List.of("forward-1", "forward-2", "forward-3", "rollback-2"), executions);
        subscription.dispose();
        pendingRollback.tryEmitValue(1L);

        assertEquals(List.of("forward-1", "forward-2", "forward-3", "rollback-2", "rollback-1"),
                executions);
    }

    @Test
    void reportsAnUnknownOutcomeWhenTheForwardResultSignalIsLost() {
        List<String> executions = new ArrayList<>();
        ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.empty();
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                executions.add(request.sql());
                return request.sql().equals("forward")
                        ? Mono.error(new IllegalStateException("result signal lost"))
                        : Mono.just(1L);
            }
        };
        DataMigrationPlan plan = DataMigrationPlan.builder("migration")
                                                  .step("step", request("forward"), request("rollback"))
                                                  .build();

        DataMigrationException failure = assertThrows(
                DataMigrationException.class,
                () -> ReactiveDataMigration.create(executor).execute(plan).block());

        assertEquals("OUTCOME_UNKNOWN", failure.result().status().name());
        assertEquals(List.of("forward"), executions);
    }

    @Test
    void compensatesConfirmedStepsButKeepsTheFailedStepOutcomeUnknown() {
        List<String> executions = new ArrayList<>();
        ReactiveSqlExecutor executor = executor(executions, false);
        DataMigrationPlan plan = DataMigrationPlan.builder("migration")
                                                  .step("first", request("forward-1"), request("rollback-1"))
                                                  .step("second", request("forward-2"), request("rollback-2"))
                                                  .build();

        DataMigrationException failure = assertThrows(
                DataMigrationException.class,
                () -> ReactiveDataMigration.create(executor).execute(plan).block());

        assertEquals(DataMigrationStatus.OUTCOME_UNKNOWN, failure.result().status());
        assertEquals(List.of("forward-1", "forward-2", "rollback-1"), executions);
        assertTrue(failure.result().steps().getFirst().rolledBack());
    }

    @Test
    void reportsRollbackFailureWhenAConfirmedStepCannotBeCompensated() {
        List<String> executions = new ArrayList<>();
        ReactiveSqlExecutor executor = executor(executions, true);
        DataMigrationPlan plan = DataMigrationPlan.builder("migration")
                                                  .step("first", request("forward-1"), request("rollback-1"))
                                                  .step("second", request("forward-2"), request("rollback-2"))
                                                  .build();

        DataMigrationException failure = assertThrows(
                DataMigrationException.class,
                () -> ReactiveDataMigration.create(executor).execute(plan).block());

        assertEquals(DataMigrationStatus.ROLLBACK_FAILED, failure.result().status());
        assertEquals(List.of("forward-1", "forward-2", "rollback-1"), executions);
        assertNotNull(failure.result().steps().getFirst().rollbackFailure());
    }

    private static ReactiveSqlExecutor executor(List<String> executions, boolean rollbackFails) {
        return new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.empty();
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                executions.add(request.sql());
                if (request.sql().equals("forward-2") || rollbackFails && request.sql().equals("rollback-1")) {
                    return Mono.error(new IllegalStateException("execution result unavailable"));
                }
                return Mono.just(1L);
            }
        };
    }

    private static SqlRequest request(String sql) {
        return new SqlRequest(sql, List.of());
    }
}
