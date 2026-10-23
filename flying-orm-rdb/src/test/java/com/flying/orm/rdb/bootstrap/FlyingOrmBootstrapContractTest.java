package com.flying.orm.rdb.bootstrap;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.batch.BatchMemoryLimits;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.jdbc.JdbcConnectionAccess;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionLogOptions;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.reactive.R2dbcConnectionAccess;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FlyingOrmBootstrapContractTest {

    @Test
    void selectsJdbcWithoutAcquiringConnections() {
        verifyConfiguredKernels(1);
    }

    @Test
    void selectsReactiveWithoutAcquiringConnections() {
        verifyConfiguredKernels(2);
    }

    @Test
    void selectsDualKernelsWithoutAcquiringConnections() {
        verifyConfiguredKernels(3);
    }

    private void verifyConfiguredKernels(int kernels) {
        JdbcConnectionAccess jdbc = JdbcConnectionAccess.of(request -> {
            throw new AssertionError("bootstrap must not acquire a JDBC connection");
        }, (connection, request) -> {
            throw new AssertionError("bootstrap must not release an external connection");
        });
        R2dbcConnectionAccess reactive = R2dbcConnectionAccess.of(request -> {
            throw new AssertionError("bootstrap must not acquire an R2DBC connection");
        }, (signal, connection, request) -> {
            throw new AssertionError("bootstrap must not release an external connection");
        });
        FlyingOrmEnvironment environment = switch (kernels) {
            case 1 -> FlyingOrmEnvironment.of(jdbc);
            case 2 -> FlyingOrmEnvironment.of(reactive);
            default -> FlyingOrmEnvironment.of(jdbc, reactive);
        };
        FlyingOrmConfiguration defaults = FlyingOrmConfiguration.defaults();
        BatchWriteOptions batchOptions = BatchWriteOptions.of(20_000);
        FlyingOrmConfiguration configuration = new FlyingOrmConfiguration(
                "postgresql", defaults.executionOptions(), batchOptions,
                new BatchMemoryLimits(30_000, BatchMemoryLimits.DEFAULT_MAX_BUFFERED_BYTES),
                defaults.cachePolicy(), defaults.migrationExecutionOptions(), defaults.sqlLog());

        try (FlyingOrmClients clients = FlyingOrmBootstrap.create(configuration, environment)) {
            assertEquals(kernels != 2, clients.jdbcAvailable());
            assertEquals(kernels != 1, clients.reactiveAvailable());
            if (clients.jdbcAvailable()) {
                assertEquals(batchOptions, clients.syncForms().defaultBatchWriteOptions());
            } else {
                assertThrows(IllegalStateException.class, clients::syncExecutor);
            }
            if (clients.reactiveAvailable()) {
                assertEquals(batchOptions, clients.forms().defaultBatchWriteOptions());
            } else {
                assertThrows(IllegalStateException.class, clients::executor);
            }
        }
    }

    @Test
    void preservesJdbcAcquisitionErrorsAndInstallsSafeObservers() {
        verifyAcquisitionErrorsAndObservers(false);
    }

    @Test
    void preservesReactiveAcquisitionErrorsAndInstallsSafeObservers() {
        verifyAcquisitionErrorsAndObservers(true);
    }

    @Test
    void installsSqlObserverWithoutRequiringBatchObserver() {
        for (boolean reactive : List.of(false, true)) {
            List<SqlExecutionObservation> observations = new ArrayList<>();
            FlyingOrmEnvironment environment = standaloneEnvironment(reactive,
                    Optional.of(observations::add), Optional.empty());
            try (FlyingOrmClients clients = FlyingOrmBootstrap.create(
                    FlyingOrmConfiguration.defaults().withDialect("postgresql"), environment)) {
                SqlRequest request = new SqlRequest("select 1", List.of());
                assertThrows(RuntimeException.class, () -> {
                    if (reactive) clients.executor().query(request).blockLast(Duration.ofSeconds(5));
                    else clients.syncExecutor().query(request);
                });
                assertEquals(1, observations.size());
                assertEquals(SqlExecutionStatus.ERROR, observations.getFirst().status());
            }
        }
    }

    @Test
    void installsBatchObserverWithoutRequiringSqlObserver() {
        for (boolean reactive : List.of(false, true)) {
            List<BatchExecutionObservation> observations = new ArrayList<>();
            FlyingOrmEnvironment environment = standaloneEnvironment(reactive,
                    Optional.empty(), Optional.of(observations::add));
            DynamicForm form = DynamicForm.builder("samples", "samples")
                    .addField(DynamicField.of("id", "INTEGER")).build();
            BatchSpec spec = BatchSpec.insert(form, Flux.just(Map.of("id", 1)));
            try (FlyingOrmClients clients = FlyingOrmBootstrap.create(
                    FlyingOrmConfiguration.defaults().withDialect("postgresql"), environment)) {
                assertThrows(RuntimeException.class, () -> {
                    if (reactive) clients.forms().writeBatch(spec).block(Duration.ofSeconds(5));
                    else clients.syncForms().writeBatch(spec);
                });
                assertFalse(observations.isEmpty());
                assertEquals(0, observations.getLast().successfulCount());
                assertEquals(com.flying.orm.rdb.batch.BatchExecutionState.FAILED, observations.getLast().state());
            }
        }
    }

    private FlyingOrmEnvironment standaloneEnvironment(boolean reactive,
            Optional<com.flying.orm.rdb.observation.SqlExecutionObserver> sql,
            Optional<BatchExecutionObserver> batch) {
        JdbcConnectionAccess jdbc = JdbcConnectionAccess.of(request -> {
            throw new IllegalStateException("unavailable test connection");
        }, (connection, request) -> { throw new AssertionError("no connection to release"); });
        R2dbcConnectionAccess r2dbc = R2dbcConnectionAccess.of(
                request -> Mono.error(new IllegalStateException("unavailable test connection")),
                (signal, connection, request) -> { throw new AssertionError("no connection to release"); });
        return new FlyingOrmEnvironment(reactive ? Optional.empty() : Optional.of(jdbc),
                reactive ? Optional.of(r2dbc) : Optional.empty(), sql, batch, Optional.empty(), Optional.empty());
    }

    private void verifyAcquisitionErrorsAndObservers(boolean reactive) {
        IllegalStateException failure = new IllegalStateException("connection access failed");
        AtomicInteger acquisitions = new AtomicInteger();
        JdbcConnectionAccess jdbc = JdbcConnectionAccess.of(request -> {
            acquisitions.incrementAndGet();
            throw failure;
        }, (connection, request) -> {
            throw new AssertionError("no JDBC connection was acquired");
        });
        R2dbcConnectionAccess r2dbc = R2dbcConnectionAccess.of(request -> Mono.defer(() -> {
            acquisitions.incrementAndGet();
            return Mono.error(failure);
        }), (signal, connection, request) -> {
            throw new AssertionError("no R2DBC connection was acquired");
        });
        List<SqlExecutionObservation> observations = new ArrayList<>();
        List<String> messages = new ArrayList<>();
        FlyingOrmEnvironment environment = (reactive
                ? FlyingOrmEnvironment.of(r2dbc) : FlyingOrmEnvironment.of(jdbc))
                .withObservers(observations::add, BatchExecutionObserver.noop())
                .withSqlLogSink((level, message) -> messages.add(message));
        FlyingOrmConfiguration configuration = FlyingOrmConfiguration.defaults().withDialect("postgresql")
                .withSqlLog(FlyingOrmConfiguration.SqlLog.enabled(SqlExecutionLogOptions.defaults()));
        SqlRequest request = new SqlRequest("select ?", List.of("private-business-value"));

        try (FlyingOrmClients clients = FlyingOrmBootstrap.create(configuration, environment)) {
            assertEquals(0, acquisitions.get());
            if (reactive) {
                Mono<?> result = clients.executor().query(request).collectList();
                assertEquals(0, acquisitions.get());
                RuntimeException error = assertThrows(RuntimeException.class,
                        () -> result.block(Duration.ofSeconds(5)));
                assertSame(failure, rootCause(error));
            } else {
                RuntimeException error = assertThrows(RuntimeException.class,
                        () -> clients.syncExecutor().query(request));
                assertSame(failure, rootCause(error));
            }
            assertEquals(1, acquisitions.get());
            assertEquals(1, observations.size());
            assertEquals(SqlExecutionStatus.ERROR, observations.getFirst().status());
            assertSame(failure, rootCause(observations.getFirst().error()));
            assertEquals(1, messages.size());
            assertFalse(messages.getFirst().contains("private-business-value"));
            assertFalse(messages.getFirst().contains("select ?"));
            assertFalse(messages.getFirst().contains("parameters="));
            assertFalse(messages.getFirst().contains("sql="));
        }
    }

    private static Throwable rootCause(Throwable error) {
        Throwable cause = error;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }
}
