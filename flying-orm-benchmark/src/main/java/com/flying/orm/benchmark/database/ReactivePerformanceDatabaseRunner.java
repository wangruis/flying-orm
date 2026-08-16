package com.flying.orm.benchmark.database;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.ConnectionPoolConfiguration;
import io.r2dbc.pool.PoolMetrics;
import io.r2dbc.spi.ConnectionFactories;
import io.r2dbc.spi.ConnectionFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/** 负责单个数据库的连接池生命周期、准备数据和固定场景编排。 */
final class ReactivePerformanceDatabaseRunner {

    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(20);
    private static final Duration POOL_SETTLE_TIMEOUT = Duration.ofSeconds(5);

    private ReactivePerformanceDatabaseRunner() {
    }

    static DatabasePerformanceReport.DatabaseResult run(ReactivePerformanceTarget target,
                                                        ReactivePerformanceArguments arguments) {
        ConnectionFactory driver = ConnectionFactories.get(target.url());
        ConnectionPool pool = new ConnectionPool(ConnectionPoolConfiguration.builder(driver)
                .name("flying-orm-perf-" + target.key())
                .initialSize(arguments.poolSize)
                .minIdle(arguments.poolSize)
                .maxSize(arguments.poolSize)
                .maxAcquireTime(ReactivePerformanceScenarioRunner.SQL_TIMEOUT)
                .maxIdleTime(Duration.ofMinutes(5))
                .build());
        ConnectionFactory executionFactory = arguments.phaseDiagnostics
                ? new PhaseTimingConnectionFactory(pool) : pool;
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(executionFactory);
        R2dbcSqlExecutor diagnostics = "mysql".equals(target.key()) && arguments.mysqlDiagnosticsConfigured()
                ? R2dbcSqlExecutor.create(ConnectionFactories.get(arguments.mysqlDiagnosticsUrl)) : null;
        List<DatabasePerformanceReport.ScenarioResult> scenarios = new ArrayList<>();
        try {
            pool.warmup().block(CLOSE_TIMEOUT);
            prepareTable(executor, target, arguments.seedRows);
            String databaseVersion = databaseVersion(executor, target.versionSql());
            runScenarios(target, arguments, pool, executionFactory, executor, diagnostics, scenarios);
            awaitPoolIdle(pool);
            PoolMetrics finalMetrics = metrics(pool);
            DatabasePerformanceReport.Status status = scenarios.stream()
                    .allMatch(result -> result.status() == DatabasePerformanceReport.Status.PASSED)
                    ? DatabasePerformanceReport.Status.PASSED : DatabasePerformanceReport.Status.FAILED;
            return new DatabasePerformanceReport.DatabaseResult(
                    target.name(), databaseVersion, driver.getMetadata().getName(),
                    BenchmarkRunIdentity.implementationVersion(driver.getClass()),
                    BenchmarkRunIdentity.classSha256(driver.getClass()), poolVersion(), status,
                    finalMetrics.allocatedSize(), finalMetrics.acquiredSize(), finalMetrics.pendingAcquireSize(),
                    scenarios);
        } finally {
            cleanup(executor, target.dropSql());
            pool.disposeLater().onErrorResume(ignored -> Mono.empty()).block(CLOSE_TIMEOUT);
        }
    }

    private static void runScenarios(ReactivePerformanceTarget target,
                                     ReactivePerformanceArguments arguments,
                                     ConnectionPool pool,
                                     ConnectionFactory executionFactory,
                                     R2dbcSqlExecutor executor,
                                     R2dbcSqlExecutor diagnostics,
                                     List<DatabasePerformanceReport.ScenarioResult> scenarios) {
        SqlExecutionOptions sqlOptions = queryOptions(arguments);
        if (arguments.includes(ReactivePerformanceScenario.QUERY_BY_ID)) {
            AtomicLong sequence = new AtomicLong();
            scenarios.add(runScenario(target, diagnostics, ReactivePerformanceScenario.QUERY_BY_ID,
                                      arguments.queryConcurrency, 1, arguments, pool,
                                      () -> ReactivePerformanceScenarioRunner.queryById(
                                               executor, target.table(), sequence, arguments.seedRows, sqlOptions)));
        }
        if (arguments.includes(ReactivePerformanceScenario.RAW_QUERY_BY_ID)) {
            AtomicLong sequence = new AtomicLong();
            scenarios.add(runScenario(target, diagnostics, ReactivePerformanceScenario.RAW_QUERY_BY_ID,
                                      arguments.queryConcurrency, 1, arguments, pool,
                                      () -> ReactivePerformanceScenarioRunner.rawQueryById(
                                              executionFactory, target.table(), target.bindMarker(), sequence,
                                              arguments.seedRows, arguments.effectiveFetchSize())));
        }
        if (arguments.includes(ReactivePerformanceScenario.UPDATE_BY_ID)) {
            AtomicLong sequence = new AtomicLong();
            scenarios.add(runScenario(target, diagnostics, ReactivePerformanceScenario.UPDATE_BY_ID,
                                      arguments.queryConcurrency, 1, arguments, pool,
                                      () -> ReactivePerformanceScenarioRunner.updateById(
                                              executor, target.table(), sequence, arguments.seedRows, sqlOptions)));
        }
        if (arguments.includes(ReactivePerformanceScenario.TRANSACTIONAL_UPDATE_BATCH)) {
            AtomicLong sequence = new AtomicLong();
            int size = ReactivePerformanceScenarioRunner.transactionBatchSize();
            scenarios.add(runScenario(target, diagnostics,
                                      ReactivePerformanceScenario.TRANSACTIONAL_UPDATE_BATCH,
                                      arguments.queryConcurrency, size, arguments, pool,
                                      () -> ReactivePerformanceScenarioRunner.transactionalUpdateBatch(
                                              executionFactory, target.table(), target.bindMarker(),
                                              sequence.getAndAdd(size), arguments.seedRows)));
        }
        if (arguments.includes(ReactivePerformanceScenario.ATOMIC_BATCH_INSERT)) {
            AtomicLong ids = new AtomicLong(1_000_000L);
            BatchWriteOptions options = BatchWriteOptions.atomic(arguments.batchSize)
                    .withTimeout(ReactivePerformanceScenarioRunner.SQL_TIMEOUT);
            scenarios.add(runBatchScenario(target, diagnostics, ReactivePerformanceScenario.ATOMIC_BATCH_INSERT,
                                           arguments, pool, executor, ids, options));
        }
        if (arguments.includes(ReactivePerformanceScenario.INDEPENDENT_BATCH_INSERT)) {
            AtomicLong ids = new AtomicLong(1_000_000_000L);
            BatchWriteOptions options = BatchWriteOptions.independent(
                    arguments.independentChunkSize, arguments.independentConcurrency)
                    .withTimeout(ReactivePerformanceScenarioRunner.SQL_TIMEOUT);
            scenarios.add(runBatchScenario(target, diagnostics,
                                           ReactivePerformanceScenario.INDEPENDENT_BATCH_INSERT,
                                           arguments, pool, executor, ids, options));
        }
    }

    static SqlExecutionOptions queryOptions(ReactivePerformanceArguments arguments) {
        ReactivePerformanceArguments safeArguments = Objects.requireNonNull(
                arguments, "database performance arguments must not be null");
        SqlExecutionOptions options = SqlExecutionOptions.timeout(ReactivePerformanceScenarioRunner.SQL_TIMEOUT);
        return safeArguments.fetchSize() == null
                ? options : options.withFetchSize(safeArguments.fetchSize());
    }

    private static DatabasePerformanceReport.ScenarioResult runBatchScenario(
            ReactivePerformanceTarget target,
            R2dbcSqlExecutor diagnostics,
            ReactivePerformanceScenario scenario,
            ReactivePerformanceArguments arguments,
            ConnectionPool pool,
            R2dbcSqlExecutor executor,
            AtomicLong ids,
            BatchWriteOptions options) {
        return runScenario(target, diagnostics, scenario, arguments.batchConcurrency, arguments.batchSize,
                           arguments, pool, () -> ReactivePerformanceScenarioRunner.batchInsert(
                                   executor, target.table(), ids.getAndAdd(arguments.batchSize),
                                   arguments.batchSize, options));
    }

    private static DatabasePerformanceReport.ScenarioResult runScenario(
            ReactivePerformanceTarget target,
            R2dbcSqlExecutor diagnostics,
            ReactivePerformanceScenario scenario,
            int concurrency,
            long rowsPerOperation,
            ReactivePerformanceArguments arguments,
            ConnectionPool pool,
            java.util.function.Supplier<? extends org.reactivestreams.Publisher<?>> operation) {
        return ReactivePerformanceScenarioRunner.runWithDiagnostics(
                target, diagnostics, scenario.externalName, concurrency, rowsPerOperation,
                arguments, pool, operation);
    }

    private static void prepareTable(R2dbcSqlExecutor executor,
                                     ReactivePerformanceTarget target,
                                     int seedRows) {
        cleanup(executor, target.dropSql());
        executor.rowsUpdated(SqlRequest.nativeSql(target.createSql(), List.of())).block(CLOSE_TIMEOUT);
        Flux<Object[]> rows = Flux.range(1, seedRows)
                                  .map(id -> new Object[]{(long) id, "seed-" + id, (long) id});
        executor.writeBatch(new BatchWriteRequest(
                "insert into " + target.table() + " (ID, NAME, VALUE) values (?, ?, ?)",
                3, List.of(Long.class, String.class, Long.class), SqlBindMarkerStyle.CANONICAL,
                rows, BatchWriteOptions.atomic(Math.max(1, seedRows))))
                .block(Duration.ofMinutes(2));
    }

    private static String databaseVersion(R2dbcSqlExecutor executor, String versionSql) {
        return executor.query(SqlRequest.nativeSql(versionSql, List.of()))
                       .map(row -> String.valueOf(row.values().iterator().next()))
                       .single().block(CLOSE_TIMEOUT);
    }

    private static void cleanup(R2dbcSqlExecutor executor, String sql) {
        executor.rowsUpdated(SqlRequest.nativeSql(sql, List.of()))
                .onErrorResume(ignored -> Mono.empty()).block(CLOSE_TIMEOUT);
    }

    static void awaitPoolIdle(ConnectionPool pool) {
        long deadline = System.nanoTime() + POOL_SETTLE_TIMEOUT.toNanos();
        while (System.nanoTime() < deadline) {
            PoolMetrics current = metrics(pool);
            if (current.acquiredSize() == 0 && current.pendingAcquireSize() == 0) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("interrupted while waiting for performance pool to become idle", error);
            }
        }
        PoolMetrics current = metrics(pool);
        throw new IllegalStateException("performance pool did not become idle: acquired="
                                                + current.acquiredSize() + ", pending="
                                                + current.pendingAcquireSize());
    }

    static PoolMetrics metrics(ConnectionPool pool) {
        return pool.getMetrics().orElseThrow(
                () -> new IllegalStateException("R2DBC pool metrics are unavailable"));
    }

    static String poolVersion() {
        String version = ConnectionPool.class.getPackage().getImplementationVersion();
        return version == null || version.isBlank() ? "r2dbc-pool 1.0.2.RELEASE" : "r2dbc-pool " + version;
    }
}
