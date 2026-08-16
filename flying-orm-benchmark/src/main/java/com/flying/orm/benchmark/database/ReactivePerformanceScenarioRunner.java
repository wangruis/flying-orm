package com.flying.orm.benchmark.database;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/** 执行单个响应式性能场景，并把延迟、失败和资源峰值收口为统一报告。 */
final class ReactivePerformanceScenarioRunner {

    static final Duration SQL_TIMEOUT = Duration.ofSeconds(10);
    private static final Duration CLOSE_TIMEOUT = Duration.ofSeconds(20);
    private static final int TRANSACTION_UPDATE_BATCH_SIZE = 8;
    private static final String MYSQL_WAIT_SQL =
            "select EVENT_NAME, COUNT_STAR, SUM_TIMER_WAIT, MAX_TIMER_WAIT "
                    + "from performance_schema.events_waits_summary_global_by_event_name "
                    + "where EVENT_NAME in ('wait/io/file/sql/binlog', "
                    + "'wait/io/file/innodb/innodb_log_file')";

    private ReactivePerformanceScenarioRunner() {
    }

    static DatabasePerformanceReport.ScenarioResult runWithDiagnostics(
            ReactivePerformanceTarget target,
            R2dbcSqlExecutor diagnosticsExecutor,
            String name,
            int concurrency,
            long rowsPerOperation,
            ReactivePerformanceArguments arguments,
            ConnectionPool pool,
            Supplier<? extends Publisher<?>> operation) {
        MySqlWaitSnapshot before = captureMySqlWaits(target, diagnosticsExecutor);
        DatabasePerformanceReport.ScenarioResult result = run(
                name, concurrency, rowsPerOperation, arguments, pool, operation);
        MySqlWaitSnapshot after = captureMySqlWaits(target, diagnosticsExecutor);
        if (before != null && after != null) {
            printWaitDelta(name, after.minus(before));
        }
        return result;
    }

    static Mono<Void> queryById(R2dbcSqlExecutor executor,
                                String table,
                                AtomicLong sequence,
                                int seedRows,
                                SqlExecutionOptions options) {
        long id = Math.floorMod(sequence.getAndIncrement(), seedRows) + 1L;
        return executor.query(new SqlRequest(
                               "select ID, NAME, VALUE from " + table + " where ID = ?", List.of(id)), options)
                       .single()
                       .then();
    }

    /** 使用完整 R2DBC SPI 生命周期建立 ORM 查询路径的诊断对照，不进入 flying-orm 生产实现。 */
    static Mono<Void> rawQueryById(ConnectionFactory connectionFactory,
                                   String table,
                                   String bindMarker,
                                   AtomicLong sequence,
                                   int seedRows,
                                   int fetchSize) {
        ConnectionFactory safeFactory = Objects.requireNonNull(
                connectionFactory, "raw query connection factory must not be null");
        String safeTable = Objects.requireNonNull(table, "raw query table must not be null");
        String safeMarker = Objects.requireNonNull(bindMarker, "raw query bind marker must not be null");
        AtomicLong safeSequence = Objects.requireNonNull(sequence, "raw query sequence must not be null");
        if (seedRows <= 0 || fetchSize < 0) {
            throw new IllegalArgumentException("raw query seed rows and fetch size are outside their safe range");
        }
        long id = Math.floorMod(safeSequence.getAndIncrement(), seedRows) + 1L;
        String sql = "select ID, NAME, VALUE from " + safeTable + " where ID = " + safeMarker;
        return Mono.usingWhen(
                Mono.from(safeFactory.create()),
                connection -> Mono.defer(() -> {
                    Statement statement = connection.createStatement(sql);
                    if (fetchSize > 0) {
                        statement = statement.fetchSize(fetchSize);
                    }
                    statement = statement.bind(0, id);
                    return Flux.from(statement.execute())
                               .concatMap(result -> Flux.from(result.map((row, metadata) -> 1L)), 1)
                               .single()
                               .then();
                }),
                connection -> Mono.from(connection.close()),
                (connection, ignored) -> Mono.from(connection.close()),
                connection -> Mono.from(connection.close()));
    }

    static Mono<Void> updateById(R2dbcSqlExecutor executor,
                                 String table,
                                 AtomicLong sequence,
                                 int seedRows,
                                 SqlExecutionOptions options) {
        long id = Math.floorMod(sequence.getAndIncrement(), seedRows) + 1L;
        return executor.rowsUpdated(new SqlRequest(
                               "update " + table + " set VALUE = VALUE + 1 where ID = ?", List.of(id)), options)
                       .flatMap(updated -> updated == 1 ? Mono.empty() : Mono.error(
                               new IllegalStateException("performance update did not affect one row")));
    }

    static Mono<Void> transactionalUpdateBatch(ConnectionFactory connectionFactory,
                                                String table,
                                                String bindMarker,
                                                long firstSequence,
                                                int seedRows) {
        String sql = "update " + table + " set VALUE = VALUE + 1 where ID = " + bindMarker;
        return Mono.usingWhen(
                Mono.from(connectionFactory.create()),
                connection -> Mono.from(connection.beginTransaction())
                                  .thenMany(Flux.range(0, TRANSACTION_UPDATE_BATCH_SIZE)
                                                // 同一连接顺序执行，避免驱动并发 Statement 改变对照场景语义。
                                                .concatMap(offset -> updateInsideTransaction(
                                                        connection, sql,
                                                        Math.floorMod(firstSequence + offset, seedRows) + 1L)))
                                  .then(Mono.from(connection.commitTransaction())),
                connection -> Mono.from(connection.close()),
                (connection, ignored) -> rollbackThenClose(connection),
                ReactivePerformanceScenarioRunner::rollbackThenClose);
    }

    static Mono<Void> batchInsert(R2dbcSqlExecutor executor,
                                  String table,
                                  long firstId,
                                  int batchSize,
                                  BatchWriteOptions options) {
        Flux<Object[]> rows = Flux.range(0, batchSize)
                                  .map(offset -> new Object[]{firstId + offset,
                                          "batch-" + (firstId + offset), firstId + offset});
        BatchWriteRequest request = new BatchWriteRequest(
                "insert into " + table + " (ID, NAME, VALUE) values (?, ?, ?)",
                3, List.of(Long.class, String.class, Long.class),
                SqlBindMarkerStyle.CANONICAL, rows, options);
        return executor.writeBatch(request)
                       .flatMap(result -> committedBatch(result, batchSize) ? Mono.empty() : Mono.error(
                               new IllegalStateException("performance batch did not commit every input row")));
    }

    static int transactionBatchSize() {
        return TRANSACTION_UPDATE_BATCH_SIZE;
    }

    private static DatabasePerformanceReport.ScenarioResult run(
            String name,
            int concurrency,
            long rowsPerOperation,
            ReactivePerformanceArguments arguments,
            ConnectionPool pool,
            Supplier<? extends Publisher<?>> operation) {
        ReactivePerformanceResourceSampler resources = ReactivePerformanceResourceSampler.start(pool);
        DatabaseOperationPhaseRecorder phases = arguments.phaseDiagnostics
                ? new DatabaseOperationPhaseRecorder() : null;
        ReactiveDatabaseLoadProbe.Result result;
        try {
            result = ReactiveDatabaseLoadProbe.run(
                    new ReactiveDatabaseLoadProbe.Plan(
                            Duration.ofSeconds(arguments.warmupSeconds),
                            Duration.ofSeconds(arguments.measurementSeconds),
                            concurrency, rowsPerOperation, SQL_TIMEOUT),
                    operation, phases).block(Duration.ofSeconds(
                            arguments.warmupSeconds + arguments.measurementSeconds
                                    + SQL_TIMEOUT.toSeconds() * 4));
        } finally {
            resources.stop();
        }
        Objects.requireNonNull(result, "database performance load result must not be null");
        ReactivePerformanceDatabaseRunner.awaitPoolIdle(pool);
        ReactivePerformanceResourceSampler.Snapshot snapshot = resources.snapshot();
        boolean resourceFailure = snapshot.peakAllocatedConnections() > arguments.poolSize
                || snapshot.peakAcquiredConnections() > arguments.poolSize
                || ReactivePerformanceDatabaseRunner.metrics(pool).acquiredSize() != 0
                || ReactivePerformanceDatabaseRunner.metrics(pool).pendingAcquireSize() != 0;
        DatabasePerformanceReport.Status status = result.failed() == 0 && result.warmupFailures() == 0
                && !resourceFailure ? DatabasePerformanceReport.Status.PASSED
                : DatabasePerformanceReport.Status.FAILED;
        return new DatabasePerformanceReport.ScenarioResult(
                name, status, concurrency, result.operations(), result.succeeded(), result.failed(),
                result.rows(), result.warmupFailures(), result.elapsed().toMillis(),
                result.operationsPerSecond(), result.rowsPerSecond(), result.errorRate(),
                millis(result.p50Nanos()), millis(result.p95Nanos()), millis(result.p99Nanos()),
                millis(result.maxNanos()), result.failuresByType(), snapshot.peakAllocatedConnections(),
                snapshot.peakAcquiredConnections(), snapshot.peakPendingAcquires(),
                snapshot.averageProcessCpuPercent(), snapshot.peakHeapBytes(),
                phases == null ? null : phases.snapshot());
    }

    private static MySqlWaitSnapshot captureMySqlWaits(ReactivePerformanceTarget target,
                                                       R2dbcSqlExecutor executor) {
        if (!"mysql".equals(target.key()) || executor == null) {
            return null;
        }
        try {
            List<DynamicRow> rows = executor.query(SqlRequest.nativeSql(MYSQL_WAIT_SQL, List.of()))
                                             .collectList().block(CLOSE_TIMEOUT);
            return MySqlWaitSnapshot.fromRows(Objects.requireNonNull(
                    rows, "MySQL wait snapshot rows must not be null"));
        } catch (RuntimeException error) {
            System.out.println("MYSQL_WAIT_DELTA unavailable=" + error.getClass().getSimpleName());
            return null;
        }
    }

    private static void printWaitDelta(String name, MySqlWaitSnapshot delta) {
        System.out.printf(Locale.ROOT,
                          "MYSQL_WAIT_DELTA scenario=%s binlogCount=%d binlogWaitMs=%.3f "
                                  + "binlogGlobalMaxMs=%.3f redoCount=%d redoWaitMs=%.3f "
                                  + "redoGlobalMaxMs=%.3f%n",
                          name, delta.binlogCount(), delta.binlogWaitMillis(), delta.binlogMaxMillis(),
                          delta.redoCount(), delta.redoWaitMillis(), delta.redoMaxMillis());
    }

    private static Mono<Void> updateInsideTransaction(Connection connection, String sql, long id) {
        return Mono.from(connection.createStatement(sql).bind(0, id).execute())
                   .flatMap(result -> Mono.from(result.getRowsUpdated()))
                   .flatMap(updated -> updated == 1 ? Mono.empty() : Mono.error(
                           new IllegalStateException("transaction performance update did not affect one row")));
    }

    private static Mono<Void> rollbackThenClose(Connection connection) {
        return Mono.from(connection.rollbackTransaction()).onErrorResume(ignored -> Mono.empty())
                   .then(Mono.from(connection.close()));
    }

    private static boolean committedBatch(BatchWriteResult result, int expectedRows) {
        return result.status() == BatchWriteResult.Status.COMMITTED
                && result.inputCount() == expectedRows && result.affectedRows() == expectedRows;
    }

    private static double millis(long nanos) {
        return nanos / 1_000_000.0;
    }
}
