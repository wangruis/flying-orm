package com.flying.orm.rdb.performance;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchAffectedRows;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.jdbc.JdbcBatchWriter;
import com.flying.orm.rdb.jdbc.JdbcSqlExecutor;
import java.io.PrintWriter;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAccumulator;
import java.util.logging.Logger;
import javax.sql.DataSource;

/**
 * Stand-alone PostgreSQL evidence harness for the upper-layer capability gate.
 *
 * <p>This class deliberately uses only JDK JDBC pooling primitives plus flying-orm public
 * execution APIs. It does not manage a business transaction: each executor invocation owns
 * exactly the connection lifecycle already defined by flying-orm.</p>
 */
public final class PostgresqlCompatibilityLoadHarness {
    private static final String TABLE = "flying_orm_perf_upper_capability";
    private static final int REQUIRED_SEED_ROWS = 100_000;
    private static final int MIN_WARMUP_SECONDS = 30;
    private static final int MIN_MEASUREMENT_SECONDS = 60;
    private static final int MAX_LATENCY_SAMPLES_PER_WORKER = 16_384;
    private static final List<String> SCENARIOS = List.of(
            "primary-key-read",
            "condition-page",
            "single-row-write",
            "batch-100",
            "batch-1000");

    private PostgresqlCompatibilityLoadHarness() {
    }

    public static void main(String[] arguments) throws Exception {
        HarnessConfig config = HarnessConfig.parse(arguments, System.getenv());
        Result result;
        try {
            result = execute(config);
        } catch (Throwable failure) {
            result = Result.failed(config, failure);
        }
        result.write(config.output());
        if (!"PASS".equals(result.status())) {
            System.err.println("PostgreSQL compatibility load failed: " + result.failure());
            System.exit(1);
        }
    }

    private static Result execute(HarnessConfig config) throws Exception {
        Class.forName("org.postgresql.Driver");
        prepareDatabase(config);

        PoolMetrics poolMetrics = new PoolMetrics();
        Instant startedAt = Instant.now();
        try (FixedJdbcPool dataSource = new FixedJdbcPool(config, config.concurrency(), poolMetrics)) {
            DynamicForm form = form();
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(),
                    RdbDialect.postgresql());
            JdbcSqlExecutor executor = JdbcSqlExecutor.create(ConnectionAccessTestSupport.jdbc(dataSource), RdbDialect.postgresql());
            JdbcBatchWriter batchWriter = JdbcBatchWriter.create(ConnectionAccessTestSupport.jdbc(dataSource), RdbDialect.postgresql());
            Operation operation = operation(config, form, renderer, executor, batchWriter);

            runPhase(config.warmupSeconds(), config.concurrency(), operation, poolMetrics, false);
            awaitPoolQuiescence(poolMetrics);

            System.gc();
            GcSnapshot gcBefore = GcSnapshot.capture();
            MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
            long heapStart = memory.getHeapMemoryUsage().getUsed();
            LongAccumulator peakHeap = new LongAccumulator(Long::max, heapStart);
            AtomicBoolean sampling = new AtomicBoolean(true);
            Thread sampler = startSampler(memory, peakHeap, sampling);

            PhaseResult phase;
            try {
                phase = runPhase(
                        config.measurementSeconds(),
                        config.concurrency(),
                        operation,
                        poolMetrics,
                        true);
            } finally {
                sampling.set(false);
                sampler.join(TimeUnit.SECONDS.toMillis(5));
            }
            awaitPoolQuiescence(poolMetrics);
            long heapEnd = memory.getHeapMemoryUsage().getUsed();
            peakHeap.accumulate(heapEnd);
            GcSnapshot gcAfter = GcSnapshot.capture();

            String failure = phase.failure();
            if (poolMetrics.active() != 0 || poolMetrics.pending() != 0) {
                failure = appendFailure(failure, "connection leak after measured phase: active="
                        + poolMetrics.active() + ", pending=" + poolMetrics.pending());
            }
            return Result.completed(
                    config,
                    startedAt,
                    phase,
                    poolMetrics,
                    gcAfter.collections() - gcBefore.collections(),
                    gcAfter.collectionMillis() - gcBefore.collectionMillis(),
                    heapStart,
                    peakHeap.get(),
                    heapEnd,
                    failure);
        }
    }

    private static DynamicForm form() {
        return DynamicForm.builder("upper_capability", TABLE)
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("bucket", "INTEGER"))
                .addField(DynamicField.of("payload", "VARCHAR"))
                .addField(DynamicField.of("version", "BIGINT"))
                .build();
    }

    private static Operation operation(
            HarnessConfig config,
            DynamicForm form,
            FormDataSqlRenderer renderer,
            JdbcSqlExecutor executor,
            JdbcBatchWriter batchWriter) {
        return switch (config.scenario()) {
            case "primary-key-read" -> (worker, sequence) -> executor.query(renderer.select(
                    form,
                    ConditionGroup.and()
                            .where("id", "=", rowId(worker, sequence, config.concurrency()))
                            .build())).size();
            case "condition-page" -> (worker, sequence) -> executor.query(renderer.select(
                    form,
                    ConditionGroup.and().where("bucket", "=", (int) (sequence % 100L)).build(),
                    PageQuery.of(5, 50, PageSort.asc("id")))).size();
            case "single-row-write" -> (worker, sequence) -> {
                long id = rowId(worker, sequence, config.concurrency());
                long updated = executor.rowsUpdated(renderer.update(
                        form,
                        Map.of("payload", "single-" + worker + '-' + sequence, "version", sequence),
                        ConditionGroup.and().where("id", "=", id).build()));
                if (updated != 1L) {
                    throw new IllegalStateException("single-row update affected " + updated + " rows");
                }
                return 1L;
            };
            case "batch-100" -> batchOperation(
                    form, renderer, batchWriter, config.concurrency(), 100);
            case "batch-1000" -> batchOperation(
                    form, renderer, batchWriter, config.concurrency(), 1_000);
            default -> throw new IllegalArgumentException("unsupported scenario: " + config.scenario());
        };
    }

    private static Operation batchOperation(
            DynamicForm form,
            FormDataSqlRenderer renderer,
            JdbcBatchWriter batchWriter,
            int concurrency,
            int batchSize) {
        return (worker, sequence) -> {
            List<Map<String, Object>> rows = batchRows(worker, sequence, concurrency, batchSize);
            BatchExecutionEvidence result = batchWriter.writeBatch(renderer.upsertBatch(
                    form,
                    rows,
                    BatchWriteOptions.of(batchSize)));
            if (result.state() != BatchExecutionState.SUCCESS
                    || result.inputCount() != batchSize
                    || !result.affectedRows().equals(BatchAffectedRows.known(batchSize))) {
                throw new IllegalStateException("batch execution did not complete the input: state="
                        + result.state() + ", inputCount=" + result.inputCount()
                        + ", affectedRows=" + result.affectedRows() + ", expected=" + batchSize);
            }
            return result.affectedRows().value();
        };
    }

    private static List<Map<String, Object>> batchRows(
            int worker,
            long sequence,
            int concurrency,
            int batchSize) {
        int segmentSize = REQUIRED_SEED_ROWS / concurrency;
        int windows = Math.max(1, segmentSize / batchSize);
        long segmentStart = (long) worker * segmentSize + 1L;
        long windowStart = segmentStart + (sequence % windows) * batchSize;
        List<Map<String, Object>> rows = new ArrayList<>(batchSize);
        for (int offset = 0; offset < batchSize; offset++) {
            long id = windowStart + offset;
            rows.add(Map.of(
                    "id", id,
                    "bucket", (int) (id % 100L),
                    "payload", "batch-" + worker + '-' + sequence + '-' + offset,
                    "version", sequence));
        }
        return rows;
    }

    private static long rowId(int worker, long sequence, int concurrency) {
        int segmentSize = REQUIRED_SEED_ROWS / concurrency;
        return (long) worker * segmentSize + (sequence % segmentSize) + 1L;
    }

    private static PhaseResult runPhase(
            int seconds,
            int concurrency,
            Operation operation,
            PoolMetrics poolMetrics,
            boolean recordLatency) throws Exception {
        ExecutorService workers = Executors.newFixedThreadPool(concurrency);
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        AtomicBoolean failed = new AtomicBoolean();
        AtomicReference<Throwable> firstFailure = new AtomicReference<>();
        List<Future<WorkerResult>> futures = new ArrayList<>(concurrency);
        AtomicLong deadlineHolder = new AtomicLong();

        for (int worker = 0; worker < concurrency; worker++) {
            int workerIndex = worker;
            futures.add(workers.submit(() -> {
                LatencySampler latencies = new LatencySampler(MAX_LATENCY_SAMPLES_PER_WORKER);
                ready.countDown();
                start.await();
                // The start latch safely publishes the deadline. Keep it worker-local so the
                // measured loop never contends on or repeatedly reads shared counter state.
                long deadline = deadlineHolder.get();
                long sequence = 0L;
                long localOperations = 0L;
                long localRows = 0L;
                long localErrors = 0L;
                while (!failed.get() && System.nanoTime() < deadline) {
                    long started = System.nanoTime();
                    try {
                        long affectedRows = operation.execute(workerIndex, sequence++);
                        long elapsed = System.nanoTime() - started;
                        localOperations++;
                        localRows += affectedRows;
                        if (recordLatency) {
                            latencies.add(elapsed);
                        }
                    } catch (Throwable failure) {
                        localErrors++;
                        firstFailure.compareAndSet(null, failure);
                        failed.set(true);
                    }
                }
                return new WorkerResult(localOperations, localRows, localErrors, latencies);
            }));
        }

        if (!ready.await(30, TimeUnit.SECONDS)) {
            workers.shutdownNow();
            throw new IllegalStateException("workers did not become ready within 30 seconds");
        }
        long started = System.nanoTime();
        deadlineHolder.set(started + TimeUnit.SECONDS.toNanos(seconds));
        start.countDown();
        workers.shutdown();
        if (!workers.awaitTermination(seconds + 30L, TimeUnit.SECONDS)) {
            workers.shutdownNow();
            throw new IllegalStateException("workers did not stop after the phase deadline");
        }
        long elapsed = Math.max(1L, System.nanoTime() - started);

        List<long[]> sampleParts = new ArrayList<>(concurrency);
        long operations = 0L;
        long rows = 0L;
        long errors = 0L;
        for (Future<WorkerResult> future : futures) {
            WorkerResult worker = future.get();
            operations += worker.operations();
            rows += worker.rows();
            errors += worker.errors();
            sampleParts.add(worker.latencies().values());
        }
        long[] samples = mergeAndSort(sampleParts);
        Throwable failure = firstFailure.get();
        return new PhaseResult(
                operations,
                rows,
                errors,
                elapsed,
                percentile(samples, 0.50d),
                percentile(samples, 0.95d),
                percentile(samples, 0.99d),
                failure == null ? "" : failure.getClass().getName() + ": " + safeMessage(failure),
                poolMetrics.active(),
                poolMetrics.pending());
    }

    private static Thread startSampler(
            MemoryMXBean memory,
            LongAccumulator peakHeap,
            AtomicBoolean sampling) {
        Thread thread = new Thread(() -> {
            while (sampling.get()) {
                peakHeap.accumulate(memory.getHeapMemoryUsage().getUsed());
                try {
                    Thread.sleep(10L);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    return;
                }
            }
        }, "flying-orm-performance-memory-sampler");
        thread.setDaemon(true);
        thread.start();
        return thread;
    }

    private static void awaitPoolQuiescence(PoolMetrics metrics) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10L);
        while ((metrics.active() != 0 || metrics.pending() != 0) && System.nanoTime() < deadline) {
            Thread.sleep(10L);
        }
    }

    private static long[] mergeAndSort(List<long[]> parts) {
        int length = parts.stream().mapToInt(part -> part.length).sum();
        long[] merged = new long[length];
        int offset = 0;
        for (long[] part : parts) {
            System.arraycopy(part, 0, merged, offset, part.length);
            offset += part.length;
        }
        Arrays.sort(merged);
        return merged;
    }

    private static double percentile(long[] sortedNanos, double percentile) {
        if (sortedNanos.length == 0) {
            return 0.0d;
        }
        int index = (int) Math.ceil(percentile * sortedNanos.length) - 1;
        index = Math.max(0, Math.min(index, sortedNanos.length - 1));
        return sortedNanos[index] / 1_000_000.0d;
    }

    private static void prepareDatabase(HarnessConfig config) throws SQLException {
        try (Connection connection = DriverManager.getConnection(
                        config.jdbcUrl(), config.databaseUser(), config.databasePassword());
                Statement statement = connection.createStatement()) {
            statement.execute("CREATE TABLE IF NOT EXISTS " + TABLE + " ("
                    + "id BIGINT PRIMARY KEY, "
                    + "bucket INTEGER NOT NULL, "
                    + "payload VARCHAR(128) NOT NULL, "
                    + "version BIGINT NOT NULL)");
            statement.execute("CREATE INDEX IF NOT EXISTS idx_flying_orm_perf_bucket_id ON "
                    + TABLE + " (bucket, id)");
            statement.execute("TRUNCATE TABLE " + TABLE);
            try (PreparedStatement seed = connection.prepareStatement(
                    "INSERT INTO " + TABLE + " (id, bucket, payload, version) "
                            + "SELECT value, (value % 100)::INTEGER, 'seed-' || value, 0 "
                            + "FROM generate_series(1, ?) AS value")) {
                seed.setInt(1, config.seedRows());
                int inserted = seed.executeUpdate();
                if (inserted != REQUIRED_SEED_ROWS) {
                    throw new SQLException("expected " + REQUIRED_SEED_ROWS
                            + " seed rows but inserted " + inserted);
                }
            }
            statement.execute("ANALYZE " + TABLE);
        }
    }

    private static String appendFailure(String current, String addition) {
        return current == null || current.isBlank() ? addition : current + "; " + addition;
    }

    private static String safeMessage(Throwable failure) {
        String message = failure.getMessage();
        return message == null ? "no message" : message;
    }

    @FunctionalInterface
    private interface Operation {
        long execute(int worker, long sequence) throws Exception;
    }

    private record PhaseResult(
            long operations,
            long rows,
            long errors,
            long elapsedNanos,
            double p50Millis,
            double p95Millis,
            double p99Millis,
            String failure,
            int finalActiveConnections,
            int finalPendingConnections) {
    }

    private record WorkerResult(
            long operations,
            long rows,
            long errors,
            LatencySampler latencies) {
    }

    private record GcSnapshot(long collections, long collectionMillis) {
        private static GcSnapshot capture() {
            long collections = 0L;
            long millis = 0L;
            for (GarbageCollectorMXBean collector : ManagementFactory.getGarbageCollectorMXBeans()) {
                collections += Math.max(0L, collector.getCollectionCount());
                millis += Math.max(0L, collector.getCollectionTime());
            }
            return new GcSnapshot(collections, millis);
        }
    }

    private static final class LatencySampler {
        private final long[] values;
        private long seen;
        private long stride = 1L;
        private long nextSample;
        private int size;

        private LatencySampler(int capacity) {
            values = new long[capacity];
        }

        private void add(long value) {
            long position = seen++;
            if (position != nextSample) {
                return;
            }
            if (size == values.length) {
                compact();
            }
            values[size++] = value;
            nextSample = position + stride;
        }

        /**
         * Keeps every second sample and doubles the interval before accepting more values.
         * The resulting fixed-size sample stays evenly spread from the beginning to the end
         * of the measured window. This is deterministic, allocates nothing in {@link #add(long)},
         * and avoids per-operation random-number generation used by reservoir sampling.
         */
        private void compact() {
            int retained = 0;
            for (int source = 0; source < size; source += 2) {
                values[retained++] = values[source];
            }
            size = retained;
            stride *= 2L;
        }

        private long[] values() {
            return Arrays.copyOf(values, size);
        }
    }

    private static final class PoolMetrics {
        private final AtomicInteger active = new AtomicInteger();
        private final AtomicInteger pending = new AtomicInteger();
        private final LongAccumulator peakActive = new LongAccumulator(Long::max, 0L);
        private final LongAccumulator peakPending = new LongAccumulator(Long::max, 0L);
        private final LongAccumulator peakTotal = new LongAccumulator(Long::max, 0L);

        private void opened(int count) {
            peakTotal.accumulate(count);
        }

        private void borrowing() {
            peakPending.accumulate(pending.incrementAndGet());
        }

        private void borrowed() {
            pending.decrementAndGet();
            peakActive.accumulate(active.incrementAndGet());
        }

        private void returned() {
            active.decrementAndGet();
        }

        private int active() {
            return active.get();
        }

        private int pending() {
            return pending.get();
        }
    }

    private static final class FixedJdbcPool implements DataSource, AutoCloseable {
        private final HarnessConfig config;
        private final PoolMetrics metrics;
        private final BlockingQueue<Connection> idle;
        private final List<Connection> physicalConnections;
        private volatile PrintWriter logWriter;
        private volatile int loginTimeout;

        private FixedJdbcPool(HarnessConfig config, int size, PoolMetrics metrics) throws SQLException {
            this.config = config;
            this.metrics = metrics;
            idle = new ArrayBlockingQueue<>(size);
            physicalConnections = new ArrayList<>(size);
            try {
                for (int index = 0; index < size; index++) {
                    Connection connection = DriverManager.getConnection(
                            config.jdbcUrl(), config.databaseUser(), config.databasePassword());
                    connection.setAutoCommit(true);
                    physicalConnections.add(connection);
                    idle.add(connection);
                }
            } catch (SQLException failure) {
                closePhysicalConnections();
                throw failure;
            }
            metrics.opened(size);
        }

        @Override
        public Connection getConnection() throws SQLException {
            metrics.borrowing();
            Connection physical;
            try {
                physical = idle.take();
            } catch (InterruptedException interrupted) {
                metrics.pending.decrementAndGet();
                Thread.currentThread().interrupt();
                throw new SQLException("interrupted while waiting for a performance connection", interrupted);
            }
            metrics.borrowed();
            InvocationHandler handler = new ReturningConnection(physical, idle, metrics);
            return (Connection) Proxy.newProxyInstance(
                    Connection.class.getClassLoader(),
                    new Class<?>[]{Connection.class},
                    handler);
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            if (!Objects.equals(config.databaseUser(), username)
                    || !Objects.equals(config.databasePassword(), password)) {
                throw new SQLException("the fixed performance pool does not accept alternate credentials");
            }
            return getConnection();
        }

        @Override
        public PrintWriter getLogWriter() {
            return logWriter;
        }

        @Override
        public void setLogWriter(PrintWriter out) {
            logWriter = out;
        }

        @Override
        public void setLoginTimeout(int seconds) {
            loginTimeout = seconds;
            DriverManager.setLoginTimeout(seconds);
        }

        @Override
        public int getLoginTimeout() {
            return loginTimeout;
        }

        @Override
        public Logger getParentLogger() {
            return Logger.getLogger("com.flying.orm.rdb.performance");
        }

        @Override
        public <T> T unwrap(Class<T> iface) throws SQLException {
            if (iface.isInstance(this)) {
                return iface.cast(this);
            }
            throw new SQLException("not a wrapper for " + iface.getName());
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) {
            return iface.isInstance(this);
        }

        @Override
        public void close() throws SQLException {
            if (metrics.active() != 0 || metrics.pending() != 0) {
                throw new SQLException("cannot close performance pool with active="
                        + metrics.active() + ", pending=" + metrics.pending());
            }
            closePhysicalConnections();
        }

        private void closePhysicalConnections() throws SQLException {
            SQLException failure = null;
            for (Connection connection : physicalConnections) {
                try {
                    connection.close();
                } catch (SQLException closeFailure) {
                    if (failure == null) {
                        failure = closeFailure;
                    } else {
                        failure.addSuppressed(closeFailure);
                    }
                }
            }
            idle.clear();
            if (failure != null) {
                throw failure;
            }
        }
    }

    private static final class ReturningConnection implements InvocationHandler {
        private final Connection physical;
        private final BlockingQueue<Connection> idle;
        private final PoolMetrics metrics;
        private final AtomicBoolean returned = new AtomicBoolean();

        private ReturningConnection(
                Connection physical,
                BlockingQueue<Connection> idle,
                PoolMetrics metrics) {
            this.physical = physical;
            this.idle = idle;
            this.metrics = metrics;
        }

        @Override
        public Object invoke(Object proxy, Method method, Object[] arguments) throws Throwable {
            String name = method.getName();
            if ("close".equals(name) && method.getParameterCount() == 0) {
                if (returned.compareAndSet(false, true)) {
                    if (!idle.offer(physical)) {
                        throw new SQLException("performance connection pool overflow");
                    }
                    metrics.returned();
                }
                return null;
            }
            if ("isClosed".equals(name) && method.getParameterCount() == 0) {
                return returned.get() || physical.isClosed();
            }
            if (returned.get()) {
                throw new SQLException("connection was already returned to the performance pool");
            }
            if ("unwrap".equals(name) && method.getParameterCount() == 1) {
                Class<?> requested = (Class<?>) arguments[0];
                if (requested.isInstance(physical)) {
                    return requested.cast(physical);
                }
            }
            if ("isWrapperFor".equals(name) && method.getParameterCount() == 1) {
                return ((Class<?>) arguments[0]).isInstance(physical);
            }
            try {
                return method.invoke(physical, arguments);
            } catch (InvocationTargetException failure) {
                throw failure.getCause();
            }
        }
    }

    private record HarnessConfig(
            String scenario,
            int concurrency,
            int seedRows,
            int warmupSeconds,
            int measurementSeconds,
            int round,
            String variant,
            Path output,
            String jdbcUrl,
            String databaseUser,
            String databasePassword,
            String harnessSha256,
            String coreJarSha256,
            String rdbJarSha256) {

        private static HarnessConfig parse(String[] arguments, Map<String, String> environment) {
            Properties options = new Properties();
            for (String argument : arguments) {
                if (!argument.startsWith("--") || !argument.contains("=")) {
                    throw new IllegalArgumentException("arguments must use --name=value: " + argument);
                }
                int separator = argument.indexOf('=');
                options.setProperty(argument.substring(2, separator), argument.substring(separator + 1));
            }
            String scenario = required(options, "scenario");
            int concurrency = integer(options, "concurrency");
            int seedRows = integer(options, "seed-rows");
            int warmup = integer(options, "warmup-seconds");
            int measurement = integer(options, "measurement-seconds");
            int round = integer(options, "round");
            String variant = required(options, "variant");
            Path output = checkedOutput(Path.of(required(options, "output")));
            if (!SCENARIOS.contains(scenario)) {
                throw new IllegalArgumentException("unsupported scenario: " + scenario);
            }
            if (concurrency != 1 && concurrency != 32) {
                throw new IllegalArgumentException("concurrency must be 1 or 32");
            }
            if (seedRows != REQUIRED_SEED_ROWS) {
                throw new IllegalArgumentException("seed-rows must be exactly " + REQUIRED_SEED_ROWS);
            }
            if (warmup < MIN_WARMUP_SECONDS) {
                throw new IllegalArgumentException("warmup-seconds must be at least " + MIN_WARMUP_SECONDS);
            }
            if (measurement < MIN_MEASUREMENT_SECONDS) {
                throw new IllegalArgumentException(
                        "measurement-seconds must be at least " + MIN_MEASUREMENT_SECONDS);
            }
            if (round < 1 || round > 5) {
                throw new IllegalArgumentException("round must be between 1 and 5");
            }
            if (!"baseline".equals(variant) && !"candidate".equals(variant)) {
                throw new IllegalArgumentException("variant must be baseline or candidate");
            }
            return new HarnessConfig(
                    scenario,
                    concurrency,
                    seedRows,
                    warmup,
                    measurement,
                    round,
                    variant,
                    output,
                    required(environment, "FLYING_ORM_PERF_JDBC_URL"),
                    required(environment, "FLYING_ORM_PERF_DB_USER"),
                    required(environment, "FLYING_ORM_PERF_DB_PASSWORD"),
                    environment.getOrDefault("FLYING_ORM_PERF_HARNESS_SHA256", "unknown"),
                    environment.getOrDefault("FLYING_ORM_PERF_CORE_SHA256", "unknown"),
                    environment.getOrDefault("FLYING_ORM_PERF_RDB_SHA256", "unknown"));
        }

        private static String required(Properties options, String name) {
            String value = options.getProperty(name);
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("missing --" + name + "=value");
            }
            return value;
        }

        private static String required(Map<String, String> environment, String name) {
            String value = environment.get(name);
            if (value == null || value.isBlank()) {
                throw new IllegalArgumentException("missing environment variable " + name);
            }
            return value;
        }

        private static int integer(Properties options, String name) {
            try {
                return Integer.parseInt(required(options, name));
            } catch (NumberFormatException failure) {
                throw new IllegalArgumentException("--" + name + " must be an integer", failure);
            }
        }

        private static Path checkedOutput(Path requested) {
            Path output = requested.toAbsolutePath().normalize();
            String normalized = output.toString().replace('\\', '/').toLowerCase();
            if (!normalized.contains("/.tmp/performance/upper-capability/")) {
                throw new IllegalArgumentException(
                        "output must be inside .tmp/performance/upper-capability/<run-id>");
            }
            return output;
        }
    }

    private record Result(
            String schemaVersion,
            String protocol,
            String variant,
            int round,
            String scenario,
            int concurrency,
            int seedRows,
            int warmupSeconds,
            int measurementSeconds,
            String harnessSha256,
            String coreJarSha256,
            String rdbJarSha256,
            String startedAt,
            String status,
            long operations,
            long rows,
            double operationsPerSecond,
            double rowsPerSecond,
            double p50Millis,
            double p95Millis,
            double p99Millis,
            long errors,
            long peakActiveConnections,
            long peakTotalConnections,
            long peakPendingConnections,
            int finalActiveConnections,
            int finalPendingConnections,
            long gcCollections,
            long gcMillis,
            long heapStartBytes,
            long peakHeapBytes,
            long heapEndBytes,
            String failure) {

        private static Result completed(
                HarnessConfig config,
                Instant startedAt,
                PhaseResult phase,
                PoolMetrics metrics,
                long gcCollections,
                long gcMillis,
                long heapStart,
                long peakHeap,
                long heapEnd,
                String failure) {
            double seconds = phase.elapsedNanos() / 1_000_000_000.0d;
            String status = phase.errors() == 0L && (failure == null || failure.isBlank()) ? "PASS" : "FAIL";
            return new Result(
                    "1",
                    "upper-capability-v1",
                    config.variant(),
                    config.round(),
                    config.scenario(),
                    config.concurrency(),
                    config.seedRows(),
                    config.warmupSeconds(),
                    config.measurementSeconds(),
                    config.harnessSha256(),
                    config.coreJarSha256(),
                    config.rdbJarSha256(),
                    startedAt.toString(),
                    status,
                    phase.operations(),
                    phase.rows(),
                    phase.operations() / seconds,
                    phase.rows() / seconds,
                    phase.p50Millis(),
                    phase.p95Millis(),
                    phase.p99Millis(),
                    phase.errors(),
                    metrics.peakActive.get(),
                    metrics.peakTotal.get(),
                    metrics.peakPending.get(),
                    phase.finalActiveConnections(),
                    phase.finalPendingConnections(),
                    gcCollections,
                    gcMillis,
                    heapStart,
                    peakHeap,
                    heapEnd,
                    failure == null ? "" : failure);
        }

        private static Result failed(HarnessConfig config, Throwable failure) {
            return new Result(
                    "1",
                    "upper-capability-v1",
                    config.variant(),
                    config.round(),
                    config.scenario(),
                    config.concurrency(),
                    config.seedRows(),
                    config.warmupSeconds(),
                    config.measurementSeconds(),
                    config.harnessSha256(),
                    config.coreJarSha256(),
                    config.rdbJarSha256(),
                    Instant.now().toString(),
                    "FAIL",
                    0L,
                    0L,
                    0.0d,
                    0.0d,
                    0.0d,
                    0.0d,
                    0.0d,
                    1L,
                    0L,
                    0L,
                    0L,
                    0,
                    0,
                    0L,
                    0L,
                    0L,
                    0L,
                    0L,
                    failure.getClass().getName() + ": " + safeMessage(failure));
        }

        private void write(Path output) throws Exception {
            Files.createDirectories(output.getParent());
            Files.writeString(output, json(), StandardCharsets.UTF_8);
        }

        private String json() {
            return "{\n"
                    + field("schemaVersion", schemaVersion) + ",\n"
                    + field("protocol", protocol) + ",\n"
                    + field("variant", variant) + ",\n"
                    + number("round", round) + ",\n"
                    + field("scenario", scenario) + ",\n"
                    + number("concurrency", concurrency) + ",\n"
                    + number("seedRows", seedRows) + ",\n"
                    + number("warmupSeconds", warmupSeconds) + ",\n"
                    + number("measurementSeconds", measurementSeconds) + ",\n"
                    + field("harnessSha256", harnessSha256) + ",\n"
                    + field("coreJarSha256", coreJarSha256) + ",\n"
                    + field("rdbJarSha256", rdbJarSha256) + ",\n"
                    + field("startedAt", startedAt) + ",\n"
                    + field("status", status) + ",\n"
                    + number("operations", operations) + ",\n"
                    + number("rows", rows) + ",\n"
                    + decimal("operationsPerSecond", operationsPerSecond) + ",\n"
                    + decimal("rowsPerSecond", rowsPerSecond) + ",\n"
                    + decimal("p50Millis", p50Millis) + ",\n"
                    + decimal("p95Millis", p95Millis) + ",\n"
                    + decimal("p99Millis", p99Millis) + ",\n"
                    + number("errors", errors) + ",\n"
                    + number("peakActiveConnections", peakActiveConnections) + ",\n"
                    + number("peakTotalConnections", peakTotalConnections) + ",\n"
                    + number("peakPendingConnections", peakPendingConnections) + ",\n"
                    + number("finalActiveConnections", finalActiveConnections) + ",\n"
                    + number("finalPendingConnections", finalPendingConnections) + ",\n"
                    + number("gcCollections", gcCollections) + ",\n"
                    + number("gcMillis", gcMillis) + ",\n"
                    + number("heapStartBytes", heapStartBytes) + ",\n"
                    + number("peakHeapBytes", peakHeapBytes) + ",\n"
                    + number("heapEndBytes", heapEndBytes) + ",\n"
                    + field("failure", failure) + "\n}\n";
        }

        private static String field(String name, String value) {
            return "  \"" + name + "\": \"" + escape(value) + "\"";
        }

        private static String number(String name, long value) {
            return "  \"" + name + "\": " + value;
        }

        private static String decimal(String name, double value) {
            return "  \"" + name + "\": " + String.format(java.util.Locale.ROOT, "%.6f", value);
        }

        private static String escape(String value) {
            return value.replace("\\", "\\\\")
                    .replace("\"", "\\\"")
                    .replace("\r", "\\r")
                    .replace("\n", "\\n")
                    .replace("\t", "\\t");
        }
    }
}
