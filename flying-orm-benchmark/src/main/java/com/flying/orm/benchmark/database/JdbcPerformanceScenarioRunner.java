package com.flying.orm.benchmark.database;

import com.zaxxer.hikari.HikariDataSource;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.LongAdder;

/** 用固定数量的 Java 21 虚拟线程压测阻塞 JDBC，Hikari 最大连接数继续独立限制数据库资源。 */
final class JdbcPerformanceScenarioRunner {

    private static final long HIGHEST_TRACKABLE_NANOS = 120L * 1_000_000_000L;

    DatabasePerformanceReport.ScenarioResult run(String name,
                                                  int concurrency,
                                                  JdbcPerformanceArguments arguments,
                                                  HikariDataSource dataSource,
                                                  JdbcOperation operation) {
        Objects.requireNonNull(name, "JDBC benchmark scenario name must not be null");
        Objects.requireNonNull(arguments, "JDBC benchmark arguments must not be null");
        Objects.requireNonNull(dataSource, "JDBC benchmark data source must not be null");
        Objects.requireNonNull(operation, "JDBC benchmark operation must not be null");
        Recorder latency = new Recorder(HIGHEST_TRACKABLE_NANOS, 3);
        RunCounters counters = new RunCounters();
        JdbcPoolSampler sampler = new JdbcPoolSampler();
        ExecutorService workers = newWorkerExecutor();
        CountDownLatch ready = new CountDownLatch(concurrency);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<?>> futures = new ArrayList<>(concurrency);
        for (int index = 0; index < concurrency; index++) {
            futures.add(workers.submit(() -> runWorker(ready, start, arguments, operation, dataSource,
                                                       sampler, latency, counters)));
        }
        try {
            if (!ready.await(30, java.util.concurrent.TimeUnit.SECONDS)) {
                throw new IllegalStateException("JDBC benchmark workers did not become ready");
            }
            start.countDown();
            for (Future<?> future : futures) {
                future.get();
            }
            long elapsedNanos = Math.max(1L, arguments.measurementSeconds * 1_000_000_000L);
            return result(name, concurrency, counters, latency, sampler.snapshot(), elapsedNanos);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("JDBC benchmark was interrupted", error);
        } catch (java.util.concurrent.ExecutionException error) {
            throw new IllegalStateException("JDBC benchmark worker failed", error.getCause());
        } finally {
            workers.shutdownNow();
        }
    }

    /** 每个在途 JDBC 请求独占一个虚拟线程，物理数据库并发仍由 Hikari 有界池控制。 */
    static ExecutorService newWorkerExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }

    private static void runWorker(CountDownLatch ready,
                                  CountDownLatch start,
                                  JdbcPerformanceArguments arguments,
                                  JdbcOperation operation,
                                  HikariDataSource dataSource,
                                  JdbcPoolSampler sampler,
                                  Recorder latency,
                                  RunCounters counters) {
        ready.countDown();
        try {
            start.await();
            long base = System.nanoTime();
            long warmupDeadline = base + arguments.warmupSeconds * 1_000_000_000L;
            long measurementDeadline = warmupDeadline + arguments.measurementSeconds * 1_000_000_000L;
            runUntil(warmupDeadline, false, operation, dataSource, sampler, latency, counters);
            runUntil(measurementDeadline, true, operation, dataSource, sampler, latency, counters);
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
        }
    }

    private static void runUntil(long deadline,
                                 boolean measured,
                                 JdbcOperation operation,
                                 HikariDataSource dataSource,
                                 JdbcPoolSampler sampler,
                                 Recorder latency,
                                 RunCounters counters) {
        while (System.nanoTime() < deadline) {
            long started = System.nanoTime();
            try {
                long rows = operation.run();
                if (measured) {
                    counters.succeeded.increment();
                    counters.rows.add(rows);
                }
            } catch (Exception error) {
                if (measured) {
                    counters.failed.increment();
                    counters.failures.computeIfAbsent(error.getClass().getSimpleName(), ignored -> new LongAdder())
                            .increment();
                } else {
                    counters.warmupFailures.increment();
                }
            } finally {
                if (measured) {
                    counters.operations.increment();
                    record(latency, System.nanoTime() - started);
                }
                sampler.sample(dataSource);
            }
        }
    }

    private static DatabasePerformanceReport.ScenarioResult result(String name,
                                                                    int concurrency,
                                                                    RunCounters counters,
                                                                    Recorder latency,
                                                                    JdbcResourceSnapshot resources,
                                                                    long elapsedNanos) {
        long operations = counters.operations.sum();
        long succeeded = counters.succeeded.sum();
        long failed = counters.failed.sum();
        double seconds = elapsedNanos / 1_000_000_000.0;
        Histogram snapshot = latency.getIntervalHistogram();
        double p50 = millis(snapshot, 50.0);
        double p95 = millis(snapshot, 95.0);
        double p99 = millis(snapshot, 99.0);
        double max = millis(snapshot, 100.0);
        JdbcPoolState peak = resources.pool();
        return new DatabasePerformanceReport.ScenarioResult(
                name,
                failed == 0 && counters.warmupFailures.sum() == 0
                        ? DatabasePerformanceReport.Status.PASSED : DatabasePerformanceReport.Status.FAILED,
                concurrency,
                operations,
                succeeded,
                failed,
                counters.rows.sum(),
                counters.warmupFailures.sum(),
                Math.max(1L, elapsedNanos / 1_000_000L),
                operations / seconds,
                counters.rows.sum() / seconds,
                operations == 0 ? 0.0 : (double) failed / operations,
                p50,
                p95,
                p99,
                max,
                failures(counters.failures),
                peak.total(),
                peak.active(),
                peak.pending(),
                resources.averageProcessCpuPercent(),
                resources.peakHeapBytes(),
                null);
    }

    private static Map<String, Long> failures(Map<String, LongAdder> failures) {
        Map<String, Long> result = new ConcurrentHashMap<>();
        failures.forEach((type, count) -> result.put(type, count.sum()));
        return Map.copyOf(result);
    }

    private static void record(Recorder recorder, long nanos) {
        recorder.recordValue(Math.max(1L, Math.min(HIGHEST_TRACKABLE_NANOS, nanos)));
    }

    private static double millis(Histogram histogram, double percentile) {
        return histogram.getTotalCount() == 0 ? 0.0 : histogram.getValueAtPercentile(percentile) / 1_000_000.0;
    }

    @FunctionalInterface
    interface JdbcOperation {
        long run() throws Exception;
    }

    private static final class RunCounters {
        private final LongAdder operations = new LongAdder();
        private final LongAdder succeeded = new LongAdder();
        private final LongAdder failed = new LongAdder();
        private final LongAdder rows = new LongAdder();
        private final LongAdder warmupFailures = new LongAdder();
        private final Map<String, LongAdder> failures = new ConcurrentHashMap<>();
    }
}
