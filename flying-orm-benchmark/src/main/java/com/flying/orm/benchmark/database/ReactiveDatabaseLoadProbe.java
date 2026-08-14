package com.flying.orm.benchmark.database;

import com.flying.orm.rdb.exception.RdbException;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.reactivestreams.Publisher;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Supplier;

/**
 * 用固定数量的响应式 worker 持续执行数据库操作，并用固定内存统计吞吐和延迟分布。
 *
 * <p>每个 worker 一次只订阅一个操作，完成后才发起下一次，所以最大在途操作不会超过 concurrency。这里不创建
 * 每请求线程，也不把所有延迟保存进 List；HDR Histogram 的 Recorder 可以让多个完成线程安全地记录延迟。</p>
 *
 * @author wangr
 * @date 2026-08-02
 * @version v1.0
 */
final class ReactiveDatabaseLoadProbe {

    private static final long HIGHEST_TRACKABLE_NANOS = Duration.ofMinutes(2).toNanos();

    private ReactiveDatabaseLoadProbe() {
    }

    /**
     * 先预热再正式测量。预热错误单独保留，不能因为正式阶段碰巧成功就把环境问题藏起来。
     *
     * @param plan      时长、并发和单次操作代表的行数
     * @param operation 每次订阅创建一个新的数据库操作
     * @return 正式测量结果以及预热失败数
     */
    static Mono<Result> run(Plan plan, Supplier<? extends Publisher<?>> operation) {
        return run(plan, operation, null);
    }

    /**
     * 阶段 recorder 只包正式测量，不包预热。这样连接池和 JIT 的预热抖动不会混进阶段分位数。
     */
    static Mono<Result> run(Plan plan,
                            Supplier<? extends Publisher<?>> operation,
                            DatabaseOperationPhaseRecorder phaseRecorder) {
        Plan safePlan = Objects.requireNonNull(plan, "database load plan must not be null");
        Supplier<? extends Publisher<?>> safeOperation = Objects.requireNonNull(
                operation,
                "database load operation must not be null");

        return runPhase(safePlan.warmup(), safePlan, safeOperation, false, null)
                .flatMap(warmup -> runPhase(safePlan.measurement(), safePlan, safeOperation, true, phaseRecorder)
                        .map(measurement -> measurement.toResult(safePlan, warmup.failed())));
    }

    private static Mono<Phase> runPhase(Duration duration,
                                        Plan plan,
                                        Supplier<? extends Publisher<?>> operation,
                                        boolean recordLatency,
                                        DatabaseOperationPhaseRecorder phaseRecorder) {
        if (duration.isZero()) {
            return Mono.just(new Phase(Duration.ZERO, new Counters(recordLatency)));
        }

        return Mono.defer(() -> {
            long startedAt = System.nanoTime();
            long deadline = startedAt + duration.toNanos();
            Counters counters = new Counters(recordLatency);
            return Flux.range(0, plan.concurrency())
                       .flatMap(ignored -> worker(deadline, plan.operationTimeout(), operation, counters, phaseRecorder),
                                plan.concurrency())
                       .then(Mono.fromSupplier(() -> new Phase(Duration.ofNanos(System.nanoTime() - startedAt),
                                                               counters)))
                       // 截止时间前发出的操作最多再等一个 operationTimeout，额外余量留给取消和连接归还。
                       .timeout(duration.plus(plan.operationTimeout().multipliedBy(2)));
        });
    }

    private static Mono<Void> worker(long deadline,
                                     Duration operationTimeout,
                                     Supplier<? extends Publisher<?>> operation,
                                     Counters counters,
                                     DatabaseOperationPhaseRecorder phaseRecorder) {
        return Mono.defer(() -> executeOnce(deadline, operationTimeout, operation, counters, phaseRecorder))
                   .repeat(() -> System.nanoTime() < deadline)
                   .then();
    }

    private static Mono<Void> executeOnce(long deadline,
                                          Duration operationTimeout,
                                          Supplier<? extends Publisher<?>> operation,
                                          Counters counters,
                                          DatabaseOperationPhaseRecorder phaseRecorder) {
        if (System.nanoTime() >= deadline) {
            return Mono.empty();
        }

        long startedAt = System.nanoTime();
        Publisher<?> publisher;
        try {
            publisher = Objects.requireNonNull(operation.get(), "database load operation returned null");
        } catch (Throwable error) {
            publisher = Mono.error(ReactivePerformanceReportSupport.errorSignal(error));
        }

        Publisher<?> measured = phaseRecorder == null ? publisher : phaseRecorder.track(publisher);
        return Flux.from(measured)
                   .then()
                   .timeout(operationTimeout)
                   .doOnSuccess(ignored -> counters.success(System.nanoTime() - startedAt))
                   .onErrorResume(error -> {
                       ReactivePerformanceReportSupport.rethrowFatal(error);
                       counters.failure(error, System.nanoTime() - startedAt);
                       return Mono.empty();
                   });
    }

    /**
     * @param warmup           预热时间，0 表示跳过
     * @param measurement      正式测量时间，必须大于 0
     * @param concurrency      固定 worker 数量
     * @param rowsPerOperation 一次成功操作代表多少业务行
     * @param operationTimeout 单次操作最多允许运行多久
     */
    record Plan(Duration warmup,
                Duration measurement,
                int concurrency,
                long rowsPerOperation,
                Duration operationTimeout) {

        Plan {
            warmup = Objects.requireNonNull(warmup, "database load warmup must not be null");
            measurement = Objects.requireNonNull(measurement, "database load measurement must not be null");
            operationTimeout = Objects.requireNonNull(operationTimeout,
                                                      "database load operation timeout must not be null");
            if (warmup.isNegative()) {
                throw new IllegalArgumentException("database load warmup must not be negative");
            }
            if (measurement.isZero() || measurement.isNegative()) {
                throw new IllegalArgumentException("database load measurement must be greater than zero");
            }
            if (concurrency <= 0) {
                throw new IllegalArgumentException("database load concurrency must be greater than zero");
            }
            if (rowsPerOperation <= 0) {
                throw new IllegalArgumentException("database load rows per operation must be greater than zero");
            }
            if (operationTimeout.isZero() || operationTimeout.isNegative()) {
                throw new IllegalArgumentException("database load operation timeout must be greater than zero");
            }
            if (operationTimeout.toNanos() > HIGHEST_TRACKABLE_NANOS) {
                throw new IllegalArgumentException("database load operation timeout must not exceed two minutes");
            }
        }
    }

    /** 延迟保持纳秒，写报告时再转成毫秒，避免过早转成小数损失短查询精度。 */
    record Result(long operations,
                  long succeeded,
                  long failed,
                  long rows,
                  long warmupFailures,
                  Duration elapsed,
                  double operationsPerSecond,
                  double rowsPerSecond,
                  long p50Nanos,
                  long p95Nanos,
                  long p99Nanos,
                  long maxNanos,
                  Map<String, Long> failuresByType) {

        Result {
            elapsed = Objects.requireNonNull(elapsed, "database load elapsed must not be null");
            failuresByType = Map.copyOf(Objects.requireNonNull(failuresByType,
                                                               "database load failures must not be null"));
            if (operations < 0 || succeeded < 0 || failed < 0 || rows < 0 || warmupFailures < 0
                    || p50Nanos < 0 || p95Nanos < 0 || p99Nanos < 0 || maxNanos < 0) {
                throw new IllegalArgumentException("database load counters and latencies must not be negative");
            }
            if (operations != succeeded + failed) {
                throw new IllegalArgumentException("database load operations must equal succeeded plus failed");
            }
            if (!Double.isFinite(operationsPerSecond) || operationsPerSecond < 0
                    || !Double.isFinite(rowsPerSecond) || rowsPerSecond < 0) {
                throw new IllegalArgumentException("database load throughput must be finite and non-negative");
            }
        }

        double errorRate() {
            return operations == 0 ? 0.0 : (double) failed / operations;
        }
    }

    private record Phase(Duration elapsed, Counters counters) {

        private long failed() {
            return counters.failed.sum();
        }

        private Result toResult(Plan plan, long warmupFailures) {
            long succeeded = counters.succeeded.sum();
            long failed = counters.failed.sum();
            long operations = succeeded + failed;
            long rows = multiplyWithoutOverflow(succeeded, plan.rowsPerOperation());
            double seconds = elapsed.toNanos() / 1_000_000_000.0;
            Histogram histogram = counters.histogram();
            return new Result(operations,
                              succeeded,
                              failed,
                              rows,
                              warmupFailures,
                              elapsed,
                              seconds == 0.0 ? 0.0 : succeeded / seconds,
                              seconds == 0.0 ? 0.0 : rows / seconds,
                              percentile(histogram, 50.0),
                              percentile(histogram, 95.0),
                              percentile(histogram, 99.0),
                              histogram.getTotalCount() == 0 ? 0 : histogram.getMaxValue(),
                              counters.failureSnapshot());
        }
    }

    private static final class Counters {

        private final LongAdder succeeded = new LongAdder();
        private final LongAdder failed = new LongAdder();
        private final Map<String, LongAdder> failures = new ConcurrentHashMap<>();
        private final Recorder latencyRecorder;

        private Counters(boolean recordLatency) {
            latencyRecorder = recordLatency ? new Recorder(HIGHEST_TRACKABLE_NANOS, 3) : null;
        }

        private void success(long elapsedNanos) {
            succeeded.increment();
            record(elapsedNanos);
        }

        private void failure(Throwable error, long elapsedNanos) {
            failed.increment();
            failures.computeIfAbsent(failureKey(error), ignored -> new LongAdder()).increment();
            record(elapsedNanos);
        }

        private void record(long elapsedNanos) {
            if (latencyRecorder != null) {
                latencyRecorder.recordValue(Math.max(1, Math.min(elapsedNanos, HIGHEST_TRACKABLE_NANOS)));
            }
        }

        private Histogram histogram() {
            return latencyRecorder == null ? new Histogram(HIGHEST_TRACKABLE_NANOS, 3)
                    : latencyRecorder.getIntervalHistogram();
        }

        private Map<String, Long> failureSnapshot() {
            Map<String, Long> snapshot = new TreeMap<>();
            failures.forEach((type, count) -> snapshot.put(type, count.sum()));
            return snapshot;
        }
    }

    private static String failureKey(Throwable error) {
        Throwable unwrapped = Exceptions.unwrap(Objects.requireNonNull(error, "database load failure must not be null"));
        if (unwrapped instanceof RdbException databaseError) {
            return "RDB_" + databaseError.kind().name();
        }
        String simpleName = unwrapped.getClass().getSimpleName();
        return simpleName.isBlank() ? unwrapped.getClass().getName() : simpleName;
    }

    private static long percentile(Histogram histogram, double percentile) {
        return histogram.getTotalCount() == 0 ? 0 : histogram.getValueAtPercentile(percentile);
    }

    private static long multiplyWithoutOverflow(long left, long right) {
        try {
            return Math.multiplyExact(left, right);
        } catch (ArithmeticException error) {
            throw new IllegalStateException("database load row count overflow", error);
        }
    }
}
