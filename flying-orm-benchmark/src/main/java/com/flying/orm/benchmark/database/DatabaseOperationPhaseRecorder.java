package com.flying.orm.benchmark.database;

import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 记录一次数据库操作在连接池和数据库之间分别花了多久。
 *
 * <p>这里不用 ThreadLocal。R2DBC 的获取连接、执行 SQL 和归还连接可能落在不同线程上，ThreadLocal
 * 很容易把两个并发请求的时间串在一起。每次订阅自己的样本放进 Reactor Context，连接工厂只读取当前订阅的样本。</p>
 */
final class DatabaseOperationPhaseRecorder {

    private static final Object CONTEXT_KEY = DatabaseOperationPhaseRecorder.class.getName() + ".sample";
    private static final long HIGHEST_TRACKABLE_NANOS = Duration.ofMinutes(2).toNanos();

    private final Recorder acquire = recorder();
    private final Recorder executeAndCommit = recorder();
    private final Recorder release = recorder();
    private final Recorder total = recorder();

    /**
     * 给一条完整操作建立独立样本。调用方原来就会丢弃操作结果，所以这里统一收口成 Mono&lt;Void&gt;，
     * 既能覆盖查询返回多行，也能覆盖只返回影响行数的更新。
     */
    Mono<Void> track(Publisher<?> operation) {
        Publisher<?> safeOperation = Objects.requireNonNull(operation, "database operation must not be null");
        return Mono.defer(() -> {
            Sample sample = new Sample(System.nanoTime());
            Runnable finish = () -> {
                CompletedSample completed = sample.finish(System.nanoTime());
                if (completed != null) {
                    record(completed);
                }
            };
            return Flux.from(safeOperation)
                       .then()
                       // 记录必须发生在完成信号继续向外传播之前，否则调用方紧接着取快照时可能少一条样本。
                       .doOnSuccess(ignored -> finish.run())
                       .doOnError(ignored -> finish.run())
                       .doOnCancel(finish)
                       .contextWrite(context -> context.put(CONTEXT_KEY, sample));
        });
    }

    DatabasePerformanceReport.PhaseLatency snapshot() {
        return new DatabasePerformanceReport.PhaseLatency(snapshot(acquire),
                                                          snapshot(executeAndCommit),
                                                          snapshot(release),
                                                          snapshot(total));
    }

    static Sample currentSample(ContextView context) {
        return context.getOrDefault(CONTEXT_KEY, null);
    }

    private void record(CompletedSample sample) {
        record(total, sample.totalNanos());
        record(executeAndCommit, sample.executeAndCommitNanos());
        if (sample.acquireNanos() > 0) {
            record(acquire, sample.acquireNanos());
        }
        if (sample.releaseNanos() > 0) {
            record(release, sample.releaseNanos());
        }
    }

    private static Recorder recorder() {
        return new Recorder(HIGHEST_TRACKABLE_NANOS, 3);
    }

    private static void record(Recorder recorder, long nanos) {
        recorder.recordValue(Math.max(1, Math.min(nanos, HIGHEST_TRACKABLE_NANOS)));
    }

    private static DatabasePerformanceReport.Latency snapshot(Recorder recorder) {
        Histogram histogram = recorder.getIntervalHistogram();
        long samples = histogram.getTotalCount();
        return new DatabasePerformanceReport.Latency(samples,
                                                     millis(percentile(histogram, 50)),
                                                     millis(percentile(histogram, 95)),
                                                     millis(percentile(histogram, 99)),
                                                     millis(samples == 0 ? 0 : histogram.getMaxValue()));
    }

    private static long percentile(Histogram histogram, double percentile) {
        return histogram.getTotalCount() == 0 ? 0 : histogram.getValueAtPercentile(percentile);
    }

    private static double millis(long nanos) {
        return nanos / 1_000_000.0;
    }

    /** 同一个样本只允许首次写入，防止驱动重复发终止信号时把阶段时间覆盖掉。 */
    static final class Sample {

        private final long startedAt;
        private final AtomicBoolean finished = new AtomicBoolean();
        private final AtomicLong acquireNanos = new AtomicLong();
        private final AtomicLong releaseNanos = new AtomicLong();

        private Sample(long startedAt) {
            this.startedAt = startedAt;
        }

        void acquired(long nanos) {
            acquireNanos.compareAndSet(0, Math.max(1, nanos));
        }

        void released(long nanos) {
            releaseNanos.compareAndSet(0, Math.max(1, nanos));
        }

        private CompletedSample finish(long completedAt) {
            if (!finished.compareAndSet(false, true)) {
                return null;
            }
            long total = Math.max(1, completedAt - startedAt);
            long acquired = acquireNanos.get();
            long released = releaseNanos.get();
            // 必须按每条操作先相减再进直方图。用 P99(total)-P99(acquire) 会把不同请求硬拼在一起。
            long execution = Math.max(0, total - acquired - released);
            return new CompletedSample(acquired, execution, released, total);
        }
    }

    private record CompletedSample(long acquireNanos,
                                   long executeAndCommitNanos,
                                   long releaseNanos,
                                   long totalNanos) {
    }
}
