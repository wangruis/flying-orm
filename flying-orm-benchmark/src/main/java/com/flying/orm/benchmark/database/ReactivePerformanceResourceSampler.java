package com.flying.orm.benchmark.database;

import io.r2dbc.pool.ConnectionPool;
import io.r2dbc.pool.PoolMetrics;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/** 每 10 毫秒采样连接池；CPU 和堆峰值使用 JVM 指标，不进入每次数据库操作的热路径。 */
final class ReactivePerformanceResourceSampler {

    private final ConnectionPool pool;
    private final AtomicInteger peakAllocated = new AtomicInteger();
    private final AtomicInteger peakAcquired = new AtomicInteger();
    private final AtomicInteger peakPending = new AtomicInteger();
    private final long startedAt = System.nanoTime();
    private final long cpuStartedAt = processCpuTime();
    private final Disposable sampler;
    private Snapshot snapshot;

    private ReactivePerformanceResourceSampler(ConnectionPool pool) {
        this.pool = Objects.requireNonNull(pool, "performance connection pool must not be null");
        resetHeapPeaks();
        sampler = Flux.interval(Duration.ZERO, Duration.ofMillis(10)).subscribe(ignored -> samplePool());
    }

    static ReactivePerformanceResourceSampler start(ConnectionPool pool) {
        return new ReactivePerformanceResourceSampler(pool);
    }

    void stop() {
        if (snapshot != null) {
            return;
        }
        samplePool();
        sampler.dispose();
        long elapsed = Math.max(1, System.nanoTime() - startedAt);
        long cpuElapsed = Math.max(0, processCpuTime() - cpuStartedAt);
        double cpuPercent = cpuStartedAt < 0 ? 0.0
                : Math.min(100.0, (double) cpuElapsed / elapsed
                        / Runtime.getRuntime().availableProcessors() * 100.0);
        snapshot = new Snapshot(peakAllocated.get(), peakAcquired.get(), peakPending.get(),
                                cpuPercent, peakHeapBytes());
    }

    Snapshot snapshot() {
        return Objects.requireNonNull(snapshot, "performance resource sampler was not stopped");
    }

    private void samplePool() {
        PoolMetrics current = pool.getMetrics().orElseThrow(
                () -> new IllegalStateException("R2DBC pool metrics are unavailable"));
        peakAllocated.accumulateAndGet(current.allocatedSize(), Math::max);
        peakAcquired.accumulateAndGet(current.acquiredSize(), Math::max);
        peakPending.accumulateAndGet(current.pendingAcquireSize(), Math::max);
    }

    private static long processCpuTime() {
        java.lang.management.OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
        return bean instanceof com.sun.management.OperatingSystemMXBean operatingSystem
                ? operatingSystem.getProcessCpuTime() : -1;
    }

    private static void resetHeapPeaks() {
        ManagementFactory.getMemoryPoolMXBeans().stream()
                         .filter(pool -> pool.getType() == MemoryType.HEAP)
                         .forEach(MemoryPoolMXBean::resetPeakUsage);
    }

    private static long peakHeapBytes() {
        return ManagementFactory.getMemoryPoolMXBeans().stream()
                                .filter(pool -> pool.getType() == MemoryType.HEAP)
                                .mapToLong(pool -> Math.max(0, pool.getPeakUsage().getUsed()))
                                .sum();
    }

    record Snapshot(int peakAllocatedConnections,
                    int peakAcquiredConnections,
                    int peakPendingAcquires,
                    double averageProcessCpuPercent,
                    long peakHeapBytes) {
    }
}
