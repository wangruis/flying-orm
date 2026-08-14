package com.flying.orm.benchmark.database;

import com.zaxxer.hikari.HikariDataSource;
import com.zaxxer.hikari.HikariPoolMXBean;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.util.concurrent.atomic.AtomicInteger;

/** 轻量记录 Hikari 池峰值，采样发生在每次业务操作之后，不另起监控线程。 */
final class JdbcPoolSampler {

    private final AtomicInteger peakTotal = new AtomicInteger();
    private final AtomicInteger peakActive = new AtomicInteger();
    private final AtomicInteger peakIdle = new AtomicInteger();
    private final AtomicInteger peakPending = new AtomicInteger();
    private final long startedAt = System.nanoTime();
    private final long cpuStartedAt = processCpuTime();

    JdbcPoolSampler() {
        ManagementFactory.getMemoryPoolMXBeans().stream()
                         .filter(pool -> pool.getType() == MemoryType.HEAP)
                         .forEach(MemoryPoolMXBean::resetPeakUsage);
    }

    void sample(HikariDataSource dataSource) {
        HikariPoolMXBean pool = dataSource.getHikariPoolMXBean();
        if (pool == null) {
            return;
        }
        update(peakTotal, pool.getTotalConnections());
        update(peakActive, pool.getActiveConnections());
        update(peakIdle, pool.getIdleConnections());
        update(peakPending, pool.getThreadsAwaitingConnection());
    }

    JdbcResourceSnapshot snapshot() {
        long elapsed = Math.max(1L, System.nanoTime() - startedAt);
        long cpuElapsed = Math.max(0L, processCpuTime() - cpuStartedAt);
        double cpuPercent = cpuStartedAt < 0 ? 0.0
                : Math.min(100.0, (double) cpuElapsed / elapsed
                        / Runtime.getRuntime().availableProcessors() * 100.0);
        long peakHeap = ManagementFactory.getMemoryPoolMXBeans().stream()
                                         .filter(pool -> pool.getType() == MemoryType.HEAP)
                                         .mapToLong(pool -> Math.max(0L, pool.getPeakUsage().getUsed()))
                                         .sum();
        return new JdbcResourceSnapshot(
                new JdbcPoolState(peakTotal.get(), peakActive.get(), peakIdle.get(), peakPending.get()),
                cpuPercent, peakHeap);
    }

    static JdbcPoolState current(HikariDataSource dataSource) {
        HikariPoolMXBean pool = dataSource.getHikariPoolMXBean();
        return pool == null ? new JdbcPoolState(0, 0, 0, 0)
                : new JdbcPoolState(pool.getTotalConnections(), pool.getActiveConnections(),
                                    pool.getIdleConnections(), pool.getThreadsAwaitingConnection());
    }

    private static void update(AtomicInteger value, int candidate) {
        value.accumulateAndGet(candidate, Math::max);
    }

    private static long processCpuTime() {
        java.lang.management.OperatingSystemMXBean bean = ManagementFactory.getOperatingSystemMXBean();
        return bean instanceof com.sun.management.OperatingSystemMXBean operatingSystem
                ? operatingSystem.getProcessCpuTime() : -1L;
    }
}

record JdbcResourceSnapshot(JdbcPoolState pool, double averageProcessCpuPercent, long peakHeapBytes) {
}

record JdbcPoolState(int total, int active, int idle, int pending) {
    JdbcPoolState {
        if (total < 0 || active < 0 || idle < 0 || pending < 0) {
            throw new IllegalArgumentException("JDBC pool counters must not be negative");
        }
    }
}
