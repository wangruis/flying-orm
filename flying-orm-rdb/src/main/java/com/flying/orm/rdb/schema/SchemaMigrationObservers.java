package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.observation.SqlExecutionStatus;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * 迁移观测的轻量组合工具。这里不引入日志或指标框架，上层可以把事件接到自己已有的监控系统里。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class SchemaMigrationObservers {

    private SchemaMigrationObservers() {
    }

    /** 返回不做任何事情的 observer，适合作为默认配置。 */
    public static SchemaMigrationObserver noop() {
        return SchemaMigrationObserver.noop();
    }

    /** observer 的普通故障只丢本次观测；异常图中的 JVM 致命错误仍原样传播。 */
    public static SchemaMigrationObserver safe(SchemaMigrationObserver observer) {
        SchemaMigrationObserver delegate = Objects.requireNonNull(observer,
                                                                   "schema migration observer must not be null");
        return observation -> {
            try {
                delegate.onMigration(observation);
            } catch (RuntimeException failure) {
                rethrowVirtualMachineError(failure);
                // 监控后端不可用时，数据库已经产生的结果仍应原样返回给调用方。
            }
        };
    }

    /** 普通扩展故障由调用点隔离；异常图中的 JVM 致命错误必须恢复为原对象。 */
    static void rethrowVirtualMachineError(Throwable failure) {
        VirtualMachineError fatal = findVirtualMachineError(failure);
        if (fatal != null) {
            throw fatal;
        }
    }

    /** 按对象身份非递归遍历 cause/suppressed；循环图不会重复访问，深层致命错误也不会被截断。 */
    static VirtualMachineError findVirtualMachineError(Throwable failure) {
        if (failure == null) {
            return null;
        }
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.add(failure);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (current instanceof VirtualMachineError fatal) {
                return fatal;
            }
            if (current.getCause() != null) {
                pending.addLast(current.getCause());
            }
            for (Throwable suppressed : current.getSuppressed()) {
                pending.addLast(suppressed);
            }
        }
        return null;
    }

    /** 按声明顺序通知多个 observer，其中一个普通失败不会挡住后面的 observer。 */
    public static SchemaMigrationObserver composite(SchemaMigrationObserver... observers) {
        Objects.requireNonNull(observers, "schema migration observers must not be null");
        List<SchemaMigrationObserver> delegates = new ArrayList<>(observers.length);
        for (SchemaMigrationObserver observer : observers) {
            delegates.add(safe(Objects.requireNonNull(observer,
                                                      "schema migration observer must not be null")));
        }
        if (delegates.isEmpty()) {
            return noop();
        }
        return observation -> delegates.forEach(observer -> observer.onMigration(observation));
    }

    /** 只把满足条件的迁移事件交给下游 observer。 */
    public static SchemaMigrationObserver when(Predicate<SchemaMigrationObservation> predicate,
                                                SchemaMigrationObserver observer) {
        Predicate<SchemaMigrationObservation> condition = Objects.requireNonNull(
                predicate, "schema migration observation predicate must not be null");
        SchemaMigrationObserver delegate = safe(observer);
        return safe(observation -> {
            if (condition.test(observation)) {
                delegate.onMigration(observation);
            }
        });
    }

    /** 只接收达到指定耗时的迁移事件，便于单独记录慢 DDL。 */
    public static SchemaMigrationObserver slow(Duration threshold, SchemaMigrationObserver observer) {
        Duration safeThreshold = Objects.requireNonNull(threshold,
                                                        "slow schema migration threshold must not be null");
        if (safeThreshold.isNegative()) {
            throw new IllegalArgumentException("slow schema migration threshold must not be negative");
        }
        return when(observation -> Duration.ofNanos(observation.durationNanos()).compareTo(safeThreshold) >= 0,
                    observer);
    }

    /** 只接收执行失败的迁移，不包含主动取消。 */
    public static SchemaMigrationObserver errors(SchemaMigrationObserver observer) {
        return when(observation -> observation.status() == SqlExecutionStatus.ERROR, observer);
    }

    /** 每 N 次迁移保留一次事件，计数器可由多个并发订阅安全共享。 */
    public static SchemaMigrationObserver sampleEvery(long interval, SchemaMigrationObserver observer) {
        if (interval <= 0) {
            throw new IllegalArgumentException("schema migration sample interval must be positive");
        }
        AtomicLong sequence = new AtomicLong();
        return when(ignored -> sequence.incrementAndGet() % interval == 0, observer);
    }

}
