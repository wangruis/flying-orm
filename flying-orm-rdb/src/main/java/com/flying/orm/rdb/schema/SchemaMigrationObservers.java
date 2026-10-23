package com.flying.orm.rdb.schema;

import com.flying.orm.core.internal.error.ThrowableGraph;
import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
import com.flying.orm.rdb.execution.SqlExecutionStepResult;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.observation.SqlFailureCategory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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
        return Noop.INSTANCE;
    }

    private enum Noop implements SchemaMigrationObserver {
        INSTANCE;
        @Override
        public void onMigration(SchemaMigrationObservation observation) {
        }
    }

    /** observer 的普通故障只丢本次观测；直接抛出的 {@link Error} 仍原样传播。 */
    public static SchemaMigrationObserver safe(SchemaMigrationObserver observer) {
        SchemaMigrationObserver delegate = Objects.requireNonNull(observer,
                                                                   "schema migration observer must not be null");
        return delegate == Noop.INSTANCE || delegate instanceof SafeSchemaMigrationObserver
                ? delegate : new SafeSchemaMigrationObserver(delegate);
    }

    /** 两种执行方式共享同一组完成步骤、失败阶段和计数规则。 */
    static void observe(SchemaMigrationObserver observer, SchemaMigrationPlan plan,
                        SchemaMigrationResult result, List<SqlExecutionStepResult> completedSteps,
                        long startedAt, SqlExecutionStatus status, Throwable error) {
        if (observer == Noop.INSTANCE) {
            return;
        }
        SqlExecutionSequenceException sequenceFailure =
                ThrowableGraph.findCause(error, SqlExecutionSequenceException.class);
        List<SqlExecutionStepResult> progress;
        if (result != null) {
            progress = result.steps();
        } else if (sequenceFailure != null) {
            progress = sequenceFailure.completedWorkSteps();
        } else {
            synchronized (completedSteps) {
                progress = List.copyOf(completedSteps);
            }
        }
        long rows;
        try {
            rows = result == null
                    ? progress.stream().mapToLong(SqlExecutionStepResult::rowsUpdated)
                            .reduce(0L, SchemaMigrationResult::addRows)
                    : result.rowsUpdated();
        } catch (RdbException overflow) {
            // 计数不可精确表示时不伪造观测，也不覆盖原始执行错误。
            return;
        }
        SqlFailureCategory category = status == SqlExecutionStatus.SUCCESS
                ? SqlFailureCategory.NONE : status == SqlExecutionStatus.CANCELLED
                ? SqlFailureCategory.CANCELLED : SqlFailureCategory.classify(error);
        observer.onMigration(new SchemaMigrationObservation(
                status, plan.executableSqlCount(), progress.size(), rows,
                System.nanoTime() - startedAt, category,
                sequenceFailure == null ? null : sequenceFailure.phase(),
                sequenceFailure == null ? null : sequenceFailure.stepIndex(), error));
    }

    private record SafeSchemaMigrationObserver(SchemaMigrationObserver delegate)
            implements SchemaMigrationObserver {
        @Override
        public void onMigration(SchemaMigrationObservation observation) {
            try {
                delegate.onMigration(observation);
            } catch (RuntimeException failure) {
                // 监控后端不可用时，数据库已经产生的结果仍应原样返回给调用方。
            }
        }
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
        SchemaMigrationObserver delegate = Objects.requireNonNull(
                observer, "schema migration observer must not be null");
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
