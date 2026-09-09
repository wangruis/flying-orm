package com.flying.orm.rdb.schema;

import com.flying.orm.core.internal.error.ThrowableGraph;
import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
import com.flying.orm.rdb.execution.SqlExecutionStepResult;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.observation.SqlFailureCategory;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/** 响应式 Schema 迁移的单次终态观测与精确计数。 */
final class ReactiveSchemaMigrationObservation {

    private ReactiveSchemaMigrationObservation() {
    }

    static void observe(SchemaMigrationObserver observer,
                        AtomicBoolean observed,
                        ReviewedSchemaMigrationPlan plan,
                        String planFingerprint,
                        SchemaMigrationResult result,
                        List<SqlExecutionStepResult> completedSteps,
                        long startedAt,
                        SqlExecutionStatus status,
                        Throwable error) {
        if (!observed.compareAndSet(false, true)) {
            return;
        }
        if (result == null && isAffectedRowsOverflow(error)) {
            // 汇总失败后无法在 long 中精确表达已执行行数；不把它伪装为零行失败观测。
            return;
        }
        SqlExecutionSequenceException sequenceFailure = findSequenceFailure(error);
        List<SqlExecutionStepResult> progress = List.of();
        if (result == null) {
            if (sequenceFailure != null) {
                progress = sequenceFailure.completedWorkSteps();
            } else {
                synchronized (completedSteps) {
                    progress = List.copyOf(completedSteps);
                }
            }
        }
        int completed = result == null
                ? progress.size()
                : result.steps().isEmpty() ? plan.migration().executableSqlCount() : result.steps().size();
        long rows;
        try {
            rows = result == null
                    ? progress.stream().mapToLong(SqlExecutionStepResult::rowsUpdated)
                              .reduce(0L, ReactiveSchemaMigrationObservation::addExact)
                    : result.rowsUpdated();
        } catch (RdbException overflow) {
            // 已经处于失败路径时不伪造截断后的观测值，也不能让旁路观测覆盖原始数据库异常。
            return;
        }
        SqlFailureCategory category = status == SqlExecutionStatus.SUCCESS
                ? SqlFailureCategory.NONE
                : status == SqlExecutionStatus.CANCELLED
                        ? SqlFailureCategory.CANCELLED : SqlFailureCategory.classify(error);
        observer.onMigration(new SchemaMigrationObservation(
                planFingerprint, plan.riskLevel(), status, plan.migration().executableSqlCount(), completed,
                rows, System.nanoTime() - startedAt, category,
                sequenceFailure == null ? null : sequenceFailure.phase(),
                sequenceFailure == null ? null : sequenceFailure.stepIndex(), error));
    }

    static SqlExecutionSequenceException findSequenceFailure(Throwable error) {
        return ThrowableGraph.findCause(error, SqlExecutionSequenceException.class);
    }

    static long addExact(long left, long right) {
        try {
            return Math.addExact(left, right);
        } catch (ArithmeticException overflow) {
            throw new RdbException(RdbErrorKind.UNKNOWN,
                                   "database execution count exceeds supported range",
                                   null,
                                   null,
                                   overflow);
        }
    }

    private static boolean isAffectedRowsOverflow(Throwable error) {
        return error instanceof RdbException rdb
                && "database execution count exceeds supported range".equals(rdb.getMessage())
                && rdb.getCause() instanceof ArithmeticException;
    }
}
