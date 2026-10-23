package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.execution.SqlExecutionPhase;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.observation.SqlFailureCategory;

import java.util.Objects;

/**
 * 一次结构迁移的汇总事件。事件不保存参数值，只记录步骤进度和稳定错误分类，
 * 上层可以直接拿来做指标、审计关联和故障定位。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record SchemaMigrationObservation(SqlExecutionStatus status,
                                         int plannedSteps,
                                         int completedSteps,
                                         long rowsUpdated,
                                         long durationNanos,
                                         SqlFailureCategory failureCategory,
                                         SqlExecutionPhase failedPhase,
                                         Integer failedStepIndex,
                                         Throwable error) {
    public SchemaMigrationObservation {
        status = Objects.requireNonNull(status, "migration status must not be null");
        failureCategory = Objects.requireNonNull(failureCategory, "migration failure category must not be null");
        if (plannedSteps < 0 || completedSteps < 0 || completedSteps > plannedSteps
                || rowsUpdated < 0 || durationNanos < 0) {
            throw new IllegalArgumentException("migration observation counts and duration are invalid");
        }
        if ((failedPhase == null) != (failedStepIndex == null)) {
            throw new IllegalArgumentException("failed phase and step index must be present together");
        }
        if (failedStepIndex != null && failedStepIndex < 0) {
            throw new IllegalArgumentException("failed migration step index must not be negative");
        }
        if (status == SqlExecutionStatus.SUCCESS
                && (failureCategory != SqlFailureCategory.NONE || error != null || failedPhase != null)) {
            throw new IllegalArgumentException("successful migration cannot contain failure details");
        }
        if (status == SqlExecutionStatus.ERROR
                && (failureCategory == SqlFailureCategory.NONE || error == null)) {
            throw new IllegalArgumentException("failed migration must contain failure details");
        }
    }

    /**
     * 把底层异常分类、失败阶段和 DDL 前置条件错误合并成稳定错误码，避免上层解析异常文字。
     */
    public SchemaMigrationFailureCode failureCode() {
        return SchemaMigrationFailureCode.classify(error, failureCategory, failedPhase);
    }

}
