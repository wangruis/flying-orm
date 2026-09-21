package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.execution.SqlExecutionPhase;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.observation.SqlFailureCategory;

import java.util.Objects;

/**
 * 一次审核迁移的汇总事件。事件不保存参数值，只记录计划指纹、风险、步骤进度和稳定错误分类，
 * 上层可以直接拿来做指标、审计关联和故障定位。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record SchemaMigrationObservation(String planFingerprint,
                                         SchemaMigrationRiskLevel riskLevel,
                                         SqlExecutionStatus status,
                                         int plannedSteps,
                                         int completedSteps,
                                         long rowsUpdated,
                                         long durationNanos,
                                         SqlFailureCategory failureCategory,
                                         SqlExecutionPhase failedPhase,
                                         Integer failedStepIndex,
                                         Throwable error) {
    public SchemaMigrationObservation {
        planFingerprint = requireText(planFingerprint);
        riskLevel = Objects.requireNonNull(riskLevel, "migration risk level must not be null");
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
     * 把底层异常分类、失败阶段和迁移安全拒绝合并成稳定错误码。上层处理业务时用它，避免解析异常文字。
     */
    public SchemaMigrationFailureCode failureCode() {
        return SchemaMigrationFailureCode.classify(error, failureCategory, failedPhase);
    }

    private static String requireText(String value) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("migration plan fingerprint must not be blank");
        }
        return value.trim();
    }
}
