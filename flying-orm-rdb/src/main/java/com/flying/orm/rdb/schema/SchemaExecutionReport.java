package com.flying.orm.rdb.schema;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * 一次审核结构计划执行的不可变证据。
 *
 * <p>SQL 已发送和数据库返回的更新数只记录为步骤事实；只有执行后重新读取结构并通过原兼容模式
 * 验证，整体状态才可以是 {@link SchemaExecutionStatus#SUCCESS}；外部事务尚未结束时，即使回读一致也只能
 * 报告 {@link SchemaExecutionStatus#EXTERNAL_TRANSACTION_PENDING}。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class SchemaExecutionReport {

    private final String planFingerprint;
    private final SchemaExecutionStatus status;
    private final List<StepResult> steps;
    private final String preExecutionActualFingerprint;
    private final String postExecutionActualFingerprint;
    private final SchemaCompatibilityReport verification;

    private SchemaExecutionReport(String planFingerprint,
                                  SchemaExecutionStatus status,
                                  List<StepResult> steps,
                                  String preExecutionActualFingerprint,
                                  String postExecutionActualFingerprint,
                                  SchemaCompatibilityReport verification) {
        this.planFingerprint = requireText(planFingerprint, "schema plan fingerprint");
        this.status = Objects.requireNonNull(status, "schema execution status must not be null");
        this.steps = List.copyOf(Objects.requireNonNull(steps, "schema step results must not be null"));
        if (this.steps.stream().anyMatch(Objects::isNull)) {
            throw new IllegalArgumentException("schema step results must not contain null");
        }
        this.preExecutionActualFingerprint = optionalText(
                preExecutionActualFingerprint, "pre-execution schema fingerprint");
        this.postExecutionActualFingerprint = optionalText(
                postExecutionActualFingerprint, "post-execution schema fingerprint");
        this.verification = verification;
        validateCompletedExecution();
    }

    public static SchemaExecutionReport of(String planFingerprint,
                                           SchemaExecutionStatus status,
                                           List<StepResult> steps,
                                           String preExecutionActualFingerprint,
                                           String postExecutionActualFingerprint,
                                           SchemaCompatibilityReport verification) {
        return new SchemaExecutionReport(planFingerprint, status, steps,
                                         preExecutionActualFingerprint,
                                         postExecutionActualFingerprint, verification);
    }

    public String planFingerprint() {
        return planFingerprint;
    }

    public SchemaExecutionStatus status() {
        return status;
    }

    public List<StepResult> steps() {
        return steps;
    }

    public Optional<String> preExecutionActualFingerprint() {
        return Optional.ofNullable(preExecutionActualFingerprint);
    }

    public Optional<String> postExecutionActualFingerprint() {
        return Optional.ofNullable(postExecutionActualFingerprint);
    }

    public Optional<SchemaCompatibilityReport> verification() {
        return Optional.ofNullable(verification);
    }

    public boolean successful() {
        return status == SchemaExecutionStatus.SUCCESS;
    }

    private void validateCompletedExecution() {
        if (status != SchemaExecutionStatus.SUCCESS
                && status != SchemaExecutionStatus.EXTERNAL_TRANSACTION_PENDING) {
            return;
        }
        if (verification == null || !verification.compatible()
                || postExecutionActualFingerprint == null) {
            throw new IllegalArgumentException(
                    "completed schema execution must include compatible post-execution verification");
        }
        if (steps.stream().anyMatch(step -> step.status() != SchemaExecutionStatus.SUCCESS)) {
            throw new IllegalArgumentException(
                    "completed schema execution cannot contain an unsuccessful step");
        }
    }

    private static String requireText(String value, String name) {
        String text = Objects.requireNonNull(value, name + " must not be null").trim();
        if (text.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return text;
    }

    private static String optionalText(String value, String name) {
        return value == null ? null : requireText(value, name);
    }

    /** 一条审核步骤的执行事实；失败摘要必须由执行器先完成脱敏。 */
    public static final class StepResult {

        private final int order;
        private final SchemaExecutionStatus status;
        private final boolean sqlSent;
        private final Long rowsUpdated;
        private final String failureSummary;

        private StepResult(int order,
                           SchemaExecutionStatus status,
                           boolean sqlSent,
                           Long rowsUpdated,
                           String failureSummary) {
            if (order < 0) {
                throw new IllegalArgumentException("schema step result order must not be negative");
            }
            if (rowsUpdated != null && rowsUpdated < 0L) {
                throw new IllegalArgumentException("schema step rows updated must not be negative");
            }
            this.order = order;
            this.status = Objects.requireNonNull(status, "schema step status must not be null");
            this.sqlSent = sqlSent;
            this.rowsUpdated = rowsUpdated;
            this.failureSummary = optionalText(failureSummary, "schema step failure summary");
            if (status == SchemaExecutionStatus.SUCCESS && this.failureSummary != null) {
                throw new IllegalArgumentException("successful schema step cannot contain a failure summary");
            }
        }

        public static StepResult success(int order, long rowsUpdated) {
            return new StepResult(order, SchemaExecutionStatus.SUCCESS, true, rowsUpdated, null);
        }

        public static StepResult notExecuted(int order, SchemaExecutionStatus status) {
            if (status == SchemaExecutionStatus.SUCCESS) {
                throw new IllegalArgumentException("not-executed schema step cannot be successful");
            }
            return new StepResult(order, status, false, null, null);
        }

        public static StepResult failure(int order,
                                         SchemaExecutionStatus status,
                                         boolean sqlSent,
                                         String redactedSummary) {
            if (status == SchemaExecutionStatus.SUCCESS) {
                throw new IllegalArgumentException("failed schema step cannot be successful");
            }
            return new StepResult(order, status, sqlSent, null,
                                  requireText(redactedSummary, "schema step failure summary"));
        }

        public int order() {
            return order;
        }

        public SchemaExecutionStatus status() {
            return status;
        }

        public boolean sqlSent() {
            return sqlSent;
        }

        public OptionalLong rowsUpdated() {
            return rowsUpdated == null ? OptionalLong.empty() : OptionalLong.of(rowsUpdated);
        }

        public Optional<String> failureSummary() {
            return Optional.ofNullable(failureSummary);
        }
    }
}
