package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.dialect.RdbDialectResolver;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionStepResult;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * 执行已经冻结精确 SQL 的审核计划，并在 SQL 前后读取同一结构事实边界。
 *
 * <p>本执行器不渲染、不重排，也不把影响行数当成结构收敛。执行前指纹漂移以零 SQL 返回，执行后
 * 必须先失效缓存、再按原兼容模式重新比较；异常摘要只保留类型，不携带 SQL、参数或驱动消息。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class VerifiedSchemaPlanExecutor {

    private VerifiedSchemaPlanExecutor() {
    }

    /** 同步执行入口，并在读取快照或发送 SQL 前核对当前 reader coverage。 */
    public static SchemaExecutionReport executeJdbc(ReviewedSchemaPlan plan,
                                                     SyncSqlExecutor executor,
                                                     Supplier<SchemaSnapshot> snapshotReader,
                                                     Supplier<SchemaSnapshotCoverage> coverageReader,
                                                     Runnable metadataInvalidator,
                                                     SqlExecutionOptions options) {
        return executeJdbc(plan, executor, snapshotReader, coverageReader, metadataInvalidator,
                           new SchemaMigrationExecutionOptions(options, null));
    }

    /** 使用完整迁移保护，并在 SQL 前核对当前 reader coverage。 */
    public static SchemaExecutionReport executeJdbc(ReviewedSchemaPlan plan,
                                                     SyncSqlExecutor executor,
                                                     Supplier<SchemaSnapshot> snapshotReader,
                                                     Supplier<SchemaSnapshotCoverage> coverageReader,
                                                     Runnable metadataInvalidator,
                                                     SchemaMigrationExecutionOptions options) {
        SchemaMigrationExecutionOptions safeOptions = Objects.requireNonNull(
                options, "schema migration execution options must not be null");
        SyncSqlExecutor safeExecutor = Objects.requireNonNull(
                executor, "sync SQL executor must not be null");
        ReviewedSchemaPlan safePlan = Objects.requireNonNull(plan, "reviewed schema plan must not be null");
        RdbDialect databaseDialect = RdbDialectResolver.tryResolveName(safePlan.database().dialectId())
                .orElse(null);
        return new JdbcSchemaMigrationExecutor(
                safeExecutor, executionRenderer(safePlan, databaseDialect), SchemaMigrationObserver.noop(),
                databaseDialect == null ? SchemaDdlTransactionSupport.UNKNOWN
                        : SchemaDdlTransactionSupport.from(databaseDialect),
                safeExecutor::currentTransaction, ignored -> { })
                .executeReviewed(safePlan, snapshotReader, coverageReader, metadataInvalidator, safeOptions);
    }

    static SchemaExecutionReport executeJdbcGuarded(
            ReviewedSchemaPlan plan,
            Supplier<SchemaSnapshot> snapshotReader,
            Supplier<SchemaSnapshotCoverage> coverageReader,
            Runnable metadataInvalidator,
            SchemaMigrationExecutionOptions options,
            JdbcPlanExecution execution) {
        SchemaMigrationExecutionOptions safeOptions = Objects.requireNonNull(
                options, "schema migration execution options must not be null");
        return executeJdbc(plan, snapshotReader, coverageReader, metadataInvalidator,
                           safeOptions.sqlExecutionOptions(), safeOptions.approval(), execution);
    }

    private static SchemaExecutionReport executeJdbc(ReviewedSchemaPlan plan,
                                                      Supplier<SchemaSnapshot> snapshotReader,
                                                      Supplier<SchemaSnapshotCoverage> coverageReader,
                                                      Runnable metadataInvalidator,
                                                      SqlExecutionOptions options,
                                                      SchemaMigrationApproval approval,
                                                      JdbcPlanExecution execution) {
        ReviewedSchemaPlan safePlan = Objects.requireNonNull(
                plan, "reviewed schema plan must not be null");
        Supplier<SchemaSnapshot> safeReader = Objects.requireNonNull(
                snapshotReader, "schema snapshot reader must not be null");
        Supplier<SchemaSnapshotCoverage> safeCoverageReader = Objects.requireNonNull(
                coverageReader, "schema snapshot coverage reader must not be null");
        Objects.requireNonNull(metadataInvalidator, "schema metadata invalidator must not be null");
        Objects.requireNonNull(options, "schema SQL execution options must not be null");
        JdbcPlanExecution safeExecution = Objects.requireNonNull(
                execution, "reviewed JDBC plan execution must not be null");
        RelationalTableDefinition desired = safePlan.desiredTable().orElse(null);
        if (desired == null) {
            return reportWithoutExecution(safePlan, SchemaExecutionStatus.FAILED, null);
        }
        if (!executionAuthorized(safePlan, approval)) {
            return reportWithoutExecution(safePlan, SchemaExecutionStatus.FAILED, null);
        }

        String coverageFingerprint;
        try {
            coverageFingerprint = Objects.requireNonNull(
                    safeCoverageReader.get(), "schema snapshot coverage reader must not return null")
                    .fingerprint();
        } catch (RuntimeException failure) {
            return reportWithoutExecution(safePlan, SchemaExecutionStatus.UNKNOWN, null);
        }
        if (!safePlan.snapshotCoverageFingerprint().equals(coverageFingerprint)) {
            return reportWithoutExecution(
                    safePlan, SchemaExecutionStatus.PRECONDITION_FAILED, null);
        }

        SchemaSnapshot before;
        try {
            before = Objects.requireNonNull(safeReader.get(), "schema snapshot reader must not return null");
        } catch (RuntimeException failure) {
            return reportWithoutExecution(safePlan, SchemaExecutionStatus.UNKNOWN, null);
        }
        String beforeFingerprint = SchemaSnapshotFingerprint.of(before);
        if (!preconditionsMatch(safePlan, beforeFingerprint, coverageFingerprint)) {
            return reportWithoutExecution(
                    safePlan, SchemaExecutionStatus.PRECONDITION_FAILED, beforeFingerprint);
        }

        ExecutionAttempt attempt = Objects.requireNonNull(
                safeExecution.execute(safePlan.steps()),
                "reviewed JDBC plan execution must not return null");
        if (!attempt.sqlSent() && attempt.status() != SchemaExecutionStatus.SUCCESS) {
            return SchemaExecutionReport.of(
                    safePlan.fingerprint(), attempt.status(), attempt.steps(),
                    beforeFingerprint, null, null);
        }
        SchemaSnapshot after;
        try {
            after = Objects.requireNonNull(safeReader.get(), "schema snapshot reader must not return null");
        } catch (RuntimeException failure) {
            return withoutPostSnapshot(safePlan, attempt, beforeFingerprint);
        }
        return verifiedReport(
                safePlan, desired, attempt, beforeFingerprint, after);
    }

    /** 响应式执行入口，并在第一次元数据读取或 SQL 前核对当前 reader coverage。 */
    public static Mono<SchemaExecutionReport> executeReactive(
            ReviewedSchemaPlan plan,
            ReactiveSqlExecutor executor,
            Supplier<Mono<SchemaSnapshot>> snapshotReader,
            Supplier<SchemaSnapshotCoverage> coverageReader,
            Runnable metadataInvalidator,
            SqlExecutionOptions options) {
        return executeReactive(plan, executor, snapshotReader, coverageReader, metadataInvalidator,
                               new SchemaMigrationExecutionOptions(options, null));
    }

    /** 响应式完整迁移保护入口，并在 SQL 前核对当前 reader coverage。 */
    public static Mono<SchemaExecutionReport> executeReactive(
            ReviewedSchemaPlan plan,
            ReactiveSqlExecutor executor,
            Supplier<Mono<SchemaSnapshot>> snapshotReader,
            Supplier<SchemaSnapshotCoverage> coverageReader,
            Runnable metadataInvalidator,
            SchemaMigrationExecutionOptions options) {
        SchemaMigrationExecutionOptions safeOptions = Objects.requireNonNull(
                options, "schema migration execution options must not be null");
        ReactiveSqlExecutor safeExecutor = Objects.requireNonNull(
                executor, "reactive SQL executor must not be null");
        ReviewedSchemaPlan safePlan = Objects.requireNonNull(plan, "reviewed schema plan must not be null");
        RdbDialect databaseDialect = RdbDialectResolver.tryResolveName(safePlan.database().dialectId())
                .orElse(null);
        return new SchemaMigrationExecutor(
                safeExecutor, executionRenderer(safePlan, databaseDialect), SchemaMigrationObserver.noop(),
                databaseDialect == null ? SchemaDdlTransactionSupport.UNKNOWN
                        : SchemaDdlTransactionSupport.from(databaseDialect))
                .executeReviewed(safePlan, snapshotReader, coverageReader, metadataInvalidator, safeOptions);
    }

    private static FormSchemaSqlRenderer executionRenderer(ReviewedSchemaPlan plan, RdbDialect databaseDialect) {
        SchemaDialect schemaDialect = plan.comparisonDialect();
        if (schemaDialect == null) {
            schemaDialect = databaseDialect == null ? SchemaDialect.standard() : databaseDialect.schema();
        }
        // 只为会话保护读取方言；审核 SQL 仍原样交给迁移执行器。
        return FormSchemaSqlRenderer.create(schemaDialect);
    }

    static Mono<SchemaExecutionReport> executeReactiveGuarded(
            ReviewedSchemaPlan plan,
            Supplier<Mono<SchemaSnapshot>> snapshotReader,
            Supplier<SchemaSnapshotCoverage> coverageReader,
            Runnable metadataInvalidator,
            SchemaMigrationExecutionOptions options,
            ReactivePlanExecution execution) {
        SchemaMigrationExecutionOptions safeOptions = Objects.requireNonNull(
                options, "schema migration execution options must not be null");
        return executeReactive(plan, snapshotReader, coverageReader, metadataInvalidator,
                               safeOptions.sqlExecutionOptions(), safeOptions.approval(), execution);
    }

    private static Mono<SchemaExecutionReport> executeReactive(
            ReviewedSchemaPlan plan,
            Supplier<Mono<SchemaSnapshot>> snapshotReader,
            Supplier<SchemaSnapshotCoverage> coverageReader,
            Runnable metadataInvalidator,
            SqlExecutionOptions options,
            SchemaMigrationApproval approval,
            ReactivePlanExecution execution) {
        ReviewedSchemaPlan safePlan = Objects.requireNonNull(
                plan, "reviewed schema plan must not be null");
        Supplier<Mono<SchemaSnapshot>> safeReader = Objects.requireNonNull(
                snapshotReader, "schema snapshot reader must not be null");
        Supplier<SchemaSnapshotCoverage> safeCoverageReader = Objects.requireNonNull(
                coverageReader, "schema snapshot coverage reader must not be null");
        Objects.requireNonNull(metadataInvalidator, "schema metadata invalidator must not be null");
        Objects.requireNonNull(options, "schema SQL execution options must not be null");
        ReactivePlanExecution safeExecution = Objects.requireNonNull(
                execution, "reviewed reactive plan execution must not be null");
        RelationalTableDefinition desired = safePlan.desiredTable().orElse(null);
        if (desired == null) {
            return Mono.just(reportWithoutExecution(safePlan, SchemaExecutionStatus.FAILED, null));
        }
        if (!executionAuthorized(safePlan, approval)) {
            return Mono.just(reportWithoutExecution(safePlan, SchemaExecutionStatus.FAILED, null));
        }

        return Mono.defer(() -> {
            String coverageFingerprint;
            try {
                coverageFingerprint = Objects.requireNonNull(
                        safeCoverageReader.get(), "schema snapshot coverage reader must not return null")
                        .fingerprint();
            } catch (RuntimeException failure) {
                return Mono.just(reportWithoutExecution(
                        safePlan, SchemaExecutionStatus.UNKNOWN, null));
            }
            if (!safePlan.snapshotCoverageFingerprint().equals(coverageFingerprint)) {
                return Mono.just(reportWithoutExecution(
                        safePlan, SchemaExecutionStatus.PRECONDITION_FAILED, null));
            }
            return readReactive(safeReader)
                    .map(SnapshotRead::success)
                    .switchIfEmpty(Mono.just(SnapshotRead.failure()))
                    .onErrorResume(RuntimeException.class, failure -> Mono.just(SnapshotRead.failure()))
                    .flatMap(read -> executeAfterReactivePrecondition(
                            safePlan, desired, safeExecution, safeReader,
                            coverageFingerprint, read));
        });
    }

    private static Mono<SchemaExecutionReport> executeAfterReactivePrecondition(
            ReviewedSchemaPlan plan,
            RelationalTableDefinition desired,
            ReactivePlanExecution execution,
            Supplier<Mono<SchemaSnapshot>> reader,
            String coverageFingerprint,
            SnapshotRead read) {
        if (read.snapshot() == null) {
            return Mono.just(reportWithoutExecution(plan, SchemaExecutionStatus.UNKNOWN, null));
        }
        String beforeFingerprint = SchemaSnapshotFingerprint.of(read.snapshot());
        if (!preconditionsMatch(plan, beforeFingerprint, coverageFingerprint)) {
            return Mono.just(reportWithoutExecution(
                    plan, SchemaExecutionStatus.PRECONDITION_FAILED, beforeFingerprint));
        }
        return Mono.defer(() -> Objects.requireNonNull(
                        execution.execute(plan.steps()),
                        "reviewed reactive plan execution must not return null"))
                .flatMap(attempt -> {
                    if (!attempt.sqlSent() && attempt.status() != SchemaExecutionStatus.SUCCESS) {
                        return Mono.just(SchemaExecutionReport.of(
                                plan.fingerprint(), attempt.status(), attempt.steps(),
                                beforeFingerprint, null, null));
                    }
                    return readReactive(reader)
                            .map(after -> verifiedReport(
                                    plan, desired, attempt, beforeFingerprint,
                                    after))
                            .switchIfEmpty(Mono.just(withoutPostSnapshot(
                                    plan, attempt, beforeFingerprint)))
                            .onErrorResume(RuntimeException.class, failure -> Mono.just(
                                    withoutPostSnapshot(plan, attempt, beforeFingerprint)));
                });
    }

    private static SchemaExecutionReport verifiedReport(ReviewedSchemaPlan plan,
                                                        RelationalTableDefinition desired,
                                                        ExecutionAttempt attempt,
                                                        String beforeFingerprint,
                                                        SchemaSnapshot after) {
        String afterFingerprint = SchemaSnapshotFingerprint.of(after);
        SchemaCompatibilityReport verification;
        try {
            verification = SchemaDiffer.diff(
                    desired,
                    after,
                    plan.database().capabilities(),
                    plan.compatibilityMode(),
                    plan.database().dialectId(),
                    plan.comparisonDialect());
        } catch (RuntimeException failure) {
            return SchemaExecutionReport.of(
                    plan.fingerprint(),
                    completedExecution(attempt.status())
                            ? SchemaExecutionStatus.VERIFICATION_FAILED : attempt.status(),
                    attempt.steps(), beforeFingerprint, afterFingerprint, null);
        }
        SchemaExecutionStatus status = attempt.status();
        if (completedExecution(status)) {
            if (!verification.compatible()) {
                status = SchemaExecutionStatus.VERIFICATION_FAILED;
            }
        }
        return SchemaExecutionReport.of(
                plan.fingerprint(), status, attempt.steps(),
                beforeFingerprint, afterFingerprint, verification);
    }

    private static SchemaExecutionReport withoutPostSnapshot(ReviewedSchemaPlan plan,
                                                             ExecutionAttempt attempt,
                                                             String beforeFingerprint) {
        SchemaExecutionStatus status = completedExecution(attempt.status())
                ? SchemaExecutionStatus.UNKNOWN : attempt.status();
        return SchemaExecutionReport.of(
                plan.fingerprint(), status, attempt.steps(),
                beforeFingerprint, null, null);
    }

    private static SchemaExecutionReport reportWithoutExecution(ReviewedSchemaPlan plan,
                                                                SchemaExecutionStatus status,
                                                                String beforeFingerprint) {
        List<SchemaExecutionReport.StepResult> results = plan.steps().stream()
                .map(step -> SchemaExecutionReport.StepResult.notExecuted(step.order(), status))
                .toList();
        return SchemaExecutionReport.of(
                plan.fingerprint(), status, results, beforeFingerprint, null, null);
    }

    private static boolean preconditionsMatch(ReviewedSchemaPlan plan,
                                              String actualFingerprint,
                                              String coverageFingerprint) {
        if (!plan.actualFingerprint().equals(actualFingerprint)) {
            return false;
        }
        for (SchemaPlanStep step : plan.steps()) {
            for (SchemaPlanPrecondition precondition : step.preconditions()) {
                String actual = switch (precondition.kind()) {
                    case DATABASE_DESCRIPTOR -> plan.database().fingerprint();
                    case CAPABILITIES -> plan.database().capabilityFingerprint();
                    case ACTUAL_SCHEMA -> actualFingerprint;
                    case SNAPSHOT_COVERAGE -> coverageFingerprint;
                };
                if (!precondition.expectedFingerprint().equals(actual)) {
                    return false;
                }
            }
        }
        return true;
    }

    private static boolean completedExecution(SchemaExecutionStatus status) {
        return status == SchemaExecutionStatus.SUCCESS
                || status == SchemaExecutionStatus.EXTERNAL_TRANSACTION_PENDING;
    }

    private static boolean executionAuthorized(ReviewedSchemaPlan plan,
                                               SchemaMigrationApproval approval) {
        if (plan.requiresManualAction()) {
            return false;
        }
        if (plan.risk() == SchemaMigrationRiskLevel.LOW) {
            return true;
        }
        return approval != null && plan.fingerprint().equals(approval.planFingerprint());
    }

    private static Mono<SchemaSnapshot> readReactive(Supplier<Mono<SchemaSnapshot>> reader) {
        return Mono.defer(() -> Objects.requireNonNull(
                reader.get(), "schema snapshot reader must not return null"));
    }

    private static void addNotExecuted(List<SchemaPlanStep> steps,
                                       int first,
                                       List<SchemaExecutionReport.StepResult> results) {
        for (int index = first; index < steps.size(); index++) {
            results.add(SchemaExecutionReport.StepResult.notExecuted(
                    steps.get(index).order(), SchemaExecutionStatus.FAILED));
        }
    }

    private static String redactedSummary(RuntimeException failure) {
        String simpleName = failure.getClass().getSimpleName();
        return simpleName.isBlank() ? failure.getClass().getName() : simpleName;
    }

    static ExecutionAttempt successfulAttempt(List<SchemaPlanStep> steps,
                                              List<SqlExecutionStepResult> completed) {
        if (completed.size() != steps.size()) {
            throw new IllegalArgumentException("successful reviewed execution must complete every schema step");
        }
        List<SchemaExecutionReport.StepResult> results = new ArrayList<>(steps.size());
        for (int index = 0; index < steps.size(); index++) {
            results.add(SchemaExecutionReport.StepResult.success(
                    steps.get(index).order(), completed.get(index).rowsUpdated()));
        }
        return new ExecutionAttempt(results, SchemaExecutionStatus.SUCCESS, !results.isEmpty());
    }

    /**
     * 外部事务中只能确认 DDL 已执行，不能把尚未由上层提交的结果报告成最终成功。
     */
    static ExecutionAttempt externalTransactionAttempt(
            List<SchemaPlanStep> steps,
            List<SqlExecutionStepResult> completed) {
        ExecutionAttempt executed = successfulAttempt(steps, completed);
        return new ExecutionAttempt(
                executed.steps(), SchemaExecutionStatus.EXTERNAL_TRANSACTION_PENDING,
                executed.sqlSent());
    }

    static ExecutionAttempt failedAttempt(List<SchemaPlanStep> steps,
                                          List<SqlExecutionStepResult> completed,
                                          int failedIndex,
                                          boolean failedSqlSent,
                                          RuntimeException failure) {
        List<SchemaExecutionReport.StepResult> results = new ArrayList<>(steps.size());
        int successful = Math.min(completed.size(), steps.size());
        for (int index = 0; index < successful; index++) {
            results.add(SchemaExecutionReport.StepResult.success(
                    steps.get(index).order(), completed.get(index).rowsUpdated()));
        }
        if (failedIndex >= 0 && failedIndex < steps.size()) {
            while (results.size() < failedIndex) {
                results.add(SchemaExecutionReport.StepResult.notExecuted(
                        steps.get(results.size()).order(), SchemaExecutionStatus.FAILED));
            }
            results.add(SchemaExecutionReport.StepResult.failure(
                    steps.get(failedIndex).order(), SchemaExecutionStatus.UNKNOWN,
                    failedSqlSent, redactedSummary(failure)));
            addNotExecuted(steps, failedIndex + 1, results);
        } else {
            addNotExecuted(steps, results.size(), results);
        }
        boolean sqlSent = successful > 0 || failedSqlSent;
        SchemaExecutionStatus status = successful > 0
                ? SchemaExecutionStatus.PARTIAL : SchemaExecutionStatus.UNKNOWN;
        return new ExecutionAttempt(results, status, sqlSent);
    }

    static ExecutionAttempt cleanupFailedAttempt(List<SchemaPlanStep> steps,
                                                 List<SqlExecutionStepResult> completed) {
        ExecutionAttempt successful = successfulAttempt(steps, completed);
        return new ExecutionAttempt(successful.steps(), SchemaExecutionStatus.UNKNOWN,
                                    successful.sqlSent());
    }

    @FunctionalInterface
    interface JdbcPlanExecution {
        ExecutionAttempt execute(List<SchemaPlanStep> steps);
    }

    @FunctionalInterface
    interface ReactivePlanExecution {
        Mono<ExecutionAttempt> execute(List<SchemaPlanStep> steps);
    }

    static record ExecutionAttempt(List<SchemaExecutionReport.StepResult> steps,
                                   SchemaExecutionStatus status,
                                   boolean sqlSent) {

        ExecutionAttempt {
            steps = List.copyOf(steps);
        }
    }

    private record SnapshotRead(SchemaSnapshot snapshot) {

        private static SnapshotRead success(SchemaSnapshot snapshot) {
            return new SnapshotRead(Objects.requireNonNull(
                    snapshot, "schema snapshot must not be null"));
        }

        private static SnapshotRead failure() {
            return new SnapshotRead(null);
        }
    }
}
