package com.flying.orm.rdb.schema;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionSequence;
import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.observation.SqlFailureCategory;
import com.flying.orm.rdb.reactive.ConnectionScopedReactiveSqlExecutor;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * 负责按顺序执行迁移 SQL，并统一处理观测和元数据缓存失效。
 *
 * <p>批准校验发生在第一条 SQL 之前；一旦真正开始执行，无论成功、失败还是取消都保守失效目标表缓存。
 * observer 和缓存清理的普通故障属于旁路动作，不能覆盖数据库已经确定的结果；JVM 致命错误仍原样传播。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
final class SchemaMigrationExecutor {

    private final ReactiveSqlExecutor executor;
    private final FormSchemaSqlRenderer renderer;
    private final SchemaMigrationObserver observer;
    private final SchemaDdlTransactionSupport ddlTransactionSupport;

    SchemaMigrationExecutor(ReactiveSqlExecutor executor,
                            FormSchemaSqlRenderer renderer,
                            SchemaMigrationObserver observer,
                            SchemaDdlTransactionSupport ddlTransactionSupport) {
        this.executor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        this.renderer = Objects.requireNonNull(renderer, "form schema sql renderer must not be null");
        this.observer = SchemaMigrationObservers.safe(observer);
        this.ddlTransactionSupport = Objects.requireNonNull(
                ddlTransactionSupport, "DDL transaction support must not be null");
    }

    Mono<Long> execute(List<SqlRequest> requests, SqlExecutionOptions options) {
        return guardExternalTransaction(requests, false)
                .then(Mono.defer(() -> executeRequests(requests, options)));
    }

    Mono<Long> execute(List<SqlRequest> requests) {
        return guardExternalTransaction(requests, false)
                .then(Mono.defer(() -> executeRequests(requests)));
    }

    private Mono<Long> executeRequests(List<SqlRequest> requests, SqlExecutionOptions options) {
        return Flux.fromIterable(requests)
                   .concatMap(request -> executor.rowsUpdated(request, options))
                   .reduce(0L, SchemaMigrationExecutor::addExact);
    }

    private Mono<Long> executeRequests(List<SqlRequest> requests) {
        return Flux.fromIterable(requests)
                   .concatMap(executor::rowsUpdated)
                   .reduce(0L, SchemaMigrationExecutor::addExact);
    }

    Mono<Long> executeWithInvalidation(List<SqlRequest> requests,
                                       List<String> tables,
                                       Consumer<String> metadataInvalidator,
                                       SqlExecutionOptions options) {
        if (requests.isEmpty()) {
            return Mono.just(0L);
        }
        List<String> safeTables = safeTables(tables);
        return Mono.defer(() -> {
            AtomicBoolean started = new AtomicBoolean();
            AtomicBoolean invalidated = new AtomicBoolean();
            return guardExternalTransaction(requests, false, safeTables, metadataInvalidator)
                    .then(Mono.defer(() -> {
                        started.set(true);
                        return executeRequests(requests, options);
                    }))
                    .doOnSuccess(ignored -> invalidateAfterExecution(
                            started, invalidated, metadataInvalidator, safeTables))
                    .doOnError(ignored -> invalidateAfterExecution(
                            started, invalidated, metadataInvalidator, safeTables))
                    .doFinally(ignored -> invalidateAfterExecution(
                            started, invalidated, metadataInvalidator, safeTables));
        });
    }

    Mono<SchemaMigrationResult> executeReviewed(ReviewedSchemaMigrationPlan plan,
                                                List<String> tables,
                                                Consumer<String> metadataInvalidator,
                                                SchemaMigrationExecutionOptions options) {
        ReviewedSchemaMigrationPlan safePlan = Objects.requireNonNull(
                plan, "reviewed schema migration plan must not be null");
        Consumer<String> safeInvalidator = Objects.requireNonNull(
                metadataInvalidator, "schema metadata invalidator must not be null");
        List<String> safeTables = safeTables(tables);
        SchemaMigrationExecutionOptions safeOptions = Objects.requireNonNull(
                options, "schema migration execution options must not be null");
        return Mono.defer(() -> {
            long startedAt = System.nanoTime();
            AtomicBoolean observed = new AtomicBoolean();
            AtomicBoolean executionStarted = new AtomicBoolean();
            AtomicBoolean invalidated = new AtomicBoolean();
            return executeReviewedPlan(
                    safePlan, safeOptions, executionStarted, safeTables, safeInvalidator)
                    .doOnSuccess(result -> observe(observed, safePlan, result, startedAt,
                                                   SqlExecutionStatus.SUCCESS, null))
                    .doOnError(error -> observe(observed, safePlan, null, startedAt,
                                                SqlExecutionStatus.ERROR, error))
                    .doOnCancel(() -> observe(observed, safePlan, null, startedAt,
                                              SqlExecutionStatus.CANCELLED, null))
                    .doOnSuccess(ignored -> invalidateAfterExecution(
                            executionStarted, invalidated, safeInvalidator, safeTables))
                    .doOnError(ignored -> invalidateAfterExecution(
                            executionStarted, invalidated, safeInvalidator, safeTables))
                    .doFinally(ignored -> invalidateAfterExecution(
                            executionStarted, invalidated, safeInvalidator, safeTables));
        });
    }

    private Mono<SchemaMigrationResult> executeReviewedPlan(ReviewedSchemaMigrationPlan plan,
                                                             SchemaMigrationExecutionOptions options,
                                                             AtomicBoolean executionStarted,
                                                             List<String> tables,
                                                             Consumer<String> metadataInvalidator) {
        List<SqlRequest> requests = plan.requestsForExecution(options.approval());
        if (requests.isEmpty()) {
            return Mono.just(new SchemaMigrationResult(plan.migration(), 0L, List.of()));
        }
        return guardExternalTransaction(
                requests, plan.onlineDdl().requiresNonTransactionalExecution(), tables, metadataInvalidator)
                .then(Mono.defer(() -> executeReviewedRequests(plan, options, executionStarted, requests)));
    }

    private Mono<SchemaMigrationResult> executeReviewedRequests(ReviewedSchemaMigrationPlan plan,
                                                                  SchemaMigrationExecutionOptions options,
                                                                  AtomicBoolean executionStarted,
                                                                  List<SqlRequest> requests) {
        if (!options.hasLockTimeout()) {
            return executeRequests(requests, options.sqlExecutionOptions())
                    .doOnSubscribe(ignored -> executionStarted.set(true))
                    .map(rows -> new SchemaMigrationResult(plan.migration(), rows, List.of()));
        }
        if (!(executor instanceof ConnectionScopedReactiveSqlExecutor scoped)) {
            return Mono.error(new SchemaMigrationRejectedException(
                    SchemaMigrationFailureCode.EXECUTOR_CAPABILITY_REQUIRED,
                    "DDL lock timeout requires a connection-scoped reactive SQL executor"));
        }
        SchemaDdlSessionGuard guard = renderer.lockTimeoutGuard(options.lockTimeout());
        SqlExecutionSequence sequence = new SqlExecutionSequence(guard.setup(), requests, guard.cleanup());
        return scoped.executeInConnection(sequence, options.sqlExecutionOptions())
                     .doOnSubscribe(ignored -> executionStarted.set(true))
                     .map(result -> new SchemaMigrationResult(
                             plan.migration(), result.rowsUpdated(), result.workSteps()));
    }

    /**
     * 外部事务存在时，只允许已确认支持事务的普通 DDL。隐式提交、未知方言和计划明确标出的非事务语句
     * 都在第一条 SQL 前拒绝；没有外部事务时保持原来的执行行为。
     */
    private Mono<Void> guardExternalTransaction(List<SqlRequest> requests,
                                                boolean requiresNonTransactionalExecution) {
        return guardExternalTransaction(requests, requiresNonTransactionalExecution, null, null);
    }

    private Mono<Void> guardExternalTransaction(List<SqlRequest> requests,
                                                boolean requiresNonTransactionalExecution,
                                                List<String> tables,
                                                Consumer<String> metadataInvalidator) {
        if (requests.isEmpty()) {
            return Mono.empty();
        }
        return executor.currentTransaction()
                       .flatMap(transaction -> guardExternalTransaction(
                               transaction, requiresNonTransactionalExecution, tables, metadataInvalidator))
                       .then();
    }

    private Mono<Void> guardExternalTransaction(R2dbcTransactionContext transaction,
                                                boolean requiresNonTransactionalExecution,
                                                List<String> tables,
                                                Consumer<String> metadataInvalidator) {
        if (!ddlTransactionSupport.allowsExternalTransaction() || requiresNonTransactionalExecution) {
            return Mono.error(new SchemaMigrationRejectedException(
                    SchemaMigrationFailureCode.DDL_TRANSACTION_NOT_SUPPORTED,
                    "DDL cannot join the current external transaction: support=" + ddlTransactionSupport));
        }
        if (tables != null && !transaction.completion().register(
                ignored -> Mono.fromRunnable(() -> invalidateTables(metadataInvalidator, tables)))) {
            return Mono.error(new SchemaMigrationRejectedException(
                    SchemaMigrationFailureCode.DDL_TRANSACTION_NOT_SUPPORTED,
                    "external DDL requires transaction completion notification for metadata consistency"));
        }
        return Mono.empty();
    }

    private void observe(AtomicBoolean observed,
                         ReviewedSchemaMigrationPlan plan,
                         SchemaMigrationResult result,
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
        int completed = result == null
                ? sequenceFailure == null ? 0 : sequenceFailure.completedWorkSteps().size()
                : result.steps().isEmpty() ? plan.migration().executableSqlCount() : result.steps().size();
        long rows;
        try {
            rows = result == null
                    ? sequenceFailure == null ? 0L : sequenceFailure.completedWorkSteps().stream()
                                                                      .mapToLong(step -> step.rowsUpdated())
                                                                      .reduce(0L, SchemaMigrationExecutor::addExact)
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
                plan.fingerprint(), plan.riskLevel(), status, plan.migration().executableSqlCount(), completed,
                rows, System.nanoTime() - startedAt, category,
                sequenceFailure == null ? null : sequenceFailure.phase(),
                sequenceFailure == null ? null : sequenceFailure.stepIndex(), error));
    }

    private static SqlExecutionSequenceException findSequenceFailure(Throwable error) {
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        Throwable current = error;
        while (current != null && visited.add(current)) {
            if (current instanceof SqlExecutionSequenceException sequenceFailure) {
                return sequenceFailure;
            }
            current = current.getCause();
        }
        return null;
    }

    private static void invalidateAfterExecution(AtomicBoolean started,
                                                 AtomicBoolean invalidated,
                                                 Consumer<String> metadataInvalidator,
                                                 List<String> tables) {
        if (!started.get() || !invalidated.compareAndSet(false, true)) {
            return;
        }
        invalidateTables(metadataInvalidator, tables);
    }

    private static void invalidateTables(Consumer<String> metadataInvalidator, List<String> tables) {
        for (String table : tables) {
            try {
                metadataInvalidator.accept(table);
            } catch (RuntimeException failure) {
                SchemaMigrationObservers.rethrowVirtualMachineError(failure);
            }
            // 清理失败不能反转已经确定的 DDL 结果。
        }
    }

    private static List<String> safeTables(List<String> tables) {
        List<String> copied = List.copyOf(Objects.requireNonNull(
                tables, "schema metadata tables must not be null"));
        LinkedHashSet<String> unique = new LinkedHashSet<>();
        for (String table : copied) {
            if (table.isBlank()) {
                throw new IllegalArgumentException("schema metadata table must not be blank");
            }
            unique.add(table);
        }
        if (unique.isEmpty()) {
            throw new IllegalArgumentException("schema metadata tables must not be empty");
        }
        return List.copyOf(unique);
    }

    private static long addExact(long left, long right) {
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
