package com.flying.orm.rdb.migration;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 顺序执行参数化数据迁移。任一步失败后，只对已经成功的步骤按相反顺序执行补偿 SQL。
 *
 * <p>这是跨数据库都能明确表达的数据级补偿，不声称 DDL 能自动恢复已经删除的数据。补偿本身失败时结果会是
 * {@link DataMigrationStatus#ROLLBACK_FAILED}，上层必须停止发布并人工处理。</p>
 *
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class ReactiveDataMigration {

    private final ReactiveSqlExecutor executor;

    /** null 表示沿用 executor 的默认保护；只有显式 create 重载才会覆盖。 */
    private final SqlExecutionOptions options;

    private ReactiveDataMigration(ReactiveSqlExecutor executor) {
        this.executor = Objects.requireNonNull(executor, "data migration executor must not be null");
        this.options = null;
    }

    private ReactiveDataMigration(ReactiveSqlExecutor executor, SqlExecutionOptions options) {
        this.executor = Objects.requireNonNull(executor, "data migration executor must not be null");
        this.options = Objects.requireNonNull(options, "data migration execution options must not be null");
    }

    public static ReactiveDataMigration create(ReactiveSqlExecutor executor) {
        return new ReactiveDataMigration(executor);
    }

    public static ReactiveDataMigration create(ReactiveSqlExecutor executor, SqlExecutionOptions options) {
        return new ReactiveDataMigration(executor, options);
    }

    public Mono<DataMigrationResult> execute(DataMigrationPlan plan) {
        DataMigrationPlan safePlan = Objects.requireNonNull(plan, "data migration plan must not be null");
        // usingWhen 的 cancel 清理会等待补偿链执行完再结束资源生命周期，普通 doOnCancel 做不到这一点。
        return Mono.usingWhen(
                Mono.fromSupplier(MigrationProgress::new),
                progress -> Flux.fromIterable(safePlan.steps())
                               .concatMap(step -> rowsUpdated(step.forward())
                                                          .doOnNext(rows -> progress.results.add(
                                                                  DataMigrationStepResult.completed(step.id(), rows))))
                               .then(Mono.fromSupplier(() -> new DataMigrationResult(
                                       safePlan.id(), DataMigrationStatus.SUCCEEDED, progress.results)))
                               .onErrorResume(failure -> progress.beginCleanup()
                                       ? compensate(safePlan, progress.results, failure)
                                       : Mono.error(failure)),
                ignored -> Mono.empty(),
                (ignored, failure) -> Mono.empty(),
                progress -> progress.beginCleanup()
                        ? rollback(safePlan, progress.results)
                                .flatMap(result -> result.status() == DataMigrationStatus.ROLLBACK_FAILED
                                        ? Mono.error(new DataMigrationException(
                                                result,
                                                new CancellationException("data migration subscription was cancelled")))
                                        : Mono.empty())
                        : Mono.empty());
    }

    private Mono<DataMigrationResult> compensate(DataMigrationPlan plan,
                                                  List<DataMigrationStepResult> results,
                                                  Throwable originalFailure) {
        return rollback(plan, results)
                .flatMap(result -> Mono.error(new DataMigrationException(result, originalFailure)));
    }

    private Mono<DataMigrationResult> rollback(DataMigrationPlan plan,
                                               List<DataMigrationStepResult> results) {
        AtomicInteger activeIndex = new AtomicInteger(-1);
        List<Integer> indexes = new ArrayList<>();
        for (int index = 0; index < results.size(); index++) {
            indexes.add(index);
        }
        Collections.reverse(indexes);
        return Flux.fromIterable(indexes)
                   .concatMap(index -> {
                       activeIndex.set(index);
                       DataMigrationStep step = plan.steps().get(index);
                       return rowsUpdated(step.rollback())
                                      .doOnNext(rows -> results.set(index, results.get(index).rolledBack(rows)))
                                      .onErrorResume(failure -> {
                                          VirtualMachineError fatal = findVirtualMachineError(failure);
                                          if (fatal != null) {
                                              return Mono.error(fatal);
                                          }
                                          results.set(index, results.get(index).rollbackFailed(failure));
                                          return Mono.empty();
                                      });
                   })
                   .then(Mono.fromSupplier(() -> {
                       boolean rollbackFailed = results.stream().anyMatch(result -> result.rollbackFailure() != null);
                       return new DataMigrationResult(
                               plan.id(),
                               rollbackFailed ? DataMigrationStatus.ROLLBACK_FAILED : DataMigrationStatus.ROLLED_BACK,
                               results);
                   }))
                   .transform(rollback -> withCleanupTimeout(rollback, plan, results, activeIndex));
    }

    /**
     * 在驱动包装的异常图中保留 JVM 致命错误；按对象身份去重，避免异常 cause/suppressed 环导致补偿链卡死。
     */
    private static VirtualMachineError findVirtualMachineError(Throwable failure) {
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        pending.add(Objects.requireNonNull(failure, "migration failure must not be null"));
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (current instanceof VirtualMachineError fatal) {
                return fatal;
            }
            Throwable cause = current.getCause();
            if (cause != null) {
                pending.addLast(cause);
            }
            Throwable[] suppressed = current.getSuppressed();
            for (int index = suppressed.length - 1; index >= 0; index--) {
                pending.addFirst(suppressed[index]);
            }
        }
        return null;
    }

    private Mono<DataMigrationResult> withCleanupTimeout(Mono<DataMigrationResult> rollback,
                                                         DataMigrationPlan plan,
                                                         List<DataMigrationStepResult> results,
                                                         AtomicInteger activeIndex) {
        Duration timeout = options == null
                ? SqlExecutionOptions.DEFAULT_CLEANUP_TIMEOUT
                : options.cleanupTimeout();
        if (timeout.isZero()) {
            return rollback;
        }
        return rollback.timeout(timeout)
                       .onErrorResume(TimeoutException.class, failure -> {
                           int index = activeIndex.get();
                           if (index >= 0 && index < results.size()
                                   && results.get(index).rollbackFailure() == null) {
                               results.set(index, results.get(index).rollbackFailed(failure));
                           }
                           return Mono.just(new DataMigrationResult(plan.id(),
                                                                     DataMigrationStatus.ROLLBACK_FAILED,
                                                                     results));
                       });
    }

    /** 没有单次覆盖时走无 options 契约，让执行器自己应用已经装好的统一默认值。 */
    private Mono<Long> rowsUpdated(SqlRequest request) {
        return options == null ? executor.rowsUpdated(request) : executor.rowsUpdated(request, options);
    }

    /** 每次订阅独享的进度。原子门闩避免错误和取消竞态时把同一步补偿两遍。 */
    private static final class MigrationProgress {
        private final List<DataMigrationStepResult> results = new ArrayList<>();
        private final AtomicBoolean cleanupStarted = new AtomicBoolean();

        private boolean beginCleanup() {
            return cleanupStarted.compareAndSet(false, true);
        }
    }
}
