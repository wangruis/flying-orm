package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorPageResult;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetPageResult;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.aggregate.AggregateResultDecoder;
import com.flying.orm.rdb.aggregate.AggregateRow;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.aggregate.FormAggregatePlanner;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.lock.LockingReadSpec;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 动态表单内部操作总入口。
 *
 * <p>查询、JOIN、聚合和单条写入都由本类直接拥有，避免为同一个执行所有者再建立无状态转发层。
 * 只有需要独立流式状态机的批量写入仍交给专用协作者；它们共享本对象创建的 Scope 合并器、结果映射器
 * 和默认执行配置，不复制 SQL、安全校验或执行保护逻辑。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class ReactiveFormOperations extends ReactiveFormOperationSupport {

    final ReactiveFormBatchInsertOperations batchInserts;
    final ReactiveFormBatchUpdateOperations batchUpdates;
    private final JoinQueryPlanner joinPlanner;

    ReactiveFormOperations(ReactiveSqlExecutor executor, FormConfiguration configuration) {
        super(executor, configuration);
        this.batchInserts = new ReactiveFormBatchInsertOperations(this);
        this.batchUpdates = new ReactiveFormBatchUpdateOperations(this);
        this.joinPlanner = new JoinQueryPlanner(renderer, scopes, defaultExecutionOptions);
    }

    Flux<DynamicRow> selectJoin(JoinQuerySpec spec, com.flying.orm.rdb.execution.SqlExecutionOptions options) {
        JoinQuerySpec safeSpec = Objects.requireNonNull(spec, "join query spec must not be null");
        if (governed) {
            return requiresProtectedPlanning(safeSpec)
                    ? ReactiveProtectionCpuBoundary.plan(() -> joinPlanner.planGoverned(
                            safeSpec, options, fieldUsePolicy, queryShapeLimits))
                                                   .flatMapMany(this::selectJoin)
                    : selectJoin(joinPlanner.planGoverned(
                            safeSpec, options, fieldUsePolicy, queryShapeLimits));
        }
        return requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(() -> joinPlanner.plan(safeSpec, options))
                                               .flatMapMany(this::selectJoinPlan)
                : selectJoinPlan(joinPlanner.plan(safeSpec, options));
    }

    FieldUseSnapshot previewFieldUse(JoinQuerySpec spec) {
        return governed
                ? joinPlanner.planGoverned(spec, null, fieldUsePolicy, queryShapeLimits).fieldUse()
                : FieldUseSnapshot.unrestricted();
    }

    Mono<PageResult<DynamicRow>> pageJoin(JoinQuerySpec spec,
                                          PageQuery page,
                                          com.flying.orm.rdb.execution.SqlExecutionOptions options) {
        JoinQuerySpec safeSpec = Objects.requireNonNull(spec, "join query spec must not be null");
        PageQuery safePage = Objects.requireNonNull(page, "join page query must not be null");
        if (governed) {
            return requiresProtectedPlanning(safeSpec)
                    ? ReactiveProtectionCpuBoundary.plan(() -> joinPlanner.pageGoverned(
                            safeSpec, safePage, options, fieldUsePolicy, queryShapeLimits))
                                                   .flatMap(this::pageJoin)
                    : pageJoin(joinPlanner.pageGoverned(
                            safeSpec, safePage, options, fieldUsePolicy, queryShapeLimits));
        }
        return requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(
                        () -> joinPlanner.page(safeSpec, safePage, options))
                                               .flatMap(this::pageJoinPlan)
                : pageJoinPlan(joinPlanner.page(safeSpec, safePage, options));
    }

    private Flux<DynamicRow> selectJoin(
            GovernedPlanEnvelope<JoinQueryPlanner.PlannedJoin> envelope) {
        JoinQueryPlanner.PlannedJoin plan = envelope.plan();
        return selectJoinPlan(plan).map(row -> FieldUseGuard.applyJoinVisibility(
                renderer, plan.spec(), row, envelope.fieldUse()));
    }

    private Flux<DynamicRow> selectJoinPlan(JoinQueryPlanner.PlannedJoin plan) {
        Flux<DynamicRow> rows = results.decodeRows(
                plan.resultForm(), executor.query(plan.request(), plan.options()), plan.options(),
                DataScope.none(), SensitiveDisplayMode.FULL, plan.decodingPlan());
        return plan.resultPlan().direct() ? rows
                : ReactiveProtectionCpuBoundary.sequence(
                        rows, plan.resultPlan().requiresCpuBoundary(),
                        ReactiveProtectionCpuBoundary.QUERY_PREFETCH)
                .map(plan.resultPlan()::transform);
    }

    private Mono<PageResult<DynamicRow>> pageJoin(
            GovernedPlanEnvelope<JoinQueryPlanner.PlannedJoinPage> envelope) {
        JoinQueryPlanner.PlannedJoinPage plan = envelope.plan();
        return pageJoinPlan(plan).map(result -> PageResult.of(
                result.rows().stream()
                        .map(row -> FieldUseGuard.applyJoinVisibility(
                                renderer, plan.spec(), row, envelope.fieldUse()))
                        .toList(),
                result.total(), plan.page()));
    }

    private Mono<PageResult<DynamicRow>> pageJoinPlan(
            JoinQueryPlanner.PlannedJoinPage plan) {
        Mono<Long> total = executor.query(plan.countRequest(), plan.options())
                                   .next()
                                   .map(CountResultReader::read)
                                   .defaultIfEmpty(0L);
        return total.flatMap(count -> count == 0L
                ? Mono.just(PageResult.of(List.of(), 0L, plan.page()))
                : pageJoinRows(plan)
                        .collectList()
                        .map(rows -> PageResult.of(rows, count, plan.page())));
    }

    private Flux<DynamicRow> pageJoinRows(
            JoinQueryPlanner.PlannedJoinPage plan) {
        Flux<DynamicRow> rows = results.decodeRows(
                plan.resultForm(), executor.query(plan.dataRequest(), plan.options()), plan.options(),
                DataScope.none(), SensitiveDisplayMode.FULL, plan.decodingPlan());
        return plan.resultPlan().direct() ? rows
                : ReactiveProtectionCpuBoundary.sequence(
                        rows, plan.resultPlan().requiresCpuBoundary(),
                        ReactiveProtectionCpuBoundary.QUERY_PREFETCH)
                .map(plan.resultPlan()::transform);
    }

    FieldUseSnapshot previewFieldUse(AggregateSpec spec) {
        return aggregatePlan(Objects.requireNonNull(
                spec, "aggregate spec must not be null")).fieldUse();
    }

    Flux<AggregateRow> aggregate(AggregateSpec spec) {
        AggregateSpec safeSpec = Objects.requireNonNull(
                spec, "aggregate spec must not be null");
        return requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(() -> aggregatePlan(safeSpec))
                                               .flatMapMany(this::executeAggregate)
                : executeAggregate(aggregatePlan(safeSpec));
    }

    private Flux<AggregateRow> executeAggregate(FormAggregatePlanner.Plan plan) {
        AggregateResultDecoder aggregateDecoder = new AggregateResultDecoder(plan);
        return executor.query(plan.request(), plan.options()).map(aggregateDecoder::decode);
    }

    private FormAggregatePlanner.Plan aggregatePlan(AggregateSpec spec) {
        return new FormAggregatePlanner(
                renderer, configuration.resolver(), configuration.dataScope(),
                defaultExecutionOptions, fieldUsePolicy, queryShapeLimits)
                .plan(spec, configuration.explicitGovernance());
    }

    Flux<DynamicRow> selectSpec(QuerySpec spec) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        if (governed) {
            return selectSpecGoverned(safeSpec, fieldUsePolicy, queryShapeLimits);
        }
        return requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(() -> planner.select(safeSpec)).flatMapMany(this::select)
                : select(planner.select(safeSpec));
    }

    Flux<DynamicRow> selectSpecGoverned(QuerySpec spec,
                                        FieldUsePolicy policy,
                                        QueryShapeLimits limits) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        FieldUsePolicy safePolicy = Objects.requireNonNull(policy, "field use policy must not be null");
        QueryShapeLimits safeLimits = Objects.requireNonNull(limits, "query shape limits must not be null");
        return requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(
                        () -> planner.selectGoverned(safeSpec, safePolicy, safeLimits))
                                               .flatMapMany(this::select)
                : select(planner.selectGoverned(safeSpec, safePolicy, safeLimits));
    }

    Flux<DynamicRow> lockingReadSpec(LockingReadSpec spec) {
        LockingReadSpec safeSpec = Objects.requireNonNull(
                spec, "locking read spec must not be null");
        return Flux.defer(() -> {
            if (governed) {
                GovernedPlanEnvelope<FormOperationPlanner.PlannedLockingRead> envelope =
                        planner.lockingReadGoverned(
                                safeSpec, fieldUsePolicy, queryShapeLimits);
                return select(new GovernedPlanEnvelope<>(
                        envelope.plan().query(), envelope.fieldUse()));
            }
            FormOperationPlanner.PlannedLockingRead plan = planner.lockingRead(safeSpec);
            return select(plan.query());
        });
    }

    FieldUseSnapshot previewFieldUse(QuerySpec spec) {
        return governed
                ? planner.selectGoverned(spec, fieldUsePolicy, queryShapeLimits).fieldUse()
                : FieldUseSnapshot.unrestricted();
    }

    private Flux<DynamicRow> select(GovernedPlanEnvelope<FormOperationPlanner.PlannedQuery> envelope) {
        FormOperationPlanner.PlannedQuery plan = envelope.plan();
        return select(plan).map(row -> FieldUseGuard.applyVisibility(
                renderer, plan.form(), row, envelope.fieldUse()));
    }

    private Flux<DynamicRow> select(FormOperationPlanner.PlannedQuery plan) {
        if (plan.contains()) {
            return executor.query(plan.request(), plan.options())
                           .collectList()
                           .flatMapMany(rows -> verifyContains(plan, rows));
        }
        return results.decodeRows(plan.form(), executor.query(plan.request(), plan.options()), plan.options(),
                                  plan.scope(), plan.displayMode(), plan.decodingFields());
    }

    Mono<PageResult<DynamicRow>> pageSpec(QuerySpec spec, PageQuery page) {
        return ReactiveFormPageResultSupport.pageSpec(this, spec, page);
    }

    private Flux<DynamicRow> verifyContains(FormOperationPlanner.PlannedQuery plan,
                                            List<DynamicRow> rawRows) {
        ProtectedContainsResultSupport.requireCandidateLimit(rawRows.size());
        return results.decodeRows(plan.form(), Flux.fromIterable(rawRows), plan.options(),
                                  plan.scope(), com.flying.orm.core.protection.SensitiveDisplayMode.FULL,
                                  plan.decodingFields())
                      .collectList()
                      .flatMapMany(rows -> Flux.fromIterable(containsResults.finish(
                              plan.form(), plan.containsQuery(), rows, plan.outputFields(), plan.displayMode())));
    }

    Mono<CursorPageResult<DynamicRow>> cursorPageSpec(QuerySpec spec, CursorPageQuery page) {
        return ReactiveFormPageResultSupport.cursorPageSpec(this, spec, page);
    }

    Mono<KeysetPageResult<DynamicRow>> keysetPageSpec(QuerySpec spec, KeysetPageQuery page) {
        return ReactiveFormPageResultSupport.keysetPageSpec(this, spec, page);
    }

    Mono<KeysetPageResult<DynamicRow>> lockingReadSpec(
            LockingReadSpec spec,
            KeysetPageQuery page) {
        LockingReadSpec safeSpec = Objects.requireNonNull(
                spec, "locking read spec must not be null");
        KeysetPageQuery safePage = Objects.requireNonNull(
                page, "keyset page query must not be null");
        return Mono.defer(() -> {
            if (governed) {
                GovernedPlanEnvelope<FormOperationPlanner.PlannedLockingKeysetRead> envelope =
                        planner.lockingKeysetReadGoverned(
                                safeSpec, safePage, fieldUsePolicy, queryShapeLimits);
                return keysetPage(envelope.plan().query(), envelope.fieldUse());
            }
            FormOperationPlanner.PlannedLockingKeysetRead plan =
                    planner.lockingKeysetRead(safeSpec, safePage);
            return keysetPage(plan.query(), null);
        });
    }

    private Mono<KeysetPageResult<DynamicRow>> keysetPage(
            FormOperationPlanner.PlannedKeysetPage plan,
            FieldUseSnapshot fieldUse) {
        return ReactiveFormPageResultSupport.keysetPage(this, plan, fieldUse);
    }

    Mono<Long> insertSpec(WriteSpec spec) {
        WriteSpec safeSpec = Objects.requireNonNull(spec, "insert spec must not be null");
        if (governed) {
            return requiresProtectedPlanning(safeSpec)
                    ? ReactiveProtectionCpuBoundary.plan(() -> planner.insertGoverned(
                            safeSpec, fieldUsePolicy, queryShapeLimits)).flatMap(envelope -> write(envelope.plan()))
                    : write(planner.insertGoverned(safeSpec, fieldUsePolicy, queryShapeLimits).plan());
        }
        return requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(() -> planner.insert(safeSpec)).flatMap(this::write)
                : write(planner.insert(safeSpec));
    }

    private Mono<Long> write(FormOperationPlanner.PlannedWrite plan) {
        if (plan.protectedWriteRequired()) {
            return executor.protectedWrite(plan.protectedWrite(), plan.options())
                           .map(result -> plan.requireSuccess(result.affectedRows()));
        }
        return executor.rowsUpdated(plan.request(), plan.options()).map(plan::requireSuccess);
    }

    Mono<SqlWriteResult> insertReturningKeysSpec(WriteSpec spec) {
        WriteSpec safeSpec = Objects.requireNonNull(spec, "insert spec must not be null");
        if (governed) {
            return requiresProtectedPlanning(safeSpec)
                    ? ReactiveProtectionCpuBoundary.plan(() -> planner.insertGoverned(
                            safeSpec, fieldUsePolicy, queryShapeLimits))
                                                   .flatMap(envelope -> writeReturningKeys(envelope.plan()))
                    : writeReturningKeys(
                            planner.insertGoverned(safeSpec, fieldUsePolicy, queryShapeLimits).plan());
        }
        return requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(() -> planner.insert(safeSpec))
                                               .flatMap(this::writeReturningKeys)
                : writeReturningKeys(planner.insert(safeSpec));
    }

    private Mono<SqlWriteResult> writeReturningKeys(FormOperationPlanner.PlannedWrite plan) {
        if (plan.protectedWriteRequired()) {
            return executor.protectedWrite(plan.protectedWrite(), plan.options())
                           .doOnNext(result -> plan.requireSuccess(result.affectedRows()));
        }
        Mono<SqlWriteResult> result = plan.generatedKeyColumn()
                                          .map(column -> executor.rowsUpdatedReturningKeys(
                                                  plan.request(), plan.options(), column))
                                          .orElseGet(() -> executor.rowsUpdatedReturningKeys(
                                                  plan.request(), plan.options()));
        return result.doOnNext(value -> plan.requireSuccess(value.affectedRows()));
    }

    Mono<Long> updateSpec(WriteSpec spec) {
        WriteSpec safeSpec = Objects.requireNonNull(spec, "update spec must not be null");
        if (governed) {
            return requiresProtectedPlanning(safeSpec)
                    ? ReactiveProtectionCpuBoundary.plan(() -> planner.updateGoverned(
                            safeSpec, fieldUsePolicy, queryShapeLimits)).flatMap(envelope -> write(envelope.plan()))
                    : write(planner.updateGoverned(safeSpec, fieldUsePolicy, queryShapeLimits).plan());
        }
        return requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(() -> planner.update(safeSpec)).flatMap(this::write)
                : write(planner.update(safeSpec));
    }

    Mono<Long> deleteSpec(WriteSpec spec) {
        WriteSpec safeSpec = Objects.requireNonNull(spec, "delete spec must not be null");
        if (governed) {
            return requiresProtectedPlanning(safeSpec)
                    ? ReactiveProtectionCpuBoundary.plan(() -> planner.deleteGoverned(
                            safeSpec, fieldUsePolicy, queryShapeLimits)).flatMap(envelope -> write(envelope.plan()))
                    : write(planner.deleteGoverned(safeSpec, fieldUsePolicy, queryShapeLimits).plan());
        }
        return requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(() -> planner.delete(safeSpec)).flatMap(this::write)
                : write(planner.delete(safeSpec));
    }

    Mono<Long> physicalDeleteSpec(WriteSpec spec) {
        WriteSpec safeSpec = Objects.requireNonNull(spec, "physical delete spec must not be null");
        if (governed) {
            return requiresProtectedPlanning(safeSpec)
                    ? ReactiveProtectionCpuBoundary.plan(() -> planner.physicalDeleteGoverned(
                            safeSpec, fieldUsePolicy, queryShapeLimits)).flatMap(envelope -> write(envelope.plan()))
                    : write(planner.physicalDeleteGoverned(
                            safeSpec, fieldUsePolicy, queryShapeLimits).plan());
        }
        return requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(() -> planner.physicalDelete(safeSpec)).flatMap(this::write)
                : write(planner.physicalDelete(safeSpec));
    }

    Mono<BatchExecutionEvidence> writeBatchSpec(BatchSpec spec, java.util.function.LongFunction<? extends Publisher<Void>> rowCompleted) {
        BatchSpec safeSpec = Objects.requireNonNull(spec, "batch spec must not be null");
        BatchWriteOptions options = safeSpec.options().orElse(defaultBatchWriteOptions);
        return switch (safeSpec.operation()) {
            case INSERT -> batchInserts.writeBatch(safeSpec.form(), mapRows(safeSpec), options, false,
                                                   safeSpec.scope(), safeSpec.generatedKeys(), rowCompleted);
            case UPSERT -> batchInserts.writeBatch(safeSpec.form(), mapRows(safeSpec), options, true,
                                                   safeSpec.scope(), safeSpec.generatedKeys(), rowCompleted);
            case UPDATE -> batchUpdates.updateBatch(safeSpec.form(), updateRows(safeSpec), safeSpec.scope(), options,
                                                     rowCompleted);
        };
    }



    private boolean requiresProtectedPlanning(JoinQuerySpec spec) {
        for (JoinSource source : spec.sources()) {
            DynamicForm form = source.form();
            if (form.protections().encryptedFields().isEmpty()) {
                continue;
            }
            DataScope effectiveScope = scopes.effectiveScope(spec.scope(source));
            if (ReactiveProtectionCpuBoundary.usesEncryptedCondition(form, spec.where(source))
                    || ReactiveProtectionCpuBoundary.usesEncryptedScope(form, effectiveScope)) {
                return true;
            }
        }
        return false;
    }

    private boolean requiresProtectedPlanning(AggregateSpec spec) {
        QuerySpec query = spec.query();
        if (query.form().protections().encryptedFields().isEmpty()) {
            return false;
        }
        DataScope effectiveScope = configuration.dataScope().and(query.scope());
        return ReactiveProtectionCpuBoundary.usesEncryptedCondition(query.form(), query.where())
                || ReactiveProtectionCpuBoundary.usesEncryptedScope(query.form(), effectiveScope)
                || query.structuredInput().isPresent();
    }

    boolean requiresProtectedPlanning(QuerySpec spec) {
        if (spec.form().protections().encryptedFields().isEmpty()) {
            return false;
        }
        DataScope effectiveScope = scopes.effectiveScope(spec.scope());
        return ReactiveProtectionCpuBoundary.usesEncryptedCondition(spec.form(), spec.where())
                || ReactiveProtectionCpuBoundary.usesEncryptedScope(spec.form(), effectiveScope)
                || spec.structuredInput().isPresent();
    }

    private boolean requiresProtectedPlanning(WriteSpec spec) {
        if (spec.form().protections().encryptedFields().isEmpty()) {
            return false;
        }
        DataScope effectiveScope = scopes.effectiveScope(spec.scope());
        return ReactiveProtectionCpuBoundary.writesEncryptedField(spec.form(), spec.ownedValues())
                || ReactiveProtectionCpuBoundary.usesEncryptedCondition(spec.form(), spec.where())
                || ReactiveProtectionCpuBoundary.usesEncryptedScope(spec.form(), effectiveScope);
    }

    @SuppressWarnings("unchecked")
    private static Publisher<Map<String, Object>> mapRows(BatchSpec spec) {
        return Flux.from(spec.rows()).map(row -> {
            if (!(row instanceof Map<?, ?>)) {
                throw new IllegalArgumentException("insert/upsert batch rows must be field maps");
            }
            return (Map<String, Object>) row;
        });
    }

    private static Publisher<BatchOptimisticUpdate> updateRows(BatchSpec spec) {
        return Flux.from(spec.rows()).map(row -> {
            if (!(row instanceof BatchOptimisticUpdate update)) {
                throw new IllegalArgumentException("update batch rows must be BatchOptimisticUpdate values");
            }
            return update;
        });
    }

}
