package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetPageResult;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.aggregate.AggregateRow;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.form.FormOperationPlanner.WriteAction;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.mapping.RowMapper;
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
final class ReactiveFormOperations {

    final ReactiveSqlExecutor executor;
    final FormDataSqlRenderer renderer;
    final BatchWriteOptions defaultBatchWriteOptions;
    final FormScopeSupport scopes;
    final FormOperationPlanner planner;
    final FormResultDecoder results;
    final ProtectedContainsResultSupport containsResults;
    final FieldUsePolicy fieldUsePolicy;
    final QueryShapeLimits queryShapeLimits;
    final boolean governed;

    final ReactiveFormBatchInsertOperations batchInserts;
    final ReactiveFormBatchUpdateOperations batchUpdates;
    private final FormConfiguration configuration;
    private final JoinQueryPlanner joinPlanner;
    private final FormAggregatePlanner aggregatePlanner;

    ReactiveFormOperations(ReactiveSqlExecutor executor, FormConfiguration configuration) {
        this.executor = Objects.requireNonNull(executor, "reactive sql executor must not be null");
        this.configuration = Objects.requireNonNull(configuration, "form configuration must not be null");
        this.renderer = configuration.renderer();
        SqlExecutionOptions defaultExecutionOptions = configuration.executionOptions();
        this.defaultBatchWriteOptions = configuration.batchOptions();
        this.fieldUsePolicy = configuration.fieldUsePolicy();
        this.queryShapeLimits = configuration.queryShapeLimits();
        this.governed = configuration.governed();
        this.scopes = new FormScopeSupport(renderer, configuration.resolver(), configuration.dataScope());
        this.planner = new FormOperationPlanner(renderer, scopes, defaultExecutionOptions);
        this.results = new FormResultDecoder(renderer, configuration.entityModels());
        this.containsResults = new ProtectedContainsResultSupport(renderer);
        this.joinPlanner = planner.reads.joins;
        this.aggregatePlanner = new FormAggregatePlanner(
                renderer, scopes,
                defaultExecutionOptions, fieldUsePolicy, queryShapeLimits);
        this.batchInserts = new ReactiveFormBatchInsertOperations(this);
        this.batchUpdates = new ReactiveFormBatchUpdateOperations(this);
    }

    Flux<DynamicRow> selectJoin(JoinQuerySpec spec, com.flying.orm.rdb.execution.SqlExecutionOptions options) {
        JoinQuerySpec safeSpec = Objects.requireNonNull(spec, "join query spec must not be null");
        if (governed) {
            return requiresProtectedPlanning(safeSpec)
                    ? ReactiveProtectionCpuBoundary.plan(() -> joinPlanner.planGoverned(
                            safeSpec, options, fieldUsePolicy, queryShapeLimits))
                                                   .flatMapMany(this::select)
                    : select(joinPlanner.planGoverned(
                            safeSpec, options, fieldUsePolicy, queryShapeLimits));
        }
        return requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(() -> joinPlanner.plan(safeSpec, options))
                                               .flatMapMany(this::select)
                : select(joinPlanner.plan(safeSpec, options));
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
                                                   .flatMap(plan -> page(plan, FormResultDecoder.IDENTITY))
                    : page(joinPlanner.pageGoverned(
                            safeSpec, safePage, options, fieldUsePolicy, queryShapeLimits), FormResultDecoder.IDENTITY);
        }
        return requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(
                        () -> joinPlanner.page(safeSpec, safePage, options))
                                               .flatMap(plan -> page(plan, FormResultDecoder.IDENTITY))
                : page(joinPlanner.page(safeSpec, safePage, options), FormResultDecoder.IDENTITY);
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
        if (plan.options().maxResultBytes() == 0L) {
            return Flux.defer(() -> executor.query(plan.request(), plan.options())).map(aggregateDecoder::decode);
        }
        return Flux.defer(() -> {
            FormResultDecoder.DecodedResultBudget budget =
                    new FormResultDecoder.DecodedResultBudget(plan.options().maxResultBytes());
            return executor.query(plan.request(), plan.options()).map(row -> budget.accept(aggregateDecoder.decode(row)));
        });
    }

    Mono<PageResult<AggregateRow>> aggregatePage(AggregateSpec spec, PageQuery page) {
        AggregateSpec safeSpec = Objects.requireNonNull(spec, "aggregate spec must not be null");
        PageQuery safePage = Objects.requireNonNull(page, "aggregate page must not be null");
        Mono<FormAggregatePlanner.Plan> plan = requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(() -> aggregatePlanner.planPage(
                        safeSpec, safePage, configuration.explicitGovernance()))
                : Mono.fromSupplier(() -> aggregatePlanner.planPage(
                        safeSpec, safePage, configuration.explicitGovernance()));
        return plan.flatMap(compiled -> executor.query(compiled.countRequest().orElseThrow(), compiled.options())
                .map(RowMapper.count()::map).singleOrEmpty().defaultIfEmpty(0L)
                .flatMap(total -> total == 0L ? Mono.just(PageResult.of(List.of(), total, safePage))
                        : executeAggregate(compiled).collectList().map(rows -> PageResult.of(rows, total, safePage))));
    }

    private FormAggregatePlanner.Plan aggregatePlan(AggregateSpec spec) {
        return aggregatePlanner.plan(spec, configuration.explicitGovernance());
    }

    Flux<DynamicRow> selectSpec(QuerySpec spec) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        if (governed) {
            return selectSpecGoverned(safeSpec, fieldUsePolicy, queryShapeLimits);
        }
        JoinQuerySpec related = joinPlanner.relatedQuery(safeSpec, safeSpec.page().orElse(null));
        return requiresProtectedPlanning(safeSpec, related)
                ? ReactiveProtectionCpuBoundary.plan(() -> planner.reads.select(safeSpec, related, false)).flatMapMany(this::select)
                : select(planner.reads.select(safeSpec, related, false));
    }

    Mono<DynamicRow> firstSpec(QuerySpec spec) {
        // 保留首条但等待执行器完成；next() 会提前取消并隐藏首行后的收尾异常。
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        JoinQuerySpec related = joinPlanner.relatedQuery(safeSpec, null);
        if (governed) {
            return (requiresProtectedPlanning(safeSpec, related)
                    ? ReactiveProtectionCpuBoundary.plan(
                            () -> planner.reads.selectGoverned(safeSpec, related, fieldUsePolicy, queryShapeLimits, true, null))
                            .flatMapMany(this::select)
                    : select(planner.reads.selectGoverned(safeSpec, related, fieldUsePolicy, queryShapeLimits, true, null)))
                    .reduce((first, ignored) -> first);
        }
        return (requiresProtectedPlanning(safeSpec, related)
                ? ReactiveProtectionCpuBoundary.plan(() -> planner.reads.select(safeSpec, related, true)).flatMapMany(this::select)
                : select(planner.reads.select(safeSpec, related, true))).reduce((first, ignored) -> first);
    }

    Mono<Long> countSpec(QuerySpec spec) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        JoinQuerySpec related = joinPlanner.relatedQuery(safeSpec, null, false);
        return requiresProtectedPlanning(safeSpec, related)
                ? ReactiveProtectionCpuBoundary.plan(() -> planner.reads.count(safeSpec, related, fieldUsePolicy, queryShapeLimits, governed))
                        .flatMap(this::count)
                : count(planner.reads.count(safeSpec, related, fieldUsePolicy, queryShapeLimits, governed));
    }

    private Mono<Long> count(FormReadPlanner.PlannedCount plan) {
        Flux<DynamicRow> rows = executor.query(plan.request(), plan.options());
        if (plan.containsQuery() == null) {
            return rows.map(RowMapper.count()::map).singleOrEmpty().defaultIfEmpty(0L);
        }
        return results.forContains().decodeRows(plan.form(), rows, plan.options(), plan.scope(),
                        com.flying.orm.core.protection.SensitiveDisplayMode.FULL, List.of(plan.containsQuery().fieldName()))
                .filter(row -> renderer.protection().matchesContains(plan.form(), plan.containsQuery(), row)).count();
    }

    Flux<DynamicRow> selectSpecGoverned(QuerySpec spec,
                                        FieldUsePolicy policy,
                                        QueryShapeLimits limits) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        FieldUsePolicy safePolicy = Objects.requireNonNull(policy, "field use policy must not be null");
        QueryShapeLimits safeLimits = Objects.requireNonNull(limits, "query shape limits must not be null");
        JoinQuerySpec related = joinPlanner.relatedQuery(safeSpec, safeSpec.page().orElse(null));
        return requiresProtectedPlanning(safeSpec, related)
                ? ReactiveProtectionCpuBoundary.plan(
                        () -> planner.reads.selectGoverned(safeSpec, related, safePolicy, safeLimits, false, null))
                                               .flatMapMany(this::select)
                : select(planner.reads.selectGoverned(safeSpec, related, safePolicy, safeLimits, false, null));
    }

    Flux<DynamicRow> lockingReadSpec(QuerySpec spec) {
        QuerySpec safeSpec = Objects.requireNonNull(
                spec, "locking read spec must not be null");
        safeSpec.lock().orElseThrow();
        return Flux.defer(() -> selectSpec(safeSpec));
    }

    FieldUseSnapshot previewFieldUse(QuerySpec spec) {
        return governed
                ? planner.reads.selectGoverned(spec, fieldUsePolicy, queryShapeLimits).fieldUse()
                : FieldUseSnapshot.unrestricted();
    }

    private Flux<DynamicRow> select(GovernedPlanEnvelope<FormReadPlanner.PlannedQuery> envelope) {
        FormReadPlanner.PlannedQuery plan = envelope.plan();
        return select(plan).map(row -> FieldUseGuard.applyVisibility(
                renderer, plan, row, envelope.fieldUse()));
    }

    private Flux<DynamicRow> select(FormReadPlanner.RowReadPlan plan) {
        return select(plan, true);
    }

    private Flux<DynamicRow> select(FormReadPlanner.RowReadPlan plan, boolean window) {
        if (plan.contains()) {
            Flux<DynamicRow> decoded = results.forContains().decodeRows(plan.form(), executor.query(plan.request(), plan.options()),
                            plan.options(), plan.scope(), com.flying.orm.core.protection.SensitiveDisplayMode.FULL,
                            plan.decodingFields(), plan.resultDecodingPlan());
            Flux<DynamicRow> verified = containsResults.finish(plan.form(), plan.containsQuery(), decoded,
                    plan.verifiedFields(), plan.displayMode(), plan.options());
            if (window && plan instanceof FormReadPlanner.PlannedQuery query && query.page() != null) {
                PageQuery page = query.page();
                return Flux.defer(() -> {
                    long[] matched = {0L};
                    return verified.filter(row -> {
                        long index = matched[0]++;
                        return index >= page.offset() && index - page.offset() < page.size();
                    });
                });
            }
            return verified;
        }
        return results.decodeRows(plan, executor.query(plan.request(), plan.options()));
    }

    <T> Mono<PageResult<T>> pageSpec(QuerySpec spec, PageQuery page, RowMapper<T> mapper) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        PageQuery safePage = Objects.requireNonNull(page, "page query must not be null");
        JoinQuerySpec related = joinPlanner.relatedQuery(safeSpec, safePage);
        if (governed) {
            return requiresProtectedPlanning(safeSpec, related)
                    ? ReactiveProtectionCpuBoundary.plan(() -> planner.reads.selectGoverned(
                            safeSpec, related, fieldUsePolicy, queryShapeLimits, false, safePage))
                                                   .flatMap(plan -> page(plan, mapper))
                    : page(planner.reads.selectGoverned(safeSpec, related, fieldUsePolicy, queryShapeLimits, false, safePage), mapper);
        }
        return requiresProtectedPlanning(safeSpec, related)
                ? ReactiveProtectionCpuBoundary.plan(() -> planner.reads.page(safeSpec, safePage, related))
                                               .flatMap(plan -> page(plan, mapper))
                : page(planner.reads.page(safeSpec, safePage, related), mapper);
    }

    private <T> Mono<PageResult<T>> page(
            GovernedPlanEnvelope<FormReadPlanner.PlannedQuery> envelope, RowMapper<T> mapper) {
        return page(envelope.plan(), row -> mapper.map(FieldUseGuard.applyVisibility(
                renderer, envelope.plan(), row, envelope.fieldUse())));
    }

    <T> Mono<KeysetPageResult<T>> keysetPageSpec(QuerySpec spec, KeysetPageQuery page, RowMapper<T> mapper) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        KeysetPageQuery safePage = Objects.requireNonNull(page, "keyset page query must not be null");
        JoinQuerySpec related = joinPlanner.relatedKeysetQuery(safeSpec, safePage);
        if (governed) {
            return requiresProtectedPlanning(safeSpec, related)
                    ? ReactiveProtectionCpuBoundary.plan(() -> planner.reads.keysetPageGoverned(
                            safeSpec, safePage, fieldUsePolicy, queryShapeLimits, related))
                                                   .flatMap(plan -> keysetPage(plan, mapper))
                    : keysetPage(planner.reads.keysetPageGoverned(safeSpec, safePage, fieldUsePolicy, queryShapeLimits, related), mapper);
        }
        return requiresProtectedPlanning(safeSpec, related)
                ? ReactiveProtectionCpuBoundary.plan(() -> planner.reads.keysetPage(safeSpec, safePage, related))
                                               .flatMap(plan -> keysetPage(plan, null, mapper))
                : keysetPage(planner.reads.keysetPage(safeSpec, safePage, related), null, mapper);
    }

    private <T> Mono<KeysetPageResult<T>> keysetPage(
            GovernedPlanEnvelope<FormReadPlanner.PlannedKeysetPage> envelope, RowMapper<T> mapper) {
        return keysetPage(envelope.plan(), envelope.fieldUse(), mapper);
    }

    private <T> Mono<KeysetPageResult<T>> keysetPage(
            FormReadPlanner.PlannedKeysetPage plan,
            FieldUseSnapshot fieldUse, RowMapper<T> mapper) {
        Flux<DynamicRow> decoded;
        if (plan.contains()) {
            decoded = select(plan);
        } else {
            Flux<DynamicRow> physicalRows = executor.query(plan.request(), plan.options());
            Flux<DynamicRow> rowsForDecoding = plan.layout().hasHiddenSelections()
                    ? physicalRows.map(plan.layout()::logicalRowForDecoding) : physicalRows;
            decoded = results.decodeRows(plan, rowsForDecoding).transform(rows -> plan.layout().hasHiddenSelections()
                    ? rows.map(plan.layout()::physicalRowAfterDecoding) : rows);
        }
        return decoded.collectList()
                .map(rows -> plan.layout().finish(
                        rows, plan.page().size(),
                        row -> mapper.map(fieldUse == null ? row
                                : FieldUseGuard.applyVisibility(renderer, plan, row, fieldUse))));
    }

    private <T> Mono<PageResult<T>> page(FormReadPlanner.PlannedQuery plan, RowMapper<T> mapper) {
        if (plan.contains()) {
            return select(plan, false).collectList().map(rows -> {
                int from = (int) Math.min(plan.page().offset(), rows.size());
                int to = (int) Math.min((long) from + plan.page().size(), rows.size());
                return PageResult.of(rows.subList(from, to).stream().map(mapper::map).toList(), rows.size(), plan.page());
            });
        }
        Mono<Long> total = executor.query(plan.countRequest(), plan.options())
                .map(RowMapper.count()::map).singleOrEmpty().defaultIfEmpty(0L);
        return total.flatMap(count -> count == 0L
                ? Mono.just(PageResult.of(List.of(), 0L, plan.page()))
                : select(plan).map(mapper::map).collectList().map(rows -> PageResult.of(rows, count, plan.page())));
    }

    <T> Mono<KeysetPageResult<T>> lockingReadSpec(
            QuerySpec spec,
            KeysetPageQuery page, RowMapper<T> mapper) {
        QuerySpec safeSpec = Objects.requireNonNull(
                spec, "locking read spec must not be null");
        KeysetPageQuery safePage = Objects.requireNonNull(
                page, "keyset page query must not be null");
        return Mono.defer(() -> {
            safeSpec.lock().orElseThrow();
            return keysetPageSpec(safeSpec, safePage, mapper);
        });
    }

    Mono<Long> insertSpec(WriteSpec spec) {
        return writeSpec(Objects.requireNonNull(spec, "insert spec must not be null"), WriteAction.INSERT);
    }

    private Mono<Long> writeSpec(WriteSpec spec, WriteAction action) {
        return requiresProtectedPlanning(spec)
                ? ReactiveProtectionCpuBoundary.plan(() -> planWrite(spec, action)).flatMap(this::write)
                : write(planWrite(spec, action));
    }

    private FormOperationPlanner.PlannedWrite planWrite(WriteSpec spec, WriteAction action) {
        return planner.write(spec, action, governed, fieldUsePolicy, queryShapeLimits);
    }

    private Mono<Long> write(FormOperationPlanner.PlannedWrite plan) {
        if (plan.protectedWriteRequired()) {
            return executor.protectedWrite(plan.protectedWrite(), plan.options())
                           .map(result -> plan.requireSuccess(result.affectedRows()));
        }
        Mono<Long> result = executor.rowsUpdated(plan.request(), plan.options());
        return plan.lock() == null ? result : result.map(plan::requireSuccess);
    }

    Mono<SqlWriteResult> insertReturningKeysSpec(WriteSpec spec) {
        WriteSpec safeSpec = Objects.requireNonNull(spec, "insert spec must not be null");
        return requiresProtectedPlanning(safeSpec)
                ? ReactiveProtectionCpuBoundary.plan(() -> planWrite(safeSpec, WriteAction.INSERT))
                                               .flatMap(this::writeReturningKeys)
                : writeReturningKeys(planWrite(safeSpec, WriteAction.INSERT));
    }

    private Mono<SqlWriteResult> writeReturningKeys(FormOperationPlanner.PlannedWrite plan) {
        if (plan.protectedWriteRequired()) {
            return executor.protectedWrite(plan.protectedWrite(), plan.options());
        }
        return plan.generatedKeyColumn()
                   .map(column -> executor.rowsUpdatedReturningKeys(plan.request(), plan.options(), column))
                   .orElseGet(() -> executor.rowsUpdatedReturningKeys(plan.request(), plan.options()));
    }

    Mono<Long> updateSpec(WriteSpec spec) {
        return writeSpec(Objects.requireNonNull(spec, "update spec must not be null"), WriteAction.UPDATE);
    }

    Mono<Long> deleteSpec(WriteSpec spec) {
        return writeSpec(Objects.requireNonNull(spec, "delete spec must not be null"), WriteAction.DELETE);
    }

    Mono<Long> physicalDeleteSpec(WriteSpec spec) {
        return writeSpec(Objects.requireNonNull(spec, "physical delete spec must not be null"), WriteAction.PHYSICAL_DELETE);
    }

    Mono<BatchExecutionEvidence> writeBatchSpec(BatchSpec spec, java.util.function.LongFunction<? extends Publisher<Void>> rowCompleted) {
        BatchSpec safeSpec = Objects.requireNonNull(spec, "batch spec must not be null");
        BatchWriteOptions options = safeSpec.options().orElse(defaultBatchWriteOptions);
        return switch (safeSpec.operation()) {
            case INSERT, UPSERT -> batchInserts.writeBatch(safeSpec, mapRows(safeSpec), options, rowCompleted);
            case UPDATE -> batchUpdates.updateBatch(safeSpec.form(), updateRows(safeSpec), safeSpec.scope(), options,
                                                     rowCompleted);
        };
    }

    private boolean requiresProtectedPlanning(QuerySpec spec, JoinQuerySpec related) {
        return related == null ? requiresProtectedPlanning(spec) : requiresProtectedPlanning(related);
    }

    private boolean requiresProtectedPlanning(JoinQuerySpec spec) {
        boolean hasEncryptedFields = false;
        for (JoinSource source : spec.sources()) {
            DynamicForm form = source.form();
            if (form.protections().encryptedFields().isEmpty()) {
                continue;
            }
            hasEncryptedFields = true;
            DataScope effectiveScope = scopes.effectiveScope(spec.scope(source));
            if (ReactiveProtectionCpuBoundary.usesEncryptedScope(form, effectiveScope)) {
                return true;
            }
        }
        return hasEncryptedFields && (ReactiveProtectionCpuBoundary.usesEncryptedCondition(spec.where())
                || spec.joins().stream().anyMatch(join ->
                        ReactiveProtectionCpuBoundary.usesEncryptedCondition(join.condition())));
    }

    private boolean requiresProtectedPlanning(AggregateSpec spec) {
        if (spec.joinQuery().isPresent()) {
            return requiresProtectedPlanning(spec.joinQuery().orElseThrow());
        }
        QuerySpec query = spec.query();
        if (query.form().protections().encryptedFields().isEmpty()) {
            return false;
        }
        DataScope effectiveScope = configuration.dataScope().and(query.scope());
        return ReactiveProtectionCpuBoundary.usesEncryptedCondition(query.form(), query.where())
                || ReactiveProtectionCpuBoundary.usesEncryptedScope(query.form(), effectiveScope)
                || query.structuredInput().isPresent();
    }

    private boolean requiresProtectedPlanning(QuerySpec spec) {
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

    private static Publisher<BatchUpdate> updateRows(BatchSpec spec) {
        return Flux.from(spec.rows()).map(row -> {
            if (!(row instanceof BatchUpdate update)) {
                throw new IllegalArgumentException("update batch rows must be BatchUpdate values");
            }
            return update;
        });
    }
}
