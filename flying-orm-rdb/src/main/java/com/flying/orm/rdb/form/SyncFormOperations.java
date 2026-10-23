package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetPageResult;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldUseSnapshot;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.aggregate.AggregateRow;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.form.FormOperationPlanner.WriteAction;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * 原生 JDBC 表单单条操作执行边界。
 *
 * <p>安全条件和 SQL 全部由 {@link FormOperationPlanner} 生成，本类只把计划交给同步执行器并整理结果。
 * 它不引用 Reactor，也不从响应式客户端等待结果。</p>
 */
final class SyncFormOperations {

    final FormScopeSupport scopes;
    private final SyncSqlExecutor executor;
    private final FormOperationPlanner planner;
    private final JoinQueryPlanner joinPlanner;
    private final FormAggregatePlanner aggregatePlanner;
    private final FormResultDecoder decoder;
    private final ProtectedContainsResultSupport containsResults;
    private final FormDataSqlRenderer renderer;
    private final FieldUsePolicy fieldUsePolicy;
    private final QueryShapeLimits queryShapeLimits;
    private final boolean governed;
    private final boolean explicitGovernance;

    SyncFormOperations(SyncSqlExecutor executor, FormConfiguration configuration) {
        this.executor = Objects.requireNonNull(executor, "sync sql executor must not be null");
        this.renderer = configuration.renderer();
        this.fieldUsePolicy = configuration.fieldUsePolicy();
        this.queryShapeLimits = configuration.queryShapeLimits();
        this.governed = configuration.governed();
        this.explicitGovernance = configuration.explicitGovernance();
        this.scopes = new FormScopeSupport(renderer, configuration.resolver(), configuration.dataScope());
        this.planner = new FormOperationPlanner(renderer, scopes, configuration.executionOptions());
        this.joinPlanner = planner.reads.joins;
        this.aggregatePlanner = new FormAggregatePlanner(
                renderer, scopes,
                configuration.executionOptions(), fieldUsePolicy, queryShapeLimits);
        this.containsResults = new ProtectedContainsResultSupport(renderer);
        this.decoder = new FormResultDecoder(renderer, configuration.entityModels());
    }

    List<DynamicRow> selectJoin(JoinQuerySpec spec, SqlExecutionOptions options) {
        return governed ? select(joinPlanner.planGoverned(spec, options, fieldUsePolicy, queryShapeLimits))
                : select(joinPlanner.plan(spec, options));
    }

    FieldUseSnapshot previewFieldUse(JoinQuerySpec spec) {
        return governed
                ? joinPlanner.planGoverned(spec, null, fieldUsePolicy, queryShapeLimits).fieldUse()
                : FieldUseSnapshot.unrestricted();
    }

    <T> List<T> selectJoin(JoinQuerySpec spec,
                           SqlExecutionOptions options,
                           RowMapper<T> mapper) {
        RowMapper<T> safeMapper = Objects.requireNonNull(mapper, "join row mapper must not be null");
        return governed ? selectMapped(joinPlanner.planGoverned(spec, options, fieldUsePolicy, queryShapeLimits), safeMapper, 0)
                : selectMapped(joinPlanner.plan(spec, options), safeMapper, 0);
    }

    PageResult<DynamicRow> pageJoin(JoinQuerySpec spec,
                                    PageQuery page,
                                    SqlExecutionOptions options) {
        GovernedPlanEnvelope<FormReadPlanner.PlannedQuery> envelope = governed
                ? joinPlanner.pageGoverned(spec, page, options, fieldUsePolicy, queryShapeLimits)
                : null;
        FormReadPlanner.PlannedQuery plan = governed
                ? envelope.plan() : joinPlanner.page(spec, page, options);
        return page(plan, envelope == null ? null : envelope.fieldUse(), FormResultDecoder.IDENTITY);
    }

    List<DynamicRow> select(QuerySpec spec) {
        return governed ? selectGoverned(spec, fieldUsePolicy, queryShapeLimits)
                : select(planner.reads.select(spec));
    }

    List<DynamicRow> selectGoverned(QuerySpec spec,
                                    FieldUsePolicy policy,
                                    QueryShapeLimits limits) {
        return select(planner.reads.selectGoverned(
                Objects.requireNonNull(spec, "query spec must not be null"),
                Objects.requireNonNull(policy, "field use policy must not be null"),
                Objects.requireNonNull(limits, "query shape limits must not be null")));
    }

    List<DynamicRow> lockingRead(QuerySpec spec) {
        if (governed) {
            return select(planner.reads.lockingReadGoverned(spec, fieldUsePolicy, queryShapeLimits));
        }
        return select(planner.reads.lockingRead(spec));
    }

    <T> List<T> lockingRead(QuerySpec spec, Class<T> type) {
        if (governed) {
            return selectMapped(planner.reads.lockingReadGoverned(spec, fieldUsePolicy, queryShapeLimits),
                    decoder.rowMapper(type, "form result type must not be null"), 0);
        }
        return selectMapped(planner.reads.lockingRead(spec), decoder.rowMapper(type, "form result type must not be null"), 0);
    }

    FieldUseSnapshot previewFieldUse(QuerySpec spec) {
        return governed
                ? planner.reads.selectGoverned(spec, fieldUsePolicy, queryShapeLimits).fieldUse()
                : FieldUseSnapshot.unrestricted();
    }

    FieldUseSnapshot previewFieldUse(AggregateSpec spec) {
        return aggregatePlan(spec).fieldUse();
    }

    List<AggregateRow> aggregate(AggregateSpec spec) {
        return aggregate(aggregatePlan(spec));
    }

    PageResult<AggregateRow> aggregatePage(AggregateSpec spec, PageQuery page) {
        FormAggregatePlanner.Plan plan = aggregatePlanner.planPage(spec, page, explicitGovernance);
        List<DynamicRow> countRows = executor.query(plan.countRequest().orElseThrow(), plan.options());
        long total = countRows.isEmpty() ? 0L : RowMapper.count().map(countRows.getFirst());
        return PageResult.of(total == 0L ? List.of() : aggregate(plan), total, page);
    }

    private List<AggregateRow> aggregate(FormAggregatePlanner.Plan plan) {
        AggregateResultDecoder aggregateDecoder = new AggregateResultDecoder(plan);
        if (plan.options().maxResultBytes() == 0L) {
            return executor.queryMapped(plan.request(), plan.options(), aggregateDecoder, 0);
        }
        FormResultDecoder.DecodedResultBudget budget =
                new FormResultDecoder.DecodedResultBudget(plan.options().maxResultBytes());
        return executor.queryMapped(
                plan.request(), plan.options(), row -> budget.accept(aggregateDecoder.decode(row)), 0);
    }

    private List<DynamicRow> select(GovernedPlanEnvelope<FormReadPlanner.PlannedQuery> envelope) {
        FormReadPlanner.PlannedQuery plan = envelope.plan();
        if (!plan.contains()) {
            return executor.queryMapped(
                    plan.request(), plan.options(), governedRowMapper(envelope), 0);
        }
        return selectContains(plan, envelope.fieldUse(), 0);
    }

    private List<DynamicRow> select(FormReadPlanner.RowReadPlan plan) {
        return select(plan, true);
    }

    private List<DynamicRow> select(FormReadPlanner.RowReadPlan plan, boolean window) {
        if (plan.contains()) {
            return selectContains(plan, null, 0,
                    window && plan instanceof FormReadPlanner.PlannedQuery query ? query.page() : null);
        }
        return queryDecoded(plan.request(), plan.options(), decoder.rowDecoder(plan));
    }

    private List<DynamicRow> queryDecoded(SqlRequest request, SqlExecutionOptions options,
                                          RowMapper<DynamicRow> mapper) {
        return mapper == FormResultDecoder.IDENTITY ? executor.query(request, options)
                : executor.queryMapped(request, options, mapper, 0);
    }

    <T> List<T> select(QuerySpec spec, Class<T> type) {
        return selectMapped(spec, decoder.rowMapper(type, "form result type must not be null"), 0);
    }

    <T> long forEach(QuerySpec spec, Class<T> type, Consumer<? super T> consumer) {
        return forEach(spec, decoder.rowMapper(type, "form result type must not be null"), consumer);
    }

    <T> long forEach(QuerySpec spec, RowMapper<T> mapper, Consumer<? super T> consumer) {
        Consumer<? super T> safeConsumer = Objects.requireNonNull(consumer, "row consumer must not be null");
        return consume(spec, mapper, row -> { safeConsumer.accept(row); return true; });
    }

    <T> long forEachWhile(QuerySpec spec, Class<T> type, Predicate<? super T> consumer) {
        return forEachWhile(spec, decoder.rowMapper(type, "form result type must not be null"), consumer);
    }

    <T> long forEachWhile(QuerySpec spec, RowMapper<T> mapper, Predicate<? super T> consumer) {
        return consume(spec, mapper, consumer);
    }

    private <T> long consume(QuerySpec spec, RowMapper<T> mapper, Predicate<? super T> consumer) {
        RowMapper<T> safeMapper = Objects.requireNonNull(mapper, "row mapper must not be null");
        Predicate<? super T> safeConsumer = Objects.requireNonNull(consumer, "row consumer must not be null");
        GovernedPlanEnvelope<FormReadPlanner.PlannedQuery> envelope = governed
                ? planner.reads.selectGoverned(spec, fieldUsePolicy, queryShapeLimits) : null;
        FormReadPlanner.PlannedQuery plan = envelope == null ? planner.reads.select(spec) : envelope.plan();
        if (!plan.contains() && envelope == null) {
            return executor.queryWhile(plan.request(), plan.options(), decoder.rowMapper(plan, safeMapper), safeConsumer);
        }
        RowMapper<DynamicRow> rowDecoder = plan.contains()
                ? decoder.forContains().rowDecoder(plan.form(), plan.options(), plan.scope(),
                        SensitiveDisplayMode.FULL, plan.resultDecodingPlan())
                : decoder.rowDecoder(plan);
        UnaryOperator<DynamicRow> finish = plan.contains()
                ? containsResults.rowFinisher(plan.form(), plan.containsQuery(), plan.outputFields(),
                        plan.displayMode(), plan.options()) : UnaryOperator.identity();
        long[] consumed = {0L};
        PageQuery window = plan.contains() ? plan.page() : null;
        long[] matched = window == null ? null : new long[1];
        Predicate<DynamicRow> visit = raw -> {
            DynamicRow row = finish.apply(rowDecoder.map(raw));
            if (row == null) return true;
            if (window != null) {
                long index = matched[0]++;
                if (index < window.offset() || index - window.offset() >= window.size()) return true;
            }
            if (envelope != null) {
                row = FieldUseGuard.applyVisibility(renderer, plan, row, envelope.fieldUse());
            }
            boolean more = safeConsumer.test(safeMapper.map(row));
            consumed[0]++;
            return more;
        };
        executor.queryWhile(plan.request(), plan.options(), visit);
        return consumed[0];
    }

    <T> T selectOne(QuerySpec spec, Class<T> type) {
        List<T> rows = selectMapped(spec, decoder.rowMapper(type, "form result type must not be null"), 2);
        if (rows.isEmpty()) {
            return null;
        }
        if (rows.size() != 1) {
            throw new IllegalStateException("entity query expected zero or one row but received " + rows.size());
        }
        return rows.getFirst();
    }

    <T> T selectFirst(QuerySpec spec, RowMapper<T> mapper) {
        List<T> rows = governed
                ? selectMapped(planner.reads.firstGoverned(spec, fieldUsePolicy, queryShapeLimits), mapper, 1)
                : selectMapped(planner.reads.first(spec), mapper, 1);
        return rows.isEmpty() ? null : rows.getFirst();
    }

    <T> T selectFirst(QuerySpec spec, Class<T> type) {
        return selectFirst(spec, decoder.rowMapper(type, "form result type must not be null"));
    }

    long count(QuerySpec spec) {
        FormReadPlanner.PlannedCount plan = planner.reads.count(spec, fieldUsePolicy, queryShapeLimits, governed);
        if (plan.containsQuery() == null) {
            List<DynamicRow> rows = executor.query(plan.request(), plan.options());
            return rows.isEmpty() ? 0L : RowMapper.count().map(rows.getFirst());
        }
        RowMapper<DynamicRow> verification = decoder.forContains().rowDecoder(plan.form(), plan.options(), plan.scope(),
                com.flying.orm.core.protection.SensitiveDisplayMode.FULL);
        long[] matches = {0L};
        executor.queryEach(plan.request(), plan.options(), row -> {
            if (renderer.protection().matchesContains(plan.form(), plan.containsQuery(), verification.map(row))) {
                matches[0]++;
            }
        });
        return matches[0];
    }

    <T> List<T> selectMapped(QuerySpec spec, RowMapper<T> mapper, int rowLimit) {
        return governed
                ? selectMapped(planner.reads.selectGoverned(spec, fieldUsePolicy, queryShapeLimits), mapper, rowLimit)
                : selectMapped(planner.reads.select(spec), mapper, rowLimit);
    }

    PageResult<DynamicRow> page(QuerySpec spec, PageQuery page) {
        return page(spec, page, FormResultDecoder.IDENTITY);
    }

    private <T> PageResult<T> page(QuerySpec spec, PageQuery page, RowMapper<T> mapper) {
        GovernedPlanEnvelope<FormReadPlanner.PlannedQuery> envelope = governed
                ? planner.reads.pageGoverned(spec, page, fieldUsePolicy, queryShapeLimits) : null;
        FormReadPlanner.PlannedQuery plan = envelope == null
                ? planner.reads.page(spec, page) : envelope.plan();
        return page(plan, envelope == null ? null : envelope.fieldUse(), mapper);
    }

    private <T> PageResult<T> page(FormReadPlanner.PlannedQuery plan, FieldUseSnapshot fieldUse, RowMapper<T> mapper) {
        RowMapper<T> published = fieldUse == null ? mapper : row -> mapper.map(
                FieldUseGuard.applyVisibility(renderer, plan, row, fieldUse));
        if (plan.contains()) {
            List<DynamicRow> rows = select(plan, false);
            int from = (int) Math.min(plan.page().offset(), rows.size());
            int to = (int) Math.min((long) from + plan.page().size(), rows.size());
            return PageResult.of(rows.subList(from, to).stream().map(published::map).toList(),
                    rows.size(), plan.page());
        }
        List<DynamicRow> countRows = executor.query(plan.countRequest(), plan.options());
        long total = countRows.isEmpty() ? 0L : RowMapper.count().map(countRows.getFirst());
        return PageResult.of(total == 0L ? List.of() : selectMapped(plan, published, 0), total, plan.page());
    }

    <T> PageResult<T> page(QuerySpec spec, PageQuery page, Class<T> type) {
        RowMapper<T> mapper = decoder.rowMapper(type, "page result type must not be null");
        return page(spec, page, mapper);
    }

    KeysetPageResult<DynamicRow> keysetPage(QuerySpec spec, KeysetPageQuery page) {
        return keysetPage(spec, page, FormResultDecoder.IDENTITY);
    }

    private <T> KeysetPageResult<T> keysetPage(QuerySpec spec, KeysetPageQuery page, RowMapper<T> mapper) {
        if (governed) {
            GovernedPlanEnvelope<FormReadPlanner.PlannedKeysetPage> envelope =
                    planner.reads.keysetPageGoverned(spec, page, fieldUsePolicy, queryShapeLimits);
            return keysetPage(envelope.plan(), envelope.fieldUse(), mapper);
        }
        return keysetPage(planner.reads.keysetPage(spec, page), null, mapper);
    }

    KeysetPageResult<DynamicRow> lockingRead(
            QuerySpec spec,
            KeysetPageQuery page) {
        return lockingRead(spec, page, FormResultDecoder.IDENTITY);
    }

    private <T> KeysetPageResult<T> lockingRead(QuerySpec spec, KeysetPageQuery page, RowMapper<T> mapper) {
        Objects.requireNonNull(spec, "locking read spec must not be null").lock().orElseThrow();
        return keysetPage(spec, page, mapper);
    }

    <T> KeysetPageResult<T> lockingRead(
            QuerySpec spec,
            KeysetPageQuery page,
            Class<T> type) {
        RowMapper<T> mapper = decoder.rowMapper(
                type, "locking keyset result type must not be null");
        return lockingRead(spec, page, mapper);
    }

    private <T> KeysetPageResult<T> keysetPage(
            FormReadPlanner.PlannedKeysetPage plan,
            FieldUseSnapshot fieldUse, RowMapper<T> publisher) {
        List<DynamicRow> rows;
        if (plan.contains()) {
            rows = select(plan);
        } else {
            RowMapper<DynamicRow> rowDecoder = decoder.rowDecoder(plan);
            RowMapper<DynamicRow> mapper = plan.layout().hasHiddenSelections()
                    ? row -> plan.layout().physicalRowAfterDecoding(rowDecoder.map(plan.layout().logicalRowForDecoding(row)))
                    : rowDecoder;
            rows = queryDecoded(plan.request(), plan.options(), mapper);
        }
        return plan.layout().finish(rows, plan.page().size(), row -> publisher.map(fieldUse == null ? row
                : FieldUseGuard.applyVisibility(renderer, plan, row, fieldUse)));
    }

    <T> KeysetPageResult<T> keysetPage(
            QuerySpec spec, KeysetPageQuery page, Class<T> type) {
        RowMapper<T> mapper = decoder.rowMapper(type, "keyset page result type must not be null");
        return keysetPage(spec, page, mapper);
    }

    long insert(WriteSpec spec) {
        return executeWrite(planWrite(spec, WriteAction.INSERT));
    }

    /** 生成键和影响行数必须来自同一个 JDBC Statement，不能在 insert 后另查当前序列值。 */
    SqlWriteResult insertReturningKeys(WriteSpec spec) {
        FormOperationPlanner.PlannedWrite plan = planWrite(spec, WriteAction.INSERT);
        SqlWriteResult result = plan.protectedWriteRequired()
                ? executor.protectedWrite(plan.protectedWrite(), plan.options())
                : rowsUpdatedReturningKeys(plan);
        plan.requireSuccess(result.affectedRows());
        return result;
    }

    long update(WriteSpec spec) {
        return executeWrite(planWrite(spec, WriteAction.UPDATE));
    }

    long delete(WriteSpec spec) {
        return executeWrite(planWrite(spec, WriteAction.DELETE));
    }

    long physicalDelete(WriteSpec spec) {
        return executeWrite(planWrite(spec, WriteAction.PHYSICAL_DELETE));
    }

    private FormAggregatePlanner.Plan aggregatePlan(AggregateSpec spec) {
        return aggregatePlanner.plan(
                Objects.requireNonNull(spec, "aggregate spec must not be null"), explicitGovernance);
    }

    private SqlWriteResult rowsUpdatedReturningKeys(FormOperationPlanner.PlannedWrite plan) {
        return plan.generatedKeyColumn()
                   .map(column -> executor.rowsUpdatedReturningKeys(
                           plan.request(), plan.options(), column))
                   .orElseGet(() -> executor.rowsUpdatedReturningKeys(
                           plan.request(), plan.options()));
    }

    private FormOperationPlanner.PlannedWrite planWrite(WriteSpec spec, WriteAction action) {
        return planner.write(spec, action, governed, fieldUsePolicy, queryShapeLimits);
    }

    private long executeWrite(FormOperationPlanner.PlannedWrite plan) {
        long affectedRows = plan.protectedWriteRequired()
                ? executor.protectedWrite(plan.protectedWrite(), plan.options()).affectedRows()
                : executor.rowsUpdated(plan.request(), plan.options());
        return plan.requireSuccess(affectedRows);
    }

    private <T> List<T> selectMapped(FormReadPlanner.PlannedQuery plan, RowMapper<T> entityMapper, int rowLimit) {
        if (plan.contains()) {
            return selectContains(plan, null, rowLimit).stream().map(entityMapper::map).toList();
        }
        return executor.queryMapped(
                plan.request(), plan.options(), decoder.rowMapper(plan, entityMapper), rowLimit);
    }

    /** 逐行核验全部候选以保留晚到失败，只收集终端所需的匹配行；实体映射仍在资源释放后执行。 */
    private List<DynamicRow> selectContains(FormReadPlanner.RowReadPlan plan, FieldUseSnapshot fieldUse, int rowLimit) {
        return selectContains(plan, fieldUse, rowLimit,
                plan instanceof FormReadPlanner.PlannedQuery query ? query.page() : null);
    }

    private List<DynamicRow> selectContains(FormReadPlanner.RowReadPlan plan, FieldUseSnapshot fieldUse,
                                            int rowLimit, PageQuery window) {
        RowMapper<DynamicRow> verification = decoder.forContains().rowDecoder(
                plan.form(), plan.options(), plan.scope(), SensitiveDisplayMode.FULL, plan.resultDecodingPlan());
        UnaryOperator<DynamicRow> finish = containsResults.rowFinisher(
                plan.form(), plan.containsQuery(), plan.verifiedFields(), plan.displayMode(), plan.options());
        List<DynamicRow> rows = new ArrayList<>();
        long[] matched = {0L};
        executor.queryEach(plan.request(), plan.options(), source -> {
            DynamicRow row = finish.apply(verification.map(source));
            if (row == null) return;
            if (window != null) {
                long index = matched[0]++;
                if (index < window.offset() || index - window.offset() >= window.size()) return;
            }
            if (fieldUse != null) row = FieldUseGuard.applyVisibility(renderer, plan, row, fieldUse);
            if (rowLimit == 0 || rows.size() < rowLimit) rows.add(row);
        });
        return Collections.unmodifiableList(rows);
    }

    private RowMapper<DynamicRow> governedRowMapper(
            GovernedPlanEnvelope<FormReadPlanner.PlannedQuery> envelope) {
        FormReadPlanner.PlannedQuery plan = envelope.plan();
        RowMapper<DynamicRow> rowDecoder = decoder.rowDecoder(plan);
        return row -> FieldUseGuard.applyVisibility(
                renderer, plan, rowDecoder.map(row), envelope.fieldUse());
    }

    private <T> List<T> selectMapped(
            GovernedPlanEnvelope<FormReadPlanner.PlannedQuery> envelope,
            RowMapper<T> entityMapper,
            int rowLimit) {
        FormReadPlanner.PlannedQuery plan = envelope.plan();
        if (plan.contains()) {
            return selectContains(plan, envelope.fieldUse(), rowLimit).stream().map(entityMapper::map).toList();
        }
        RowMapper<DynamicRow> rowMapper = governedRowMapper(envelope);
        return executor.queryMapped(
                plan.request(), plan.options(), row -> entityMapper.map(rowMapper.map(row)), rowLimit);
    }

}
