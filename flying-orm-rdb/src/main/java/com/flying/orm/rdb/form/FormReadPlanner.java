package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.lock.LockingReadDialect;
import com.flying.orm.rdb.lock.ReadLock;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;

import java.util.List;
import java.util.Objects;

/**
 * 表单读取规划所有者；普通查询、分页和锁定读取共享 Scope、保护与投影准备逻辑。
 */
final class FormReadPlanner {

    final FormDataSqlRenderer renderer;
    final FormScopeSupport scopes;
    private final SqlExecutionOptions defaultExecutionOptions;
    final JoinQueryPlanner joins;

    FormReadPlanner(FormDataSqlRenderer renderer,
                    FormScopeSupport scopes,
                    SqlExecutionOptions defaultExecutionOptions) {
        this.renderer = renderer;
        this.scopes = scopes;
        this.defaultExecutionOptions = defaultExecutionOptions;
        this.joins = new JoinQueryPlanner(renderer, scopes, defaultExecutionOptions);
    }

    PlannedQuery select(QuerySpec spec) {
        return select(spec, false);
    }

    PlannedQuery first(QuerySpec spec) {
        return select(spec, true);
    }

    PlannedCount count(QuerySpec spec,
                       FieldUsePolicy policy, QueryShapeLimits limits, boolean governed) {
        return count(spec, joins.relatedQuery(spec, null, false), policy, limits, governed);
    }

    PlannedCount count(QuerySpec spec, JoinQuerySpec related,
                       FieldUsePolicy policy, QueryShapeLimits limits, boolean governed) {
        if (related != null) return joins.count(related, executionOptions(spec), policy, limits, governed);
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        FormScopeSupport.GovernedRead approval = governed ? scopes.governedRead(safeSpec) : null;
        ScopedRead read = approval == null ? scopes.scopedRead(safeSpec) : approval.read();
        List<String> groups = FormQueryShapeGuard.readableGroups(safeSpec, read.form());
        ProtectedFieldRuntime.PreparedContainsQuery contains = renderer.protection()
                .prepareContainsQuery(safeSpec.form(), read.form(), read.where(), read.scope()).orElse(null);
        SqlRequest request;
        if (contains != null) {
            FormQueryShapeGuard.requireContainsShape(safeSpec);
            request = renderer.protection().contains.verificationRows(contains);
        } else {
            ProtectedFieldRuntime.PreparedQuery query = renderer.protection()
                    .prepareQuery(safeSpec.form(), read.where(), read.scope(), List.of());
            request = renderer.queryRenderer.count(query.physicalForm(), query.where(), groups);
        }
        PlannedCount plan = new PlannedCount(
                safeSpec.form(), request, executionOptions(safeSpec), read.scope(), contains);
        if (governed) {
            QueryShapeBudget budget = new QueryShapeBudget(limits);
            accountReadShape(budget, 1, groups.size(), 0);
            FieldUseGuard.query(plan, renderer, safeSpec, approval.businessWhere(), List.of(), List.of(),
                    0, false, read.scope(), request, policy, budget);
        }
        return plan;
    }

    private PlannedQuery select(
            QuerySpec spec, boolean first) {
        return select(spec, joins.relatedQuery(spec, first ? null : spec.page().orElse(null)), first);
    }

    PlannedQuery select(QuerySpec spec, JoinQuerySpec related, boolean first) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        ReadLock lock = readLock(safeSpec, first, first ? null : spec.page().orElse(null));
        if (related != null) return joins.plan(related, executionOptions(spec),
                first ? null : spec.page().orElse(null), first, false, lock);
        return select(safeSpec, scopes.scopedRead(safeSpec), safeSpec.sensitiveDisplayMode(), null, first,
                first ? null : safeSpec.page().orElse(null), false);
    }

    GovernedPlanEnvelope<PlannedQuery> selectGoverned(
            QuerySpec spec,
            FieldUsePolicy policy,
            QueryShapeLimits limits) {
        return selectGoverned(spec, policy, limits, false, null);
    }

    GovernedPlanEnvelope<PlannedQuery> firstGoverned(
            QuerySpec spec, FieldUsePolicy policy, QueryShapeLimits limits) {
        return selectGoverned(spec, policy, limits, true, null);
    }

    private GovernedPlanEnvelope<PlannedQuery> selectGoverned(
            QuerySpec spec, FieldUsePolicy policy, QueryShapeLimits limits, boolean first, PageQuery page) {
        return selectGoverned(spec, joins.relatedQuery(spec, page == null ? spec.page().orElse(null) : page),
                policy, limits, first, page);
    }

    GovernedPlanEnvelope<PlannedQuery> selectGoverned(QuerySpec spec, JoinQuerySpec related,
            FieldUsePolicy policy, QueryShapeLimits limits, boolean first, PageQuery page) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        ReadLock lock = readLock(safeSpec, first, first ? null : page == null ? spec.page().orElse(null) : page);
        if (related != null) return joins.planGoverned(related, executionOptions(spec), policy, limits,
                first ? null : page == null ? spec.page().orElse(null) : page, first, page != null, lock);
        FormScopeSupport.GovernedRead governed = scopes.governedRead(safeSpec);
        QueryShapeBudget budget = new QueryShapeBudget(limits);
        PlannedQuery plan = select(safeSpec, governed.read(),
                FieldUseGuard.effectiveDisplayMode(policy, safeSpec.sensitiveDisplayMode()), budget, first,
                first ? null : page == null ? safeSpec.page().orElse(null) : page, page != null);
        if (plan.countRequest() != null) {
            FieldUseGuard.accountRenderedRequest(new QueryShapeBudget(limits), plan.countRequest());
        }
        List<PageSort> sorts = plan.page() == null ? safeSpec.sorts() : plan.page().sorts();
        return FieldUseGuard.query(
                plan, renderer, safeSpec, governed.businessWhere(), plan.outputFields(),
                sorts.stream().map(PageSort::field).toList(), sorts.size(), false,
                plan.scope(), plan.request(), policy, budget);
    }

    PlannedQuery page(
            QuerySpec spec, PageQuery page) {
        return page(spec, page, joins.relatedQuery(spec, page));
    }

    PlannedQuery page(QuerySpec spec, PageQuery page, JoinQuerySpec related) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        PageQuery requested = Objects.requireNonNull(page, "page query must not be null");
        ReadLock lock = readLock(safeSpec, false, requested);
        if (related != null) return joins.plan(related, executionOptions(spec),
                PageQuery.of(requested.page(), requested.size()), false, true, lock);
        return select(safeSpec, scopes.scopedRead(safeSpec), safeSpec.sensitiveDisplayMode(), null, false, requested, true);
    }

    GovernedPlanEnvelope<PlannedQuery> pageGoverned(
            QuerySpec spec,
            PageQuery page,
            FieldUsePolicy policy,
            QueryShapeLimits limits) {
        return selectGoverned(spec, policy, limits, false,
                Objects.requireNonNull(page, "page query must not be null"));
    }

    PlannedQuery lockingRead(
            QuerySpec spec) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "locking read spec must not be null");
        safeSpec.lock().orElseThrow();
        return select(safeSpec);
    }

    GovernedPlanEnvelope<PlannedQuery> lockingReadGoverned(
            QuerySpec spec,
            FieldUsePolicy policy,
            QueryShapeLimits limits) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "locking read spec must not be null");
        safeSpec.lock().orElseThrow();
        return selectGoverned(safeSpec, policy, limits);
    }

    SqlExecutionOptions executionOptions(QuerySpec spec) {
        return spec.executionOptions().orElse(defaultExecutionOptions);
    }

    LockingReadDialect requireLockingDialect(ReadLock lock) {
        LockingReadDialect dialect = renderer.lockingReadDialect();
        if (!dialect.supports(Objects.requireNonNull(lock, "read lock must not be null"))) {
            throw new UnsupportedOperationException(
                    "locking read is not supported by this database descriptor");
        }
        return dialect;
    }

    private ReadLock readLock(QuerySpec spec, boolean first, PageQuery requested) {
        ReadLock lock = spec.lock().orElse(null);
        if (lock != null && requested != null) {
            throw new UnsupportedOperationException("locking offset pagination is not supported; use keyset pagination");
        }
        if (lock != null) {
            LockingReadDialect dialect = requireLockingDialect(lock);
            if (first && !dialect.supportsPagination(lock)) {
                throw new UnsupportedOperationException("locking first read is not supported by this database descriptor");
            }
        }
        if (lock != null && !spec.groups().isEmpty()) {
            throw new UnsupportedOperationException("locking read does not support grouped queries");
        }
        return lock;
    }

    private PlannedQuery select(
            QuerySpec spec,
            ScopedRead read,
            SensitiveDisplayMode displayMode,
            QueryShapeBudget budget,
            boolean first,
            PageQuery requested, boolean withTotal) {
        ReadLock lock = spec.lock().orElse(null);
        ProtectedFieldRuntime.PreparedContainsQuery contains = renderer.protection()
                .prepareContainsQuery(spec.form(), read.form(), read.where(), read.scope()).orElse(null);
        if (contains != null && lock != null) {
            throw new UnsupportedOperationException("locking read does not support protected contains queries");
        }
        List<PageSort> requestedSorts = requested != null && spec.sorts().isEmpty()
                ? requested.sorts() : spec.sorts();
        FormQueryExpressions.Projection projection = projection(
                spec, read, displayMode, contains, requestedSorts);
        List<String> outputFields = outputFields(spec, read, projection);
        List<String> ordinary = ordinaryFields(spec, outputFields);
        List<String> groups = FormQueryShapeGuard.readableGroups(spec, read.form());
        List<PageSort> sorts = querySorts(spec, read, projection, ordinary, groups, requestedSorts);
        PageQuery page = requested == null ? null : new PageQuery(
                requested.page(), requested.size(), projection == null ? sorts : requestedSorts);
        accountReadShape(budget, outputFields.size(), groups.size(), sorts.size());
        SqlRequest count = null;
        SqlRequest request;
        if (contains != null) {
            request = projection == null ? renderer.protection().contains.rows(contains, sorts)
                    : renderer.protection().contains.rows(contains, sorts, projection);
        } else {
            ProtectedFieldRuntime.PreparedQuery query = renderer.protection().prepareQuery(
                    spec.form(), read.where(), read.scope(), ordinary);
            request = projection != null ? renderer.selectExpressions(
                    query.physicalForm(), query.where(), projection, groups, sorts, first, lock)
                    : renderer.queryRenderer.select(query.physicalForm(), query.where(), query.visibleFields(),
                            groups, sorts, page, first, renderer.lockingReadDialect(), lock);
            if (page != null) {
                if (projection != null) {
                    request = renderer.paginate(request, page);
                }
                if (withTotal) count = renderer.queryRenderer.count(query.physicalForm(), query.where(), groups);
            }
        }
        return new PlannedQuery(
                projection == null ? spec.form() : projection.resultForm(), request, executionOptions(spec),
                read.scope(), displayMode, contains, outputFields, projection == null ? null
                        : FormFieldDecodingPlan.queryProjection(spec, projection.resultForm(), renderer, projection.decodingFields()),
                count, page, null, null);
    }

    /** 只有计算列才创建投影元数据，普通读取继续直接使用原表单和字段列表。 */
    private FormQueryExpressions.Projection projection(
            QuerySpec spec, ScopedRead read, SensitiveDisplayMode displayMode,
            ProtectedFieldRuntime.PreparedContainsQuery contains, List<PageSort> sorts) {
        if (contains != null) {
            FormQueryShapeGuard.requireContainsShape(spec);
        }
        if (spec.expressions().isEmpty()) {
            return null;
        }
        QuerySpec ordered = spec.sorts().equals(sorts) ? spec : spec.withSorts(sorts);
        return contains == null ? FormQueryExpressions.prepare(renderer, ordered, read, displayMode)
                : FormQueryExpressions.prepareContains(renderer, ordered, read, displayMode, contains);
    }

    private static List<String> outputFields(
            QuerySpec spec, ScopedRead read, FormQueryExpressions.Projection projection) {
        return projection == null ? FormQueryShapeGuard.readableProjections(spec, read.form()) : projection.outputFields();
    }

    private static List<String> ordinaryFields(QuerySpec spec, List<String> outputFields) {
        return spec.expressions().isEmpty() ? outputFields : outputFields.stream()
                .filter(field -> !spec.expressions().containsKey(FieldIdentity.of(field).key())).toList();
    }

    private static List<PageSort> querySorts(
            QuerySpec spec, ScopedRead read, FormQueryExpressions.Projection projection,
            List<String> ordinary, List<String> groups, List<PageSort> requested) {
        List<PageSort> sorts = projection == null
                ? FormQueryShapeGuard.readableSorts(spec.form(), read.form(), requested)
                : FormQueryShapeGuard.readableExpressionSorts(spec, read.form(), projection.resultForm(), requested);
        if (!groups.isEmpty()) {
            FormQueryShapeGuard.requireValidGrouping(ordinary, groups, projection == null ? sorts : sorts.stream()
                    .filter(sort -> !spec.expressions().containsKey(FieldIdentity.of(sort.field()).key())).toList());
        }
        return sorts;
    }

    private static void accountReadShape(QueryShapeBudget budget,
                                         int projections,
                                         int groups,
                                         int sorts) {
        if (budget == null) {
            return;
        }
        budget.addProjections(projections);
        budget.addGroups(groups);
        budget.addSorts(sorts);
    }

    PlannedKeysetPage keysetPage(QuerySpec spec, KeysetPageQuery page) {
        return keysetPage(spec, page, joins.relatedKeysetQuery(spec, page));
    }

    PlannedKeysetPage keysetPage(QuerySpec spec, KeysetPageQuery page, JoinQuerySpec related) {
        QuerySpec safeSpec = requireKeysetSpec(spec);
        KeysetPageQuery safePage = Objects.requireNonNull(page, "keyset page query must not be null");
        ReadLock lock = safeSpec.lock().orElse(null);
        LockingReadDialect dialect = lock == null ? null : requireLockingDialect(lock);
        if (related != null) return joins.keyset(related, safePage, executionOptions(safeSpec), lock, null, null).plan();
        return keysetPage(safeSpec, safePage, scopes.scopedRead(safeSpec),
                safeSpec.sensitiveDisplayMode(), null, dialect, lock);
    }

    GovernedPlanEnvelope<PlannedKeysetPage> keysetPageGoverned(
            QuerySpec spec,
            KeysetPageQuery page,
            FieldUsePolicy policy,
            QueryShapeLimits limits) {
        return keysetPageGoverned(spec, page, policy, limits, joins.relatedKeysetQuery(spec, page));
    }

    GovernedPlanEnvelope<PlannedKeysetPage> keysetPageGoverned(QuerySpec spec, KeysetPageQuery page,
            FieldUsePolicy policy, QueryShapeLimits limits, JoinQuerySpec related) {
        QuerySpec safeSpec = requireKeysetSpec(spec);
        KeysetPageQuery safePage = Objects.requireNonNull(page, "keyset page query must not be null");
        if (related != null) {
            safeSpec.lock().ifPresent(this::requireLockingDialect);
            return joins.keyset(related, safePage, executionOptions(safeSpec), safeSpec.lock().orElse(null), policy, limits);
        }
        FormScopeSupport.GovernedRead governed = scopes.governedRead(safeSpec);
        QueryShapeBudget budget = new QueryShapeBudget(limits);
        ReadLock lock = safeSpec.lock().orElse(null);
        PlannedKeysetPage plan = keysetPage(safeSpec, safePage, governed.read(),
                FieldUseGuard.effectiveDisplayMode(policy, safeSpec.sensitiveDisplayMode()), budget,
                lock == null ? null : requireLockingDialect(lock), lock);
        return FieldUseGuard.keyset(plan, renderer, safeSpec, governed.businessWhere(), plan.outputFields(),
                plan.page(), plan.scope(), plan.request(), policy, budget);
    }

    private PlannedKeysetPage keysetPage(QuerySpec spec, KeysetPageQuery page, ScopedRead read,
            SensitiveDisplayMode displayMode, QueryShapeBudget budget, LockingReadDialect dialect, ReadLock lock) {
        KeysetPageNormalizer.NormalizedKeysetPage normalized = KeysetPageNormalizer.normalize(spec.form(), page);
        FormQueryShapeGuard.requireReadableUnprotectedKeysetSorts(spec.form(), read.form(), normalized, displayMode);
        ProtectedFieldRuntime.PreparedContainsQuery contains = renderer.protection()
                .prepareContainsQuery(spec.form(), read.form(), read.where(), read.scope()).orElse(null);
        if (contains != null && lock != null) {
            throw new UnsupportedOperationException("locking read does not support protected contains queries");
        }
        FormQueryExpressions.Projection projection = projection(spec, read, displayMode, contains, spec.sorts());
        List<String> output = outputFields(spec, read, projection);
        HiddenProjectionLayout layout = HiddenProjectionLayout.of(output, normalized, contains != null);
        accountReadShape(budget, layout.selections().size(), 0, normalized.sorts().size());
        if (contains != null) {
            return new PlannedKeysetPage(projection == null ? spec.form() : projection.resultForm(),
                    renderer.protection().contains.rows(contains, normalized, projection), normalized, layout,
                    executionOptions(spec), read.scope(), displayMode, output,
                    projection == null ? null : FormFieldDecodingPlan.queryProjection(
                            spec, projection.resultForm(), renderer, projection.decodingFields()), null, null, contains);
        }
        ProtectedFieldRuntime.PreparedQuery query = renderer.protection().prepareQuery(
                spec.form(), read.where(), read.scope(), List.of());
        SqlRequest request = projection != null
                ? renderer.selectKeysetExpressions(query.physicalForm(), query.where(), projection.columns(),
                        layout, normalized, dialect, lock)
                : dialect == null ? renderer.queryRenderer.selectKeyset(
                        query.physicalForm(), query.where(), layout, normalized)
                : renderer.queryRenderer.selectKeysetLocking(
                        query.physicalForm(), query.where(), layout, normalized, dialect, lock);
        return new PlannedKeysetPage(projection == null ? spec.form() : projection.resultForm(), request,
                normalized, layout, executionOptions(spec), read.scope(), displayMode, output, projection == null ? null
                        : FormFieldDecodingPlan.queryProjection(spec, projection.resultForm(), renderer, layout.decodingFields()),
                null, null, null);
    }

    private static QuerySpec requireKeysetSpec(QuerySpec spec) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        FormQueryShapeGuard.requireUngroupedPagination(safeSpec, "keyset pagination");
        if (!safeSpec.sorts().isEmpty()) {
            throw new IllegalArgumentException("keyset pagination sorts must be declared with KeysetPageQuery");
        }
        return safeSpec;
    }

    record PlannedCount(DynamicForm form,
                        SqlRequest request,
                        SqlExecutionOptions options,
                        DataScope scope,
                        ProtectedFieldRuntime.PreparedContainsQuery containsQuery) {
    }

    /** 现有读取计划直接提供行解码信息，不为不同终端再创建结果包装对象。 */
    interface RowReadPlan {
        DynamicForm form();
        SqlRequest request();
        SqlExecutionOptions options();
        DataScope scope();
        SensitiveDisplayMode displayMode();
        ProtectedFieldRuntime.PreparedContainsQuery containsQuery();
        List<String> outputFields();
        FormFieldDecodingPlan resultDecodingPlan();

        /** 关联投影按来源转换；普通读取没有此额外转换。 */
        default JoinResultProtector.ResultPlan joinResultPlan() { return null; }
        default JoinQuerySpec joinSpec() { return null; }

        default boolean contains() {
            return containsQuery() != null;
        }

        /** 精确核验后的内部投影；分页所需隐藏字段在生成位置后才剥离。 */
        default List<String> verifiedFields() { return outputFields(); }

        default List<String> decodingFields() {
            return contains() ? containsQuery().visibleFields() : outputFields();
        }
    }

    record PlannedQuery(DynamicForm form,
                        SqlRequest request,
                        SqlExecutionOptions options,
                        DataScope scope,
                        SensitiveDisplayMode displayMode,
                        ProtectedFieldRuntime.PreparedContainsQuery containsQuery,
                        List<String> outputFields,
                        FormFieldDecodingPlan resultDecodingPlan,
                        SqlRequest countRequest,
                        PageQuery page,
                        JoinQuerySpec joinSpec,
                        JoinResultProtector.ResultPlan joinResultPlan) implements RowReadPlan {
    }

    record PlannedKeysetPage(DynamicForm form,
                             SqlRequest request,
                             KeysetPageNormalizer.NormalizedKeysetPage page,
                             HiddenProjectionLayout layout,
                             SqlExecutionOptions options,
                             com.flying.orm.core.scope.DataScope scope,
                             com.flying.orm.core.protection.SensitiveDisplayMode displayMode,
                             List<String> outputFields,
                             FormFieldDecodingPlan resultDecodingPlan,
                             JoinQuerySpec joinSpec,
                             JoinResultProtector.ResultPlan joinResultPlan,
                             ProtectedFieldRuntime.PreparedContainsQuery containsQuery) implements RowReadPlan {
        public List<String> verifiedFields() { return layout.decodingFields(); }
        public List<String> decodingFields() {
            return contains() ? containsQuery.visibleFields() : layout.decodingFields();
        }
    }

}
