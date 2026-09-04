package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.lock.LockingReadDialect;
import com.flying.orm.rdb.lock.LockingReadPlan;
import com.flying.orm.rdb.lock.LockingReadSpec;
import com.flying.orm.rdb.lock.ReadLock;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * 普通、offset、cursor 与锁定读取的纯计划逻辑；不读取连接或事务状态。
 *
 * @author wangr
 * @version v3.2
 */
final class FormReadPlanSupport {

    private FormReadPlanSupport() {
    }

    static FormOperationPlanner.PlannedQuery select(FormOperationPlanner planner, QuerySpec spec) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        return select(planner, safeSpec, scopedRead(planner, safeSpec), safeSpec.sensitiveDisplayMode());
    }

    static GovernedPlanEnvelope<FormOperationPlanner.PlannedQuery> selectGoverned(
            FormOperationPlanner planner,
            QuerySpec spec,
            FieldUsePolicy policy,
            QueryShapeLimits limits) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        FormScopeSupport.GovernedRead governed = governedRead(planner, safeSpec);
        FormOperationPlanner.PlannedQuery plan = select(planner, safeSpec, governed.read(),
                FieldUseGuard.effectiveDisplayMode(policy, safeSpec.sensitiveDisplayMode()));
        return FieldUseGuard.query(
                plan, planner.renderer, safeSpec, governed.businessWhere(), plan.outputFields(),
                safeSpec.sorts().stream().map(PageSort::field).toList(), safeSpec.sorts().size(), false,
                plan.scope(), plan.request(), policy, limits);
    }

    static FormOperationPlanner.PlannedPage page(
            FormOperationPlanner planner, QuerySpec spec, PageQuery page) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        FormQueryShapeGuard.requireUngroupedPagination(safeSpec, "offset pagination");
        PageQuery requested = Objects.requireNonNull(page, "page query must not be null");
        return page(planner, safeSpec, requested, scopedRead(planner, safeSpec), safeSpec.sensitiveDisplayMode());
    }

    static GovernedPlanEnvelope<FormOperationPlanner.PlannedPage> pageGoverned(
            FormOperationPlanner planner,
            QuerySpec spec,
            PageQuery page,
            FieldUsePolicy policy,
            QueryShapeLimits limits) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        FormQueryShapeGuard.requireUngroupedPagination(safeSpec, "offset pagination");
        PageQuery requested = Objects.requireNonNull(page, "page query must not be null");
        FormScopeSupport.GovernedRead governed = governedRead(planner, safeSpec);
        FormOperationPlanner.PlannedPage plan = page(planner, safeSpec, requested, governed.read(),
                FieldUseGuard.effectiveDisplayMode(policy, safeSpec.sensitiveDisplayMode()));
        return FieldUseGuard.query(
                plan, planner.renderer, safeSpec, governed.businessWhere(), plan.outputFields(),
                plan.page().sorts().stream().map(PageSort::field).toList(),
                plan.page().sorts().size(), false,
                plan.scope(), plan.dataRequest(), policy, limits);
    }

    static FormOperationPlanner.PlannedCursorPage cursorPage(
            FormOperationPlanner planner,
            QuerySpec spec,
            CursorPageQuery page) {
        QuerySpec safeSpec = requireCursorSpec(spec);
        CursorPageQuery safePage = Objects.requireNonNull(page, "cursor page query must not be null");
        return cursorPage(planner, safeSpec, safePage, scopedRead(planner, safeSpec), safeSpec.sensitiveDisplayMode());
    }

    static GovernedPlanEnvelope<FormOperationPlanner.PlannedCursorPage> cursorPageGoverned(
            FormOperationPlanner planner,
            QuerySpec spec,
            CursorPageQuery page,
            FieldUsePolicy policy,
            QueryShapeLimits limits) {
        QuerySpec safeSpec = requireCursorSpec(spec);
        CursorPageQuery safePage = Objects.requireNonNull(page, "cursor page query must not be null");
        FormScopeSupport.GovernedRead governed = governedRead(planner, safeSpec);
        FormOperationPlanner.PlannedCursorPage plan = cursorPage(
                planner, safeSpec, safePage, governed.read(),
                FieldUseGuard.effectiveDisplayMode(policy, safeSpec.sensitiveDisplayMode()));
        return FieldUseGuard.query(
                plan, planner.renderer, safeSpec, governed.businessWhere(), plan.outputFields(),
                plan.page().sorts().stream().map(sort -> sort.field()).toList(),
                plan.page().callerSortCount(), true,
                plan.scope(), plan.request(), policy, limits);
    }

    static FormOperationPlanner.PlannedLockingRead lockingRead(
            FormOperationPlanner planner, LockingReadSpec spec) {
        LockingReadSpec safeSpec = Objects.requireNonNull(spec, "locking read spec must not be null");
        LockingReadDialect dialect = requireLockingDialect(planner, safeSpec.lock());
        FormOperationPlanner.PlannedQuery query = selectLocking(
                planner, safeSpec.query(), dialect, safeSpec.lock(),
                scopedRead(planner, safeSpec.query()), safeSpec.query().sensitiveDisplayMode());
        return new FormOperationPlanner.PlannedLockingRead(
                query, new LockingReadPlan(query.request(), safeSpec.routingIntent()));
    }

    static GovernedPlanEnvelope<FormOperationPlanner.PlannedLockingRead> lockingReadGoverned(
            FormOperationPlanner planner,
            LockingReadSpec spec,
            FieldUsePolicy policy,
            QueryShapeLimits limits) {
        LockingReadSpec safeSpec = Objects.requireNonNull(spec, "locking read spec must not be null");
        LockingReadDialect dialect = requireLockingDialect(planner, safeSpec.lock());
        FormScopeSupport.GovernedRead governed = governedRead(planner, safeSpec.query());
        FormOperationPlanner.PlannedQuery query = selectLocking(
                planner, safeSpec.query(), dialect, safeSpec.lock(), governed.read(),
                FieldUseGuard.effectiveDisplayMode(policy, safeSpec.query().sensitiveDisplayMode()));
        FormOperationPlanner.PlannedLockingRead locking =
                new FormOperationPlanner.PlannedLockingRead(
                query, new LockingReadPlan(query.request(), safeSpec.routingIntent()));
        GovernedPlanEnvelope<FormOperationPlanner.PlannedQuery> approved = FieldUseGuard.query(
                query, planner.renderer, safeSpec.query(), governed.businessWhere(), query.outputFields(),
                safeSpec.query().sorts().stream().map(PageSort::field).toList(),
                safeSpec.query().sorts().size(), false,
                query.scope(), query.request(), policy, limits);
        return new GovernedPlanEnvelope<>(locking, approved.fieldUse());
    }

    static ScopedRead scopedRead(FormOperationPlanner planner, QuerySpec spec) {
        return spec.structuredInput()
                   .map(input -> planner.scopes.scopedStructuredRead(
                           spec.form(), input,
                           spec.structuredPolicy().orElse(StructuredConditionPolicy.defaults()), spec.scope()))
                   .orElseGet(() -> planner.scopes.scopedRead(spec.form(), spec.where(), spec.scope()));
    }

    /** governed 才创建该上下文；结构化条件仍只编译一次。 */
    static FormScopeSupport.GovernedRead governedRead(FormOperationPlanner planner, QuerySpec spec) {
        return spec.structuredInput()
                   .map(input -> planner.scopes.governedStructuredRead(
                           spec.form(), input,
                           spec.structuredPolicy().orElse(StructuredConditionPolicy.defaults()), spec.scope()))
                   .orElseGet(() -> planner.scopes.governedRead(spec.form(), spec.where(), spec.scope()));
    }

    static SqlExecutionOptions executionOptions(FormOperationPlanner planner, QuerySpec spec) {
        return spec.executionOptions().orElse(planner.defaultExecutionOptions);
    }

    static LockingReadDialect requireLockingDialect(FormOperationPlanner planner, ReadLock lock) {
        LockingReadDialect dialect = planner.renderer.lockingReadDialect();
        if (!dialect.supports(Objects.requireNonNull(lock, "read lock must not be null"))) {
            throw new UnsupportedOperationException(
                    "locking read is not supported by this database descriptor");
        }
        return dialect;
    }

    static ProtectedFieldRuntime.PreparedQuery withProjection(
            ProtectedFieldRuntime.PreparedQuery query,
            List<String> projections) {
        return projections.isEmpty()
                ? query
                : new ProtectedFieldRuntime.PreparedQuery(query.physicalForm(), query.where(), projections);
    }

    private static FormOperationPlanner.PlannedQuery select(
            FormOperationPlanner planner,
            QuerySpec spec,
            ScopedRead read,
            SensitiveDisplayMode displayMode) {
        List<String> projections = FormQueryShapeGuard.readableProjections(spec, read.form());
        List<String> groups = FormQueryShapeGuard.readableGroups(spec, read.form());
        List<PageSort> sorts = FormQueryShapeGuard.readableSorts(spec.form(), read.form(), spec.sorts());
        FormQueryShapeGuard.requireValidGrouping(projections, groups, sorts);
        Optional<ProtectedFieldRuntime.PreparedContainsQuery> contains = planner.renderer.protection()
                .prepareContainsQuery(spec.form(), read.form(), read.where(), read.scope());
        if (contains.isPresent()) {
            FormQueryShapeGuard.requireContainsShape(spec);
            List<String> outputFields = FormQueryShapeGuard.outputFields(projections, read.form());
            SqlRequest request = FormProtectionQueryRequests.containsRows(
                    planner.renderer.protection(), contains.orElseThrow(), sorts,
                    ProtectedContainsResultSupport.DEFAULT_CANDIDATE_LIMIT);
            return new FormOperationPlanner.PlannedQuery(
                    spec.form(), request, executionOptions(planner, spec),
                    read.scope(), displayMode,
                    contains.orElseThrow(), outputFields);
        }
        ProtectedFieldRuntime.PreparedQuery query = planner.renderer.protection().prepareQuery(
                spec.form(), read.form(), read.where(), read.scope());
        query = withProjection(query, projections);
        SqlRequest request = planner.renderer.protection().select(query, groups, sorts);
        return new FormOperationPlanner.PlannedQuery(
                spec.form(), request, executionOptions(planner, spec),
                read.scope(), displayMode, null,
                FormQueryShapeGuard.outputFields(projections, read.form()));
    }

    private static FormOperationPlanner.PlannedQuery selectLocking(
            FormOperationPlanner planner,
            QuerySpec spec,
            LockingReadDialect dialect,
            ReadLock lock,
            ScopedRead read,
            SensitiveDisplayMode displayMode) {
        List<String> projections = FormQueryShapeGuard.readableProjections(spec, read.form());
        List<String> groups = FormQueryShapeGuard.readableGroups(spec, read.form());
        List<PageSort> sorts = FormQueryShapeGuard.readableSorts(spec.form(), read.form(), spec.sorts());
        FormQueryShapeGuard.requireValidGrouping(projections, groups, sorts);
        if (!groups.isEmpty()) {
            throw new UnsupportedOperationException("locking read does not support grouped queries");
        }
        if (planner.renderer.protection().prepareContainsQuery(
                spec.form(), read.form(), read.where(), read.scope()).isPresent()) {
            throw new UnsupportedOperationException(
                    "locking read does not support protected contains queries");
        }
        ProtectedFieldRuntime.PreparedQuery query = planner.renderer.protection().prepareQuery(
                spec.form(), read.form(), read.where(), read.scope());
        query = withProjection(query, projections);
        SqlRequest request = planner.renderer.protection().selectLocking(
                query, groups, sorts, dialect, lock);
        return new FormOperationPlanner.PlannedQuery(
                spec.form(), request, executionOptions(planner, spec),
                read.scope(), displayMode, null,
                FormQueryShapeGuard.outputFields(projections, read.form()));
    }

    private static FormOperationPlanner.PlannedPage page(
            FormOperationPlanner planner,
            QuerySpec spec,
            PageQuery requested,
            ScopedRead read,
            SensitiveDisplayMode displayMode) {
        List<String> projections = FormQueryShapeGuard.readableProjections(spec, read.form());
        List<PageSort> requestedSorts = spec.sorts().isEmpty() ? requested.sorts() : spec.sorts();
        List<PageSort> sorts = FormQueryShapeGuard.readableSorts(spec.form(), read.form(), requestedSorts);
        PageQuery effectivePage = new PageQuery(requested.page(), requested.size(), sorts);
        Optional<ProtectedFieldRuntime.PreparedContainsQuery> contains = planner.renderer.protection()
                .prepareContainsQuery(spec.form(), read.form(), read.where(), read.scope());
        if (contains.isPresent()) {
            FormQueryShapeGuard.requireContainsShape(spec);
            SqlRequest request = FormProtectionQueryRequests.containsRows(
                    planner.renderer.protection(), contains.orElseThrow(), effectivePage.sorts(),
                    ProtectedContainsResultSupport.DEFAULT_CANDIDATE_LIMIT);
            return new FormOperationPlanner.PlannedPage(
                    spec.form(), null, request, effectivePage,
                    executionOptions(planner, spec), read.scope(),
                    displayMode, contains.orElseThrow(),
                    FormQueryShapeGuard.outputFields(projections, read.form()));
        }
        ProtectedFieldRuntime.PreparedQuery query = planner.renderer.protection().prepareQuery(
                spec.form(), read.form(), read.where(), read.scope());
        query = withProjection(query, projections);
        return new FormOperationPlanner.PlannedPage(
                spec.form(), planner.renderer.protection().count(query),
                planner.renderer.protection().select(query, effectivePage), effectivePage,
                executionOptions(planner, spec), read.scope(), displayMode,
                null, FormQueryShapeGuard.outputFields(projections, read.form()));
    }

    private static FormOperationPlanner.PlannedCursorPage cursorPage(
            FormOperationPlanner planner,
            QuerySpec spec,
            CursorPageQuery page,
            ScopedRead read,
            SensitiveDisplayMode displayMode) {
        CursorPageNormalizer.NormalizedCursorPage normalized = CursorPageNormalizer.normalize(read.form(), page);
        FormQueryShapeGuard.requireReadableUnencryptedCursorSorts(
                spec.form(), read.form(), normalized.sorts(), displayMode);
        List<String> projections = FormQueryShapeGuard.readableProjections(spec, read.form());
        FormQueryShapeGuard.requireCursorProjection(projections, normalized.sorts());
        Optional<ProtectedFieldRuntime.PreparedContainsQuery> contains = planner.renderer.protection()
                .prepareContainsQuery(spec.form(), read.form(), read.where(), read.scope());
        if (contains.isPresent()) {
            FormQueryShapeGuard.requireContainsShape(spec);
            SqlRequest request = FormProtectionQueryRequests.containsRows(
                    planner.renderer.protection(), contains.orElseThrow(), normalized,
                    ProtectedContainsResultSupport.DEFAULT_CANDIDATE_LIMIT);
            return new FormOperationPlanner.PlannedCursorPage(
                    spec.form(), request, normalized,
                    executionOptions(planner, spec), read.scope(),
                    displayMode, contains.orElseThrow(),
                    FormQueryShapeGuard.outputFields(projections, read.form()));
        }
        ProtectedFieldRuntime.PreparedQuery query = planner.renderer.protection().prepareQuery(
                spec.form(), read.form(), read.where(), read.scope());
        query = withProjection(query, projections);
        return new FormOperationPlanner.PlannedCursorPage(
                spec.form(), planner.renderer.protection().select(query, normalized),
                normalized, executionOptions(planner, spec), read.scope(),
                displayMode, null,
                FormQueryShapeGuard.outputFields(projections, read.form()));
    }

    private static QuerySpec requireCursorSpec(QuerySpec spec) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        FormQueryShapeGuard.requireUngroupedPagination(safeSpec, "cursor pagination");
        if (!safeSpec.sorts().isEmpty()) {
            throw new IllegalArgumentException(
                    "cursor pagination sorts must be declared with CursorPageQuery");
        }
        return safeSpec;
    }
}
