package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.lock.LockingReadDialect;
import com.flying.orm.rdb.lock.LockingReadPlan;
import com.flying.orm.rdb.lock.LockingReadSpec;
import com.flying.orm.rdb.lock.ReadLock;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;

import java.util.List;
import java.util.Objects;

/**
 * Keyset 与锁定 keyset 共享同一规范化、隐藏投影和字段治理计划。
 *
 * @author wangr
 * @version v3.2
 */
final class FormKeysetPlanSupport {

    private FormKeysetPlanSupport() {
    }

    static FormOperationPlanner.PlannedKeysetPage keysetPage(
            FormOperationPlanner planner,
            QuerySpec spec,
            KeysetPageQuery page) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        KeysetPageQuery safePage = requirePage(safeSpec, page);
        return plan(planner, safeSpec, safePage,
                    FormReadPlanSupport.scopedRead(planner, safeSpec), safeSpec.sensitiveDisplayMode());
    }

    static GovernedPlanEnvelope<FormOperationPlanner.PlannedKeysetPage> keysetPageGoverned(
            FormOperationPlanner planner,
            QuerySpec spec,
            KeysetPageQuery page,
            FieldUsePolicy policy,
            QueryShapeLimits limits) {
        QuerySpec safeSpec = Objects.requireNonNull(spec, "query spec must not be null");
        KeysetPageQuery safePage = requirePage(safeSpec, page);
        FormScopeSupport.GovernedRead governed = FormReadPlanSupport.governedRead(planner, safeSpec);
        FormOperationPlanner.PlannedKeysetPage plan = plan(
                planner, safeSpec, safePage, governed.read(),
                FieldUseGuard.effectiveDisplayMode(policy, safeSpec.sensitiveDisplayMode()));
        return FieldUseGuard.keyset(
                plan, planner.renderer, safeSpec, governed.businessWhere(), plan.outputFields(),
                plan.page(), plan.layout(), plan.scope(), plan.request(), policy, limits);
    }

    static FormOperationPlanner.PlannedLockingKeysetRead lockingRead(
            FormOperationPlanner planner,
            LockingReadSpec spec,
            KeysetPageQuery page) {
        LockingReadSpec safeSpec = Objects.requireNonNull(
                spec, "locking read spec must not be null");
        LockingReadDialect dialect = FormReadPlanSupport.requireLockingDialect(
                planner, safeSpec.lock());
        QuerySpec querySpec = safeSpec.query();
        KeysetPageQuery safePage = requirePage(
                querySpec, Objects.requireNonNull(page, "keyset page query must not be null"));
        FormOperationPlanner.PlannedKeysetPage query = planLocking(
                planner, querySpec, safePage, dialect, safeSpec.lock(),
                FormReadPlanSupport.scopedRead(planner, querySpec), querySpec.sensitiveDisplayMode());
        return new FormOperationPlanner.PlannedLockingKeysetRead(
                query, new LockingReadPlan(query.request(), safeSpec.routingIntent()));
    }

    static GovernedPlanEnvelope<FormOperationPlanner.PlannedLockingKeysetRead>
            lockingReadGoverned(
            FormOperationPlanner planner,
            LockingReadSpec spec,
            KeysetPageQuery page,
            FieldUsePolicy policy,
            QueryShapeLimits limits) {
        LockingReadSpec safeSpec = Objects.requireNonNull(
                spec, "locking read spec must not be null");
        LockingReadDialect dialect = FormReadPlanSupport.requireLockingDialect(
                planner, safeSpec.lock());
        QuerySpec querySpec = safeSpec.query();
        KeysetPageQuery safePage = requirePage(
                querySpec, Objects.requireNonNull(page, "keyset page query must not be null"));
        FormScopeSupport.GovernedRead governed = FormReadPlanSupport.governedRead(planner, querySpec);
        FormOperationPlanner.PlannedKeysetPage query = planLocking(
                planner, querySpec, safePage, dialect, safeSpec.lock(), governed.read(),
                FieldUseGuard.effectiveDisplayMode(policy, querySpec.sensitiveDisplayMode()));
        FormOperationPlanner.PlannedLockingKeysetRead locking =
                new FormOperationPlanner.PlannedLockingKeysetRead(
                query, new LockingReadPlan(query.request(), safeSpec.routingIntent()));
        GovernedPlanEnvelope<FormOperationPlanner.PlannedKeysetPage> approved = FieldUseGuard.keyset(
                query, planner.renderer, querySpec, governed.businessWhere(), query.outputFields(),
                query.page(), query.layout(), query.scope(), query.request(), policy, limits);
        return new GovernedPlanEnvelope<>(locking, approved.fieldUse());
    }

    private static FormOperationPlanner.PlannedKeysetPage plan(
            FormOperationPlanner planner,
            QuerySpec spec,
            KeysetPageQuery page,
            ScopedRead read,
            SensitiveDisplayMode displayMode) {
        KeysetPageNormalizer.NormalizedKeysetPage normalized = KeysetPageNormalizer.normalize(
                spec.form(), page);
        FormQueryShapeGuard.requireReadableUnprotectedKeysetSorts(
                spec.form(), read.form(), normalized, displayMode);
        List<String> projections = FormQueryShapeGuard.readableProjections(spec, read.form());
        List<String> outputFields = FormQueryShapeGuard.outputFields(projections, read.form());
        HiddenProjectionLayout layout = HiddenProjectionLayout.of(outputFields, normalized);
        requireNoProtectedContains(planner, spec, read);
        ProtectedFieldRuntime.PreparedQuery query = planner.renderer.protection().prepareQuery(
                spec.form(), read.form(), read.where(), read.scope());
        query = FormReadPlanSupport.withProjection(query, projections);
        SqlRequest request = planner.renderer.protection().selectKeyset(query, layout, normalized);
        return new FormOperationPlanner.PlannedKeysetPage(
                spec.form(), request, normalized, layout,
                FormReadPlanSupport.executionOptions(planner, spec),
                read.scope(), displayMode, outputFields);
    }

    private static FormOperationPlanner.PlannedKeysetPage planLocking(
            FormOperationPlanner planner,
            QuerySpec spec,
            KeysetPageQuery page,
            LockingReadDialect dialect,
            ReadLock lock,
            ScopedRead read,
            SensitiveDisplayMode displayMode) {
        KeysetPageNormalizer.NormalizedKeysetPage normalized = KeysetPageNormalizer.normalize(
                spec.form(), page);
        FormQueryShapeGuard.requireReadableUnprotectedKeysetSorts(
                spec.form(), read.form(), normalized, displayMode);
        List<String> projections = FormQueryShapeGuard.readableProjections(spec, read.form());
        List<String> outputFields = FormQueryShapeGuard.outputFields(projections, read.form());
        HiddenProjectionLayout layout = HiddenProjectionLayout.of(outputFields, normalized);
        requireNoProtectedContains(planner, spec, read);
        ProtectedFieldRuntime.PreparedQuery query = planner.renderer.protection().prepareQuery(
                spec.form(), read.form(), read.where(), read.scope());
        query = FormReadPlanSupport.withProjection(query, projections);
        SqlRequest request = planner.renderer.protection().selectKeysetLocking(
                query, layout, normalized, dialect, lock);
        return new FormOperationPlanner.PlannedKeysetPage(
                spec.form(), request, normalized, layout,
                FormReadPlanSupport.executionOptions(planner, spec),
                read.scope(), displayMode, outputFields);
    }

    private static KeysetPageQuery requirePage(QuerySpec spec, KeysetPageQuery page) {
        FormQueryShapeGuard.requireUngroupedPagination(spec, "keyset pagination");
        if (!spec.sorts().isEmpty()) {
            throw new IllegalArgumentException(
                    "keyset pagination sorts must be declared with KeysetPageQuery");
        }
        return Objects.requireNonNull(page, "keyset page query must not be null");
    }

    private static void requireNoProtectedContains(FormOperationPlanner planner,
                                                   QuerySpec spec,
                                                   ScopedRead read) {
        if (planner.renderer.protection().prepareContainsQuery(
                spec.form(), read.form(), read.where(), read.scope()).isPresent()) {
            throw new UnsupportedOperationException(
                    "protected contains search does not support keyset pagination");
        }
    }
}
