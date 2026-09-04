package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionGroups;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteOperation;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.lock.LockingReadPlan;
import com.flying.orm.rdb.lock.LockingReadSpec;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 把表单查询和单条写入规格编译成与驱动无关的安全 SQL 计划。
 *
 * <p>这里统一完成结构化条件解析、Scope 合并、字段保护、逻辑删除和乐观锁 SQL 选择。计划里只有
 * {@link SqlRequest} 和执行保护，不含 JDBC、R2DBC、Publisher 或 Connection，因此两条执行链消费的是同一结果，
 * 不会因为分别实现 CRUD 而产生参数顺序或安全规则偏差。</p>
 */
final class FormOperationPlanner {

    final FormDataSqlRenderer renderer;
    final FormScopeSupport scopes;
    final SqlExecutionOptions defaultExecutionOptions;
    FormOperationPlanner(FormDataSqlRenderer renderer,
                         FormScopeSupport scopes,
                         SqlExecutionOptions defaultExecutionOptions) {
        this.renderer = Objects.requireNonNull(renderer, "form data sql renderer must not be null");
        this.scopes = Objects.requireNonNull(scopes, "form scope support must not be null");
        this.defaultExecutionOptions = Objects.requireNonNull(
                defaultExecutionOptions, "default sql execution options must not be null");
    }

    PlannedQuery select(QuerySpec spec) {
        return FormReadPlanSupport.select(this, spec);
    }

    PlannedPage page(QuerySpec spec, PageQuery page) {
        return FormReadPlanSupport.page(this, spec, page);
    }

    GovernedPlanEnvelope<PlannedPage> pageGoverned(QuerySpec spec,
                                                    PageQuery page,
                                                    FieldUsePolicy policy,
                                                    QueryShapeLimits limits) {
        return FormReadPlanSupport.pageGoverned(this, spec, page, policy, limits);
    }

    /** 纯规划锁定读取；这里只编译 SQL 和路由意图，不读取事务或连接。 */
    PlannedLockingRead lockingRead(LockingReadSpec spec) {
        return FormReadPlanSupport.lockingRead(this, spec);
    }

    GovernedPlanEnvelope<PlannedLockingRead> lockingReadGoverned(
            LockingReadSpec spec,
            FieldUsePolicy policy,
            QueryShapeLimits limits) {
        return FormReadPlanSupport.lockingReadGoverned(this, spec, policy, limits);
    }

    PlannedCursorPage cursorPage(QuerySpec spec, CursorPageQuery page) {
        return FormReadPlanSupport.cursorPage(this, spec, page);
    }

    GovernedPlanEnvelope<PlannedCursorPage> cursorPageGoverned(QuerySpec spec,
                                                                CursorPageQuery page,
                                                                FieldUsePolicy policy,
                                                                QueryShapeLimits limits) {
        return FormReadPlanSupport.cursorPageGoverned(this, spec, page, policy, limits);
    }

    PlannedKeysetPage keysetPage(QuerySpec spec, KeysetPageQuery page) {
        return FormKeysetPlanSupport.keysetPage(this, spec, page);
    }

    PlannedLockingKeysetRead lockingKeysetRead(
            LockingReadSpec spec,
            KeysetPageQuery page) {
        return FormKeysetPlanSupport.lockingRead(this, spec, page);
    }

    GovernedPlanEnvelope<PlannedLockingKeysetRead> lockingKeysetReadGoverned(
            LockingReadSpec spec,
            KeysetPageQuery page,
            FieldUsePolicy policy,
            QueryShapeLimits limits) {
        return FormKeysetPlanSupport.lockingReadGoverned(
                this, spec, page, policy, limits);
    }

    GovernedPlanEnvelope<PlannedKeysetPage> keysetPageGoverned(
            QuerySpec spec,
            KeysetPageQuery page,
            FieldUsePolicy policy,
            QueryShapeLimits limits) {
        return FormKeysetPlanSupport.keysetPageGoverned(
                this, spec, page, policy, limits);
    }

    PlannedWrite insert(WriteSpec spec) {
        WriteSpec safeSpec = requireOperation(spec, WriteOperation.INSERT, "insert");
        if (safeSpec.lock().isPresent()) {
            throw new IllegalArgumentException("insert spec must not contain optimistic lock options");
        }
        DynamicForm form = safeSpec.form();
        com.flying.orm.core.scope.DataScope effectiveScope = scopes.effectiveScope(safeSpec.scope());
        Map<String, Object> values = scopes.prepareWriteValues(form, safeSpec.ownedValues(), effectiveScope);
        FormPreparedWrite write = renderer.protection().prepareWrite(
                form, values, effectiveScope);
        SqlRequest request = renderer.protection().insert(write);
        ProtectedWriteWork protectedWrite = renderer.protection().protectedWrite(
                form, values, effectiveScope, request, null, ProtectedWriteWork.Kind.INSERT).orElse(null);
        return new PlannedWrite(form, request, executionOptions(safeSpec), null, protectedWrite);
    }

    GovernedPlanEnvelope<PlannedWrite> insertGoverned(WriteSpec spec,
                                                       FieldUsePolicy policy,
                                                       QueryShapeLimits limits) {
        WriteSpec safeSpec = Objects.requireNonNull(spec, "insert spec must not be null");
        PlannedWrite plan = insert(safeSpec);
        return FieldUseGuard.write(plan, renderer, safeSpec, scopes.effectiveScope(safeSpec.scope()), plan.request(),
                                   policy, limits);
    }

    PlannedWrite update(WriteSpec spec) {
        WriteSpec safeSpec = requireOperation(spec, WriteOperation.UPDATE, "update");
        DynamicForm form = safeSpec.form();
        Map<String, Object> values = safeSpec.ownedValues();
        ConditionGroup where = scopes.writableActiveWhere(
                form, values, safeSpec.where(), safeSpec.scope());
        com.flying.orm.core.scope.DataScope effectiveScope = scopes.effectiveScope(safeSpec.scope());
        DynamicForm physicalForm = renderer.protection().physicalForm(form);
        FormPreparedWrite write = renderer.protection().prepareWrite(
                form, physicalForm, values, effectiveScope);
        ProtectedFieldRuntime.PreparedQuery query = renderer.protection().prepareQuery(
                form, physicalForm, form, where, effectiveScope);
        SqlRequest request = safeSpec.lock()
                                     .map(lock -> renderer.protection().update(write, query.where(), lock))
                                     .orElseGet(() -> renderer.protection().update(write, query.where()));
        ProtectedFieldRuntime.PreparedQuery ownerQuery = null;
        if (renderer.protection().requiresOwnerQuery(form, values)) {
            ownerQuery = safeSpec.lock()
                    .map(lock -> renderer.protection().prepareQuery(
                            form, physicalForm, form, withExpectedVersion(where, lock), effectiveScope))
                    .orElse(query);
        }
        ProtectedWriteWork protectedWrite = renderer.protection().protectedWrite(
                form, values, effectiveScope, request, ownerQuery, ProtectedWriteWork.Kind.UPDATE)
                                                       .orElse(null);
        return new PlannedWrite(form, request, executionOptions(safeSpec),
                                safeSpec.lock().orElse(null), protectedWrite);
    }

    GovernedPlanEnvelope<PlannedWrite> updateGoverned(WriteSpec spec,
                                                       FieldUsePolicy policy,
                                                       QueryShapeLimits limits) {
        WriteSpec safeSpec = Objects.requireNonNull(spec, "update spec must not be null");
        PlannedWrite plan = update(safeSpec);
        return FieldUseGuard.write(plan, renderer, safeSpec, scopes.effectiveScope(safeSpec.scope()), plan.request(),
                                   policy, limits);
    }

    PlannedWrite delete(WriteSpec spec) {
        WriteSpec safeSpec = requireOperation(spec, WriteOperation.DELETE, "delete");
        DynamicForm form = safeSpec.form();
        ConditionGroup scopedWhere = scopes.scopedWhere(form, safeSpec.where(), safeSpec.scope());
        ConditionGroup activeWhere = FormLogicDeletes.activeWhere(form, scopedWhere);
        com.flying.orm.core.scope.DataScope effectiveScope = scopes.effectiveScope(safeSpec.scope());
        OptimisticLockOptions lock = safeSpec.lock().orElse(null);
        SqlRequest request = FormLogicDeletes.deleteValues(form)
                .map(values -> {
                    FormPreparedWrite write = renderer.protection().prepareWrite(
                            form, values, effectiveScope);
                    ProtectedFieldRuntime.PreparedQuery query = renderer.protection().prepareQuery(
                            form, form, activeWhere, effectiveScope);
                    return lock == null
                            ? renderer.protection().update(write, query.where())
                            : renderer.protection().update(write, query.where(), lock);
                })
                .orElseGet(() -> {
                    ProtectedFieldRuntime.PreparedQuery query = renderer.protection().prepareQuery(
                            form, form, scopedWhere, effectiveScope);
                    return renderer.protection().delete(query, lock);
                });
        return new PlannedWrite(form, request, executionOptions(safeSpec), lock, null);
    }

    GovernedPlanEnvelope<PlannedWrite> deleteGoverned(WriteSpec spec,
                                                       FieldUsePolicy policy,
                                                       QueryShapeLimits limits) {
        WriteSpec safeSpec = Objects.requireNonNull(spec, "delete spec must not be null");
        PlannedWrite plan = delete(safeSpec);
        return FieldUseGuard.write(plan, renderer, safeSpec, scopes.effectiveScope(safeSpec.scope()), plan.request(),
                                   policy, limits);
    }

    PlannedWrite physicalDelete(WriteSpec spec) {
        WriteSpec safeSpec = requireOperation(spec, WriteOperation.DELETE, "delete");
        DynamicForm form = safeSpec.form();
        ConditionGroup where = scopes.scopedWhere(form, safeSpec.where(), safeSpec.scope());
        com.flying.orm.core.scope.DataScope effectiveScope = scopes.effectiveScope(safeSpec.scope());
        ProtectedFieldRuntime.PreparedQuery query = renderer.protection().prepareQuery(
                form, form, where, effectiveScope);
        OptimisticLockOptions lock = safeSpec.lock().orElse(null);
        SqlRequest request = renderer.protection().delete(query, lock);
        return new PlannedWrite(form, request, executionOptions(safeSpec), lock, null);
    }

    GovernedPlanEnvelope<PlannedWrite> physicalDeleteGoverned(WriteSpec spec,
                                                               FieldUsePolicy policy,
                                                               QueryShapeLimits limits) {
        WriteSpec safeSpec = Objects.requireNonNull(spec, "delete spec must not be null");
        PlannedWrite plan = physicalDelete(safeSpec);
        return FieldUseGuard.write(plan, renderer, safeSpec, scopes.effectiveScope(safeSpec.scope()), plan.request(),
                                   policy, limits);
    }

    /** 受治理查询仍复用 legacy 计划，只在当前调用外层保存动态审批结果。 */
    GovernedPlanEnvelope<PlannedQuery> selectGoverned(QuerySpec spec,
                                                       FieldUsePolicy policy,
                                                       QueryShapeLimits limits) {
        return FormReadPlanSupport.selectGoverned(this, spec, policy, limits);
    }

    private static ConditionGroup withExpectedVersion(ConditionGroup where, OptimisticLockOptions lock) {
        ConditionGroup expectedVersion = ConditionGroup.and()
                                                        .where(lock.field(), "=", lock.expectedValue())
                                                        .build();
        return ConditionGroups.and(where, expectedVersion);
    }

    private SqlExecutionOptions executionOptions(WriteSpec spec) {
        return spec.executionOptions().orElse(defaultExecutionOptions);
    }

    private static WriteSpec requireOperation(WriteSpec spec, WriteOperation expected, String action) {
        WriteSpec safeSpec = Objects.requireNonNull(spec, action + " spec must not be null");
        if (safeSpec.operation() != expected) {
            throw new IllegalArgumentException(
                    "write spec operation " + safeSpec.operation() + " cannot execute as " + expected);
        }
        return safeSpec;
    }

    record PlannedQuery(DynamicForm form,
                        SqlRequest request,
                        SqlExecutionOptions options,
                        com.flying.orm.core.scope.DataScope scope,
                        com.flying.orm.core.protection.SensitiveDisplayMode displayMode,
                        ProtectedFieldRuntime.PreparedContainsQuery containsQuery,
                        List<String> outputFields) {

        boolean contains() {
            return containsQuery != null;
        }

        List<String> decodingFields() {
            return contains() ? containsQuery.visibleFields() : outputFields;
        }
    }

    record PlannedPage(DynamicForm form,
                       SqlRequest countRequest,
                       SqlRequest dataRequest,
                       PageQuery page,
                       SqlExecutionOptions options,
                       com.flying.orm.core.scope.DataScope scope,
                       com.flying.orm.core.protection.SensitiveDisplayMode displayMode,
                       ProtectedFieldRuntime.PreparedContainsQuery containsQuery,
                       List<String> outputFields) {

        boolean contains() {
            return containsQuery != null;
        }

        List<String> decodingFields() {
            return contains() ? containsQuery.visibleFields() : outputFields;
        }
    }

    record PlannedCursorPage(DynamicForm form,
                             SqlRequest request,
                             CursorPageNormalizer.NormalizedCursorPage page,
                             SqlExecutionOptions options,
                             com.flying.orm.core.scope.DataScope scope,
                             com.flying.orm.core.protection.SensitiveDisplayMode displayMode,
                             ProtectedFieldRuntime.PreparedContainsQuery containsQuery,
                             List<String> outputFields) {

        boolean contains() {
            return containsQuery != null;
        }

        List<String> decodingFields() {
            return contains() ? containsQuery.visibleFields() : outputFields;
        }
    }

    record PlannedKeysetPage(DynamicForm form,
                             SqlRequest request,
                             KeysetPageNormalizer.NormalizedKeysetPage page,
                             HiddenProjectionLayout layout,
                             SqlExecutionOptions options,
                             com.flying.orm.core.scope.DataScope scope,
                             com.flying.orm.core.protection.SensitiveDisplayMode displayMode,
                             List<String> outputFields) {
    }

    record PlannedLockingRead(PlannedQuery query, LockingReadPlan plan) {
        PlannedLockingRead {
            query = Objects.requireNonNull(query, "locking read query plan must not be null");
            plan = Objects.requireNonNull(plan, "locking read public plan must not be null");
        }
    }

    record PlannedLockingKeysetRead(PlannedKeysetPage query, LockingReadPlan plan) {
        PlannedLockingKeysetRead {
            query = Objects.requireNonNull(query, "locking keyset query plan must not be null");
            plan = Objects.requireNonNull(plan, "locking keyset public plan must not be null");
        }
    }

    record PlannedWrite(DynamicForm form,
                        SqlRequest request,
                        SqlExecutionOptions options,
                        OptimisticLockOptions lock,
                        ProtectedWriteWork protectedWrite) {

        boolean protectedWriteRequired() {
            return protectedWrite != null;
        }

        java.util.Optional<String> generatedKeyColumn() {
            List<String> columns = form.fields().stream()
                                       .filter(field -> field.primaryKey() && field.generation().generated())
                                       .map(DynamicField::name)
                                       .toList();
            return columns.size() == 1 ? java.util.Optional.of(columns.getFirst()) : java.util.Optional.empty();
        }

        long requireSuccess(long affectedRows) {
            if (lock != null && affectedRows == 0L) {
                throw new com.flying.orm.rdb.lock.OptimisticLockConflictException(
                        form.table(), lock.field(), lock.expectedValue());
            }
            return affectedRows;
        }
    }
}
