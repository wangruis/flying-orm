package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionGroups;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.param.ParameterConditionCompiler;
import com.flying.orm.core.param.ParameterConditionPackage;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;

import java.util.Map;
import java.util.Objects;

/**
 * 统一处理动态表单的范围快照、前端结构化条件和写入字段保护。
 *
 * <p>真正的安全规则仍由既有 {@link FormScopeGuard} 执行；本类只把同一份合并后的范围传给
 * 查询、写入和批量操作，避免不同入口各自拼接条件而出现租户或字段权限偏差。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class FormScopeSupport {

    private final FormDataSqlRenderer renderer;
    private final FormScopeGuard guard;

    FormScopeSupport(FormDataSqlRenderer renderer,
                     StructuredConditionResolver resolver,
                     DataScope defaultDataScope) {
        this.renderer = Objects.requireNonNull(renderer, "form data sql renderer must not be null");
        this.guard = new FormScopeGuard(renderer, Objects.requireNonNull(resolver,
                                                                         "structured condition resolver must not be null"),
                                        Objects.requireNonNull(defaultDataScope, "default data scope must not be null"));
    }

    DataScope effectiveScope(DataScope scope) {
        return guard.effectiveScope(scope);
    }

    ConditionGroup scopedWhere(DynamicForm form, ConditionGroup where) {
        return scopedWhere(form, where, DataScope.none());
    }

    ConditionGroup scopedWhere(DynamicForm form, ConditionGroup where, DataScope scope) {
        return guard.scopedWhere(form, where, scope);
    }

    ScopedRead scopedRead(DynamicForm form, ConditionGroup where, DataScope scope) {
        FormScopeGuard.ScopedRead read = guard.scopedRead(form, where, scope);
        return new ScopedRead(read.form(), read.where(), read.scope());
    }

    ScopedRead scopedStructuredRead(DynamicForm form,
                                    StructuredConditionInput input,
                                    StructuredConditionPolicy policy) {
        return scopedStructuredRead(form, input, policy, DataScope.none());
    }

    ScopedRead scopedStructuredRead(DynamicForm form,
                                    StructuredConditionInput input,
                                    StructuredConditionPolicy policy,
                                    DataScope scope) {
        FormScopeGuard.ScopedRead read = guard.scopedStructuredRead(form, input, policy, scope);
        return new ScopedRead(read.form(), read.where(), read.scope());
    }

    ConditionGroup writableActiveWhere(DynamicForm form,
                                       Map<String, Object> values,
                                       ConditionGroup where,
                                       DataScope scope) {
        return guard.writableActiveWhere(form, values, where, scope);
    }

    ConditionGroup batchUpdateWhere(DynamicForm form,
                                    BatchOptimisticUpdate update,
                                    DataScope effectiveScope) {
        return guard.batchUpdateWhere(form, update, effectiveScope);
    }

    PreparedBatchUpdate prepareBatchUpdate(DynamicForm form,
                                           BatchOptimisticUpdate update,
                                           DataScope effectiveScope) {
        return prepareBatchUpdate(
                form, renderer.protection().physicalForm(form), update, effectiveScope);
    }

    PreparedBatchUpdate prepareBatchUpdate(DynamicForm form,
                                           DynamicForm physicalForm,
                                           BatchOptimisticUpdate update,
                                           DataScope effectiveScope) {
        FormProtectionSqlSupport.WriteOperation protection = renderer.protection().writeOperation(
                form, physicalForm, effectiveScope, null);
        return prepareBatchUpdate(form, physicalForm, update, effectiveScope, protection);
    }

    PreparedBatchUpdate prepareBatchUpdate(DynamicForm form,
                                           DynamicForm physicalForm,
                                           BatchOptimisticUpdate update,
                                           DataScope effectiveScope,
                                           FormProtectionSqlSupport.WriteOperation protection) {
        return prepareBatchUpdate(
                form, physicalForm, update,
                prepareBatchScope(form, physicalForm, effectiveScope), protection);
    }

    PreparedBatchScope prepareBatchScope(DynamicForm form,
                                         DynamicForm physicalForm,
                                         DataScope effectiveScope) {
        DynamicForm safePhysicalForm = Objects.requireNonNull(
                physicalForm, "physical form must not be null");
        DataScope safeScope = Objects.requireNonNull(effectiveScope, "effective data scope must not be null");
        ConditionGroup empty = ConditionGroup.and().build();
        ConditionGroup scopeWhere = FormDataScopes.apply(form, empty, safeScope);
        if (ConditionGroups.isEmpty(scopeWhere)) {
            return new PreparedBatchScope(safeScope, empty);
        }
        ProtectedFieldRuntime.PreparedQuery prepared = renderer.protection().prepareQuery(
                form, safePhysicalForm, form, scopeWhere, safeScope);
        return new PreparedBatchScope(safeScope, prepared.where());
    }

    PreparedBatchUpdate prepareBatchUpdate(DynamicForm form,
                                           DynamicForm physicalForm,
                                           BatchOptimisticUpdate update,
                                           PreparedBatchScope batchScope,
                                           FormProtectionSqlSupport.WriteOperation protection) {
        DynamicForm safePhysicalForm = Objects.requireNonNull(
                physicalForm, "physical form must not be null");
        PreparedBatchScope safeBatchScope = Objects.requireNonNull(
                batchScope, "prepared batch scope must not be null");
        ConditionGroup businessWhere = guard.batchUpdateBusinessWhere(
                form, update, safeBatchScope.scope());
        Map<String, Object> logicalValues = update.ownedValues();
        FormPreparedWrite write = protection.prepare(logicalValues);
        ProtectedFieldRuntime.PreparedQuery query = renderer.protection().prepareQuery(
                form, safePhysicalForm, form, businessWhere, safeBatchScope.scope());
        ConditionGroup where = FormLogicDeletes.activeWhere(
                form, combineScope(query.where(), safeBatchScope.where()));
        SqlRequest request = renderer.protection().update(write, where, update.lock());
        ProtectedFieldRuntime.PreparedQuery ownerQuery = renderer.protection().requiresOwnerQuery(form, logicalValues)
                ? new ProtectedFieldRuntime.PreparedQuery(
                        safePhysicalForm, withExpectedVersion(where, update.lock()), query.visibleFields())
                : null;
        return new PreparedBatchUpdate(
                write.physicalForm(), write.values(), logicalValues, where, update.lock(),
                request, ownerQuery);
    }

    SqlRequest renderBatchUpdate(DynamicForm form,
                                 BatchOptimisticUpdate update,
                                 DataScope effectiveScope) {
        return prepareBatchUpdate(form, update, effectiveScope).request();
    }

    Map<String, Object> prepareWriteValues(DynamicForm form,
                                           Map<String, Object> values,
                                           DataScope effectiveScope) {
        return guard.prepareWriteValues(form, values, effectiveScope);
    }

    ParameterConditionCompiler parameterCompiler(ParameterConditionPackage conditionPackage) {
        ParameterConditionPackage safePackage = Objects.requireNonNull(conditionPackage,
                                                                         "parameter condition package must not be null");
        ParameterConditionCompiler.Builder builder = ParameterConditionCompiler.builder()
                                                                                .terms(renderer.conditionRenderer()
                                                                                               .terms());
        safePackage.specs().forEach(builder::add);
        return builder.build();
    }

    private static ConditionGroup withExpectedVersion(ConditionGroup where, OptimisticLockOptions lock) {
        ConditionGroup expectedVersion = ConditionGroup.and()
                                                        .where(lock.field(), "=", lock.expectedValue())
                                                        .build();
        return ConditionGroups.and(where, expectedVersion);
    }

    private static ConditionGroup combineScope(ConditionGroup businessWhere, ConditionGroup scopeWhere) {
        return ConditionGroups.isEmpty(scopeWhere)
                ? businessWhere : ConditionGroups.and(businessWhere, scopeWhere);
    }

    record PreparedBatchScope(DataScope scope, ConditionGroup where) {

        PreparedBatchScope {
            scope = Objects.requireNonNull(scope, "prepared batch data scope must not be null");
            where = Objects.requireNonNull(where, "prepared batch scope condition must not be null");
        }
    }

    record PreparedBatchUpdate(DynamicForm form,
                               Map<String, Object> values,
                               Map<String, Object> logicalValues,
                               ConditionGroup where,
                               OptimisticLockOptions lock,
                               SqlRequest request,
                               ProtectedFieldRuntime.PreparedQuery ownerQuery) {
    }
}
