package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionGroups;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.TenantDefinition;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.scope.ScopeErrorCode;
import com.flying.orm.core.scope.TenantScope;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.form.spec.BatchOperation;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.lock.OptimisticLockMode;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 查询、写入与批量规划共用的 Scope 处理。
 *
 * <p>在同一所有者内合并默认和本次 Scope，校验字段与租户，再交给既有渲染器生成 SQL。
 * 每次调用沿用同一有效 Scope；批量共享固定范围的规划结果，不获取连接或执行数据库操作。</p>
 */
final class FormScopeSupport {

    private final FormDataSqlRenderer renderer;

    final StructuredConditionResolver structuredConditionResolver;

    private final DataScope defaultDataScope;

    FormScopeSupport(FormDataSqlRenderer renderer,
                     StructuredConditionResolver structuredConditionResolver,
                     DataScope defaultDataScope) {
        this.renderer = Objects.requireNonNull(renderer, "form data sql renderer must not be null");
        this.structuredConditionResolver = StructuredConditionResolvers.validating(structuredConditionResolver);
        this.defaultDataScope = Objects.requireNonNull(defaultDataScope, "default data scope must not be null");
    }

    DataScope effectiveScope(DataScope scope) {
        return defaultDataScope.and(Objects.requireNonNull(scope, "data scope must not be null"));
    }

    /** 使用调用方业务谓词和已合并范围。 */
    ConditionGroup applyWriteScope(DynamicForm form, ConditionGroup where, DataScope effectiveScope) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        requireTenantScope(safeForm, effectiveScope);
        return FormDataScopes.apply(safeForm, where, effectiveScope);
    }

    ScopedRead scopedRead(QuerySpec spec) {
        DataScope effective = effectiveScope(spec.scope());
        return buildScopedRead(spec.form(), businessWhere(spec, effective), effective);
    }

    GovernedRead governedRead(QuerySpec spec) {
        DataScope effective = effectiveScope(spec.scope());
        ConditionGroup business = businessWhere(spec, effective);
        return new GovernedRead(buildScopedRead(spec.form(), business, effective), business);
    }

    private ConditionGroup businessWhere(QuerySpec spec, DataScope scope) {
        return spec.structuredInput()
                .map(input -> {
                    ConditionGroup compiled = compileStructuredCondition(
                            spec.form(), input, spec.structuredPolicy().orElseThrow(), scope.fields());
                    return ConditionGroups.isEmpty(spec.where()) ? compiled : ConditionGroups.and(spec.where(), compiled);
                })
                .orElseGet(spec::where);
    }

    ScopedRead scopedRead(DynamicForm form, ConditionGroup where, DataScope scope) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        return buildScopedRead(safeForm, where, effectiveScope(scope));
    }

    /** 使用已合并范围，保留更新权限与逻辑删除语义。 */
    ConditionGroup applyUpdateScope(DynamicForm form,
                                    Map<String, Object> values,
                                    ConditionGroup where,
                                    DataScope effectiveScope) {
        validateWritableTenantUpdateValues(form, values, effectiveScope);
        return FormLogicDeletes.activeWhere(form, FormDataScopes.apply(form, where, effectiveScope));
    }

    /** Validates one batch row while leaving the fixed subscription Scope to its shared plan. */
    ConditionGroup batchUpdateBusinessWhere(DynamicForm form,
                                            BatchUpdate update,
                                            DataScope effectiveScope) {
        BatchUpdate safeUpdate = Objects.requireNonNull(update, "batch update must not be null");
        DataScope safeScope = Objects.requireNonNull(effectiveScope, "effective data scope must not be null");
        validateWritableTenantUpdateValues(form, safeUpdate.ownedValues(), safeScope);
        validateTenantLock(form, safeUpdate.lock(), safeScope);
        return safeUpdate.where();
    }

    /** 乐观锁的 SET 同样不能改变已声明的租户；普通内部版本字段不受字段写白名单限制。 */
    void validateTenantLock(DynamicForm form, OptimisticLockOptions lock, DataScope scope) {
        if (lock == null) {
            return;
        }
        scope.tenantScope(lock.field()).ifPresent(tenant -> {
            if (lock.mode() == OptimisticLockMode.INCREMENT) {
                throw scopeError(ScopeErrorCode.TENANT_VALUE_MISMATCH, form, tenant.field(),
                        "optimistic lock must not increment tenant field [" + tenant.field() + "]");
            }
            requireMatchingTenant(form, tenant, lock.nextValue());
        });
    }

    Map<String, Object> prepareWriteValues(DynamicForm form,
                                           Map<String, Object> values,
                                           DataScope effectiveScope) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        Map<String, Object> safeValues = Objects.requireNonNull(values, "dynamic form values must not be null");
        DataScope safeScope = Objects.requireNonNull(effectiveScope, "data scope must not be null");
        TenantDefinition tenant = safeForm.tenant().orElse(null);
        if (tenant == null) {
            validateWritableValues(safeForm, safeValues, safeScope);
            return safeValues;
        }

        TenantScope tenantScope = requiredTenantScope(safeForm, tenant, safeScope);
        String suppliedField = findTenantField(safeForm, safeValues, tenant.fieldName());
        if (tenant.strategy() == TenantStrategy.AUTO) {
            if (suppliedField != null) {
                requireMatchingTenant(safeForm, tenantScope, safeValues.get(suppliedField));
                if (suppliedField.equals(tenant.fieldName())) {
                    validateWritableValues(safeForm, safeValues, safeScope, suppliedField);
                    return safeValues;
                }
            }
            LinkedHashMap<String, Object> prepared = new LinkedHashMap<>(safeValues);
            if (suppliedField != null) {
                prepared.remove(suppliedField);
            }
            validateWritableValues(safeForm, prepared, safeScope);
            prepared.put(tenant.fieldName(), tenantScope.value());
            return prepared;
        }

        validateWritableValues(safeForm, safeValues, safeScope);
        if (suppliedField == null) {
            throw scopeError(ScopeErrorCode.TENANT_FIELD_REQUIRED,
                             safeForm,
                             tenant.fieldName(),
                             "tenant field [" + tenant.fieldName() + "] is required for form ["
                                     + safeForm.id() + "]");
        }
        requireMatchingTenant(safeForm, tenantScope, safeValues.get(suppliedField));
        return safeValues;
    }

    PreparedBatchUpdate prepareBatchUpdate(DynamicForm form,
                                           BatchUpdate update,
                                           DataScope effectiveScope) {
        DynamicForm physicalForm = renderer.protection().physicalForm(form);
        FormProtectionSqlSupport.WriteOperation protection = form.protections().encryptedFields().isEmpty()
                ? null : renderer.protection().writeOperation(
                form, physicalForm, effectiveScope);
        return prepareBatchUpdate(form, physicalForm, update,
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
        ConditionGroup where = form.protections().encryptedFields().isEmpty()
                ? FormDataScopes.unwrapTrustedValues(form, scopeWhere, safeScope)
                : renderer.protection().prepareQuery(form, safePhysicalForm, scopeWhere, safeScope, List.of()).where();
        return new PreparedBatchScope(safeScope, where);
    }

    /** 批量插入的 SQL 与目标 Scope 只规划一次；JDBC/R2DBC 共用相同分支规则。 */
    BatchInsertPlan prepareBatchInsert(BatchSpec spec, Map<String, Object> logicalValues,
                                       FormPreparedWrite write, Map<String, Object> sourceRow, DataScope scope) {
        DynamicForm physicalForm = write == null ? spec.form() : write.physicalForm();
        Map<String, Object> values = write == null ? logicalValues : write.values();
        if (spec.ignoresConflicts()) {
            return renderer.batchRenderer.insertIgnoringConflictsPlan(
                    physicalForm, values, spec.conflictFields());
        }
        if (spec.operation() == BatchOperation.INSERT || BatchInsertPlan.generatedInsert(spec, sourceRow)) {
            return renderer.batchRenderer.insertPlan(physicalForm, values);
        }
        ConditionGroup updateCondition = spec.updateCondition();
        if (!ConditionGroups.isEmpty(updateCondition)) {
            updateCondition = write == null
                    ? FormDataScopes.unwrapTrustedValues(spec.form(), updateCondition, scope)
                    : renderer.protection().prepareQuery(spec.form(), physicalForm,
                            updateCondition, scope, List.of()).where();
        }
        return renderer.batchRenderer.upsertPlan(spec.form(), logicalValues, physicalForm,
                values, sourceRow, scope.condition().isEmpty() ? null
                        : prepareBatchScope(spec.form(), physicalForm, scope).where(),
                spec.conflictFields(), updateCondition);
    }

    PreparedBatchUpdate prepareBatchUpdate(DynamicForm form,
                                           DynamicForm physicalForm,
                                           BatchUpdate update,
                                           PreparedBatchScope batchScope,
                                           FormProtectionSqlSupport.WriteOperation protection) {
        DynamicForm safePhysicalForm = Objects.requireNonNull(
                physicalForm, "physical form must not be null");
        PreparedBatchScope safeBatchScope = Objects.requireNonNull(
                batchScope, "prepared batch scope must not be null");
        ConditionGroup businessWhere = batchUpdateBusinessWhere(
                form, update, safeBatchScope.scope());
        Map<String, Object> logicalValues = update.ownedValues();
        if (protection == null) {
            ConditionGroup where = FormLogicDeletes.activeWhere(form, combineScope(
                    FormDataScopes.unwrapTrustedValues(form, businessWhere, safeBatchScope.scope()), safeBatchScope.where()));
            SqlRequest request = update.lock() == null ? renderer.update(form, logicalValues, where)
                    : renderer.update(form, logicalValues, where, update.lock());
            return new PreparedBatchUpdate(form, logicalValues, null, update.lock(), request, null);
        }
        FormPreparedWrite write = protection.prepare(logicalValues);
        ProtectedFieldRuntime.PreparedQuery query = renderer.protection().prepareQuery(
                form, safePhysicalForm, businessWhere, safeBatchScope.scope(), List.of());
        ConditionGroup where = FormLogicDeletes.activeWhere(
                form, combineScope(query.where(), safeBatchScope.where()));
        SqlRequest request = update.lock() == null ? renderer.update(write.physicalForm(), write.values(), where)
                : renderer.update(write.physicalForm(), write.values(), where, update.lock());
        ProtectedFieldRuntime.PreparedQuery ownerQuery = !write.containsTokens().isEmpty()
                ? new ProtectedFieldRuntime.PreparedQuery(
                        safePhysicalForm, withExpectedVersion(where, update.lock()), query.visibleFields())
                : null;
        return new PreparedBatchUpdate(
                write.physicalForm(), write.values(), write, update.lock(),
                request, ownerQuery);
    }

    private ScopedRead buildScopedRead(DynamicForm form, ConditionGroup where, DataScope effectiveScope) {
        requireTenantScope(form, effectiveScope);
        DynamicForm readableForm = readableForm(form, effectiveScope.fields());
        ConditionGroup scopedWhere = FormDataScopes.apply(readableForm, where, effectiveScope);
        return new ScopedRead(readableForm, FormLogicDeletes.activeWhere(form, scopedWhere), effectiveScope);
    }

    private ConditionGroup compileStructuredCondition(DynamicForm form,
                                                      StructuredConditionInput input,
                                                      StructuredConditionPolicy policy,
                                                      FieldScope fields) {
        StructuredConditionPolicy safePolicy = Objects.requireNonNull(
                policy, "structured condition policy must not be null");
        StructuredConditionPolicy protectedPolicy = protectFrontendConditionFields(
                form, safePolicy.withTerms(renderer.conditionRenderer().terms()), fields);
        return structuredConditionResolver.compile(form, input, protectedPolicy);
    }

    static StructuredConditionPolicy protectFrontendConditionFields(DynamicForm form,
                                                                              StructuredConditionPolicy policy,
                                                                              FieldScope fields) {
        FieldScope safeFields = Objects.requireNonNull(fields, "field scope must not be null");
        List<String> deniedFields = new ArrayList<>();
        if (!safeFields.unrestrictedRead()) {
            for (DynamicField field : form.fields()) {
                if (!safeFields.canRead(field.name())) {
                    deniedFields.add(field.name());
                }
            }
        }

        // Scope only adds restrictions; replacing the allowlist would discard the caller's policy.
        form.tenant().ifPresent(tenant -> deniedFields.add(tenant.fieldName()));
        form.logicDelete().ifPresent(logicDelete -> deniedFields.add(logicDelete.fieldName()));
        return deniedFields.isEmpty() ? policy : policy.denyFields(deniedFields);
    }

    private static void requireTenantScope(DynamicForm form, DataScope scope) {
        form.tenant().ifPresent(tenant -> {
            if (scope.tenantScope(tenant.fieldName()).isEmpty()) {
                throw scopeError(ScopeErrorCode.TENANT_SCOPE_REQUIRED,
                                 form,
                                 tenant.fieldName(),
                                 "tenant scope is required for form [" + form.id() + "]");
            }
        });
    }

    static DynamicForm readableForm(DynamicForm form, FieldScope fields) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        if (fields.unrestrictedRead()) {
            return safeForm;
        }
        if (safeForm.fields().isEmpty()) {
            throw scopeError(ScopeErrorCode.FORM_FIELDS_REQUIRED,
                             safeForm,
                             null,
                             "field scope needs form fields for [" + safeForm.id() + "]");
        }
        List<DynamicField> readableFields = safeForm.fields()
                                                   .stream()
                                                   .filter(field -> fields.canRead(field.name()))
                                                   .toList();
        if (readableFields.isEmpty()) {
            throw scopeError(ScopeErrorCode.NO_READABLE_FIELDS,
                             safeForm,
                             null,
                             "field scope leaves no readable fields for form [" + safeForm.id() + "]");
        }
        DynamicForm.Builder builder = DynamicForm.relationalBuilder(safeForm.id(), safeForm.relationIdentity())
                .namingSource(safeForm.namingSource()).features(safeForm.features());
        readableFields.forEach(builder::addField);
        return builder.build();
    }

    private static void validateWritableValues(DynamicForm form,
                                               Map<String, Object> values,
                                               DataScope scope) {
        validateWritableValues(form, values, scope, null);
    }

    private static void validateWritableValues(DynamicForm form,
                                               Map<String, Object> values,
                                               DataScope scope,
                                               String serverOwnedField) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        Map<String, Object> safeValues = Objects.requireNonNull(values, "dynamic form values must not be null");
        FieldScope fields = scope.fields();
        if (fields.unrestrictedWrite()) {
            return;
        }
        for (String fieldName : safeValues.keySet()) {
            if (Objects.equals(fieldName, serverOwnedField)) {
                continue;
            }
            DynamicField field = safeForm.field(fieldName);
            if (!fields.canWrite(field.name())) {
                throw scopeError(ScopeErrorCode.FIELD_NOT_WRITABLE,
                                 safeForm,
                                 field.name(),
                                 "field [" + field.name() + "] is not writable for form ["
                                         + safeForm.id() + "]");
            }
        }
    }

    private static void validateWritableTenantUpdateValues(DynamicForm form,
                                                            Map<String, Object> values,
                                                            DataScope scope) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        Map<String, Object> safeValues = Objects.requireNonNull(values, "dynamic form values must not be null");
        DataScope safeScope = Objects.requireNonNull(scope, "data scope must not be null");
        FieldScope fields = safeScope.fields();
        TenantDefinition tenant = safeForm.tenant().orElse(null);
        // 未启用范围或加密时，字段与表达式由渲染器校验，不逐字段执行空策略。
        if (tenant == null && safeScope.empty() && safeForm.protections().encryptedFields().isEmpty()) return;
        TenantScope formTenantScope = tenant == null ? null : requiredTenantScope(safeForm, tenant, safeScope);
        String suppliedTenantField = null;
        for (Map.Entry<String, Object> entry : safeValues.entrySet()) {
            String fieldName = entry.getKey();
            FormUpdateExpressions.validate(safeForm, fieldName, entry.getValue(), fields);
            if (!fields.unrestrictedWrite()) {
                DynamicField field = safeForm.field(fieldName);
                if (!fields.canWrite(field.name())) {
                    throw scopeError(ScopeErrorCode.FIELD_NOT_WRITABLE,
                                     safeForm,
                                     field.name(),
                                     "field [" + field.name() + "] is not writable for form ["
                                             + safeForm.id() + "]");
                }
            }
            if (tenant != null && fieldName != null
                    && fieldName.trim().equalsIgnoreCase(tenant.fieldName())) {
                if (suppliedTenantField != null) {
                    throw scopeError(ScopeErrorCode.DUPLICATE_TENANT_FIELD,
                                     safeForm,
                                     tenant.fieldName(),
                                     "duplicate tenant field values for [" + tenant.fieldName() + "]");
                }
                suppliedTenantField = fieldName;
                requireMatchingTenant(safeForm, formTenantScope, entry.getValue());
                continue;
            }
            safeScope.tenantScope(fieldName).ifPresent(tenantScope ->
                    requireMatchingTenant(safeForm, tenantScope, entry.getValue()));
        }
    }

    private static TenantScope requiredTenantScope(DynamicForm form,
                                                    TenantDefinition tenant,
                                                    DataScope scope) {
        return scope.tenantScope(tenant.fieldName())
                    .orElseThrow(() -> scopeError(ScopeErrorCode.TENANT_SCOPE_REQUIRED,
                                                  form,
                                                  tenant.fieldName(),
                                                  "tenant scope is required for form [" + form.id() + "]"));
    }

    private static void requireMatchingTenant(DynamicForm form, TenantScope tenant, Object suppliedValue) {
        if (!tenant.matches(suppliedValue)) {
            throw scopeError(ScopeErrorCode.TENANT_VALUE_MISMATCH, form, tenant.field(),
                    "tenant field [" + tenant.field() + "] does not match scope for form [" + form.id() + "]");
        }
    }

    private static String findTenantField(DynamicForm form, Map<String, Object> values, String tenantField) {
        String matchedField = null;
        for (String fieldName : values.keySet()) {
            if (fieldName == null || !fieldName.trim().equalsIgnoreCase(tenantField)) {
                continue;
            }
            if (matchedField != null) {
                throw scopeError(ScopeErrorCode.DUPLICATE_TENANT_FIELD,
                                 form,
                                 tenantField,
                                 "duplicate tenant field values for [" + tenantField + "]");
            }
            matchedField = fieldName;
        }
        return matchedField;
    }

    private static ScopeAccessException scopeError(ScopeErrorCode code,
                                                   DynamicForm form,
                                                   String field,
                                                   String message) {
        return new ScopeAccessException(code, form.id(), field, message);
    }

    static ConditionGroup withExpectedVersion(ConditionGroup where, OptimisticLockOptions lock) {
        if (lock == null) {
            return where;
        }
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
                               FormPreparedWrite write,
                               OptimisticLockOptions lock,
                               SqlRequest request,
                               ProtectedFieldRuntime.PreparedQuery ownerQuery) {
    }

    /** 只在 governed 入口创建，普通 CRUD 的 ScopedRead 布局保持不变。 */
    record GovernedRead(ScopedRead read, ConditionGroup businessWhere) {

        GovernedRead {
            read = Objects.requireNonNull(read, "governed scoped read must not be null");
            businessWhere = Objects.requireNonNull(
                    businessWhere, "governed business where condition must not be null");
        }
    }
}
