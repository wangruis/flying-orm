package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionNode;
import com.flying.orm.core.condition.StructuredConditionCompiler;
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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 动态表单读写前的统一安全守卫。
 *
 * <p>它只做三件事：合并默认和本次 Scope、校验调用方能否读写字段、把租户与逻辑删除条件
 * 安全地并入业务条件。SQL 渲染和数据库执行不在这里，因而安全规则可以独立测试，也不会被
 * 某个新的查询入口绕过去。</p>
 *
 * <p>守卫在客户端构造时一次创建，保存的依赖和默认 Scope 都不可变。每次调用先生成一份
 * 有效 Scope 快照，后续字段校验与 WHERE 合并都使用同一份对象，避免异步订阅期间出现
 * “校验时一个租户、执行时另一个租户”的时间差。</p>
 */
final class FormScopeGuard {

    private final FormDataSqlRenderer renderer;

    private final StructuredConditionResolver structuredConditionResolver;

    private final DataScope defaultDataScope;

    FormScopeGuard(FormDataSqlRenderer renderer,
                   StructuredConditionResolver structuredConditionResolver,
                   DataScope defaultDataScope) {
        this.renderer = Objects.requireNonNull(renderer, "form data sql renderer must not be null");
        this.structuredConditionResolver = Objects.requireNonNull(
                structuredConditionResolver, "structured condition resolver must not be null");
        this.defaultDataScope = Objects.requireNonNull(defaultDataScope, "default data scope must not be null");
    }

    DataScope effectiveScope(DataScope scope) {
        return defaultDataScope.and(Objects.requireNonNull(scope, "data scope must not be null"));
    }

    ConditionGroup scopedWhere(DynamicForm form, ConditionGroup where, DataScope scope) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        ConditionGroup businessWhere = requireBusinessWhere(where);
        DataScope effectiveScope = effectiveScope(scope);
        requireTenantScope(safeForm, effectiveScope);
        return FormDataScopes.apply(safeForm, businessWhere, effectiveScope);
    }

    ScopedRead scopedRead(DynamicForm form, ConditionGroup where, DataScope scope) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        return buildScopedRead(safeForm, where, effectiveScope(scope));
    }

    ScopedRead scopedStructuredRead(DynamicForm form,
                                    StructuredConditionInput input,
                                    StructuredConditionPolicy policy,
                                    DataScope scope) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        DataScope effectiveScope = effectiveScope(scope);
        ConditionGroup where = compileStructuredCondition(safeForm, input, policy, effectiveScope.fields());
        return buildScopedRead(safeForm, where, effectiveScope);
    }

    ConditionGroup writableActiveWhere(DynamicForm form,
                                       Map<String, Object> values,
                                       ConditionGroup where,
                                       DataScope scope) {
        ConditionGroup businessWhere = requireBusinessWhere(where);
        DataScope effectiveScope = effectiveScope(scope);
        validateWritableValues(form, values, effectiveScope);
        requireTenantScope(form, effectiveScope);
        validateTenantUpdateValue(form, values, effectiveScope);
        return FormLogicDeletes.activeWhere(form, FormDataScopes.apply(form, businessWhere, effectiveScope));
    }

    /**
     * 批量更新的调用方已经在批次开始时合并好了 Scope，这里直接使用该快照，
     * 不能重新读取或再次拼接默认 Scope。
     */
    ConditionGroup batchUpdateWhere(DynamicForm form,
                                    BatchOptimisticUpdate update,
                                    DataScope effectiveScope) {
        BatchOptimisticUpdate safeUpdate = Objects.requireNonNull(update, "batch update must not be null");
        DataScope safeScope = Objects.requireNonNull(effectiveScope, "effective data scope must not be null");
        ConditionGroup businessWhere = requireBusinessWhere(safeUpdate.where());
        validateWritableValues(form, safeUpdate.values(), safeScope);
        requireTenantScope(form, safeScope);
        validateTenantUpdateValue(form, safeUpdate.values(), safeScope);
        return FormLogicDeletes.activeWhere(form, FormDataScopes.apply(form, businessWhere, safeScope));
    }

    Map<String, Object> prepareWriteValues(DynamicForm form,
                                           Map<String, Object> values,
                                           DataScope effectiveScope) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        Map<String, Object> safeValues = Objects.requireNonNull(values, "dynamic form values must not be null");
        DataScope safeScope = Objects.requireNonNull(effectiveScope, "data scope must not be null");
        LinkedHashMap<String, Object> prepared = new LinkedHashMap<>(safeValues);
        TenantDefinition tenant = safeForm.tenant().orElse(null);
        if (tenant == null) {
            validateWritableValues(safeForm, prepared, safeScope);
            return prepared;
        }

        TenantScope tenantScope = requiredTenantScope(safeForm, tenant, safeScope);
        String suppliedField = findTenantField(safeForm, prepared, tenant.fieldName());
        if (tenant.strategy() == TenantStrategy.AUTO) {
            if (suppliedField != null) {
                Object suppliedValue = prepared.remove(suppliedField);
                requireMatchingTenantValue(safeForm, tenant, suppliedValue, tenantScope.value());
            }
            validateWritableValues(safeForm, prepared, safeScope);
            prepared.put(tenant.fieldName(), tenantScope.value());
            return prepared;
        }

        validateWritableValues(safeForm, prepared, safeScope);
        if (suppliedField == null) {
            throw scopeError(ScopeErrorCode.TENANT_FIELD_REQUIRED,
                             safeForm,
                             tenant.fieldName(),
                             "tenant field [" + tenant.fieldName() + "] is required for form ["
                                     + safeForm.id() + "]");
        }
        requireMatchingTenantValue(safeForm, tenant, prepared.get(suppliedField), tenantScope.value());
        return prepared;
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
                form, safePolicy.withTerms(renderer.conditionTerms()), fields);
        // 深度和节点数必须由固定入口先检查，不能交给可替换 resolver 自己决定。
        StructuredConditionCompiler.validateStructure(input, protectedPolicy);
        return structuredConditionResolver.compile(form, input, protectedPolicy);
    }

    private static StructuredConditionPolicy protectFrontendConditionFields(DynamicForm form,
                                                                              StructuredConditionPolicy policy,
                                                                              FieldScope fields) {
        FieldScope safeFields = Objects.requireNonNull(fields, "field scope must not be null");
        StructuredConditionPolicy protectedPolicy = policy;
        if (!safeFields.unrestrictedRead()) {
            List<String> readableFields = new ArrayList<>();
            for (DynamicField field : form.fields()) {
                if (safeFields.canRead(field.name())) {
                    readableFields.add(field.name());
                }
            }
            protectedPolicy = protectedPolicy.allowOnlyFields(readableFields);
        }

        List<String> serverFields = new ArrayList<>(2);
        form.tenant().ifPresent(tenant -> serverFields.add(tenant.fieldName()));
        form.logicDelete().ifPresent(logicDelete -> serverFields.add(logicDelete.fieldName()));
        return serverFields.isEmpty() ? protectedPolicy : protectedPolicy.denyFields(serverFields);
    }

    /**
     * update/delete 必须带真正的业务谓词。租户、DataScope 和逻辑删除条件只能继续收窄，
     * 不能替调用方表达“我要改哪些业务数据”。
     */
    private static ConditionGroup requireBusinessWhere(ConditionGroup where) {
        ConditionGroup safeWhere = Objects.requireNonNull(where, "where condition must not be null");
        if (!containsBusinessPredicate(safeWhere)) {
            throw new IllegalArgumentException("write business where condition must not be empty");
        }
        return safeWhere;
    }

    private static boolean containsBusinessPredicate(ConditionGroup group) {
        for (ConditionNode child : group.children()) {
            if (!(child instanceof ConditionGroup nested) || containsBusinessPredicate(nested)) {
                return true;
            }
        }
        return false;
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

    private static DynamicForm readableForm(DynamicForm form, FieldScope fields) {
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
        DynamicForm.Builder builder = DynamicForm.builder(safeForm.id(), safeForm.table());
        readableFields.forEach(builder::addField);
        return builder.build();
    }

    private static void validateWritableValues(DynamicForm form,
                                               Map<String, Object> values,
                                               DataScope scope) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        Map<String, Object> safeValues = Objects.requireNonNull(values, "dynamic form values must not be null");
        FieldScope fields = scope.fields();
        if (fields.unrestrictedWrite()) {
            return;
        }
        for (String fieldName : safeValues.keySet()) {
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

    private static void validateTenantUpdateValue(DynamicForm form,
                                                  Map<String, Object> values,
                                                  DataScope scope) {
        TenantDefinition tenant = form.tenant().orElse(null);
        if (tenant == null) {
            return;
        }
        String suppliedField = findTenantField(form, values, tenant.fieldName());
        if (suppliedField == null) {
            return;
        }
        TenantScope tenantScope = requiredTenantScope(form, tenant, scope);
        requireMatchingTenantValue(form, tenant, values.get(suppliedField), tenantScope.value());
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

    private static void requireMatchingTenantValue(DynamicForm form,
                                                   TenantDefinition tenant,
                                                   Object suppliedValue,
                                                   Object scopedValue) {
        if (!tenantValuesEqual(suppliedValue, scopedValue)) {
            throw scopeError(ScopeErrorCode.TENANT_VALUE_MISMATCH,
                             form,
                             tenant.fieldName(),
                             "tenant field [" + tenant.fieldName() + "] does not match scope for form ["
                                     + form.id() + "]");
        }
    }

    private static boolean tenantValuesEqual(Object suppliedValue, Object scopedValue) {
        String suppliedText = canonicalText(suppliedValue);
        String scopedText = canonicalText(scopedValue);
        if (suppliedText != null && scopedText != null) {
            return suppliedText.equals(scopedText);
        }
        return Objects.deepEquals(suppliedValue, scopedValue);
    }

    /** 只规范化 TextValueCodec 明确定义的文本形状，不调用任意业务对象的 toString。 */
    private static String canonicalText(Object value) {
        return switch (value) {
            case CharSequence text -> text.toString();
            case Character character -> character.toString();
            case char[] characters -> new String(characters);
            case null, default -> null;
        };
    }

    private static ScopeAccessException scopeError(ScopeErrorCode code,
                                                   DynamicForm form,
                                                   String field,
                                                   String message) {
        return new ScopeAccessException(code, form.id(), field, message);
    }

    record ScopedRead(DynamicForm form, ConditionGroup where, DataScope scope) {
    }
}
