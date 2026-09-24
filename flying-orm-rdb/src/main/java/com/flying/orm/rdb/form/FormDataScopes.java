package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionGroups;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.internal.condition.ConditionNodes;

import java.util.Objects;

/**
 * 把可信的 DataScope 合进表单条件。调用条件即使包含 OR，也只能在这个范围以内筛选数据。
 */
final class FormDataScopes {

    private FormDataScopes() {
    }

    static ConditionGroup apply(DynamicForm form, ConditionGroup where, DataScope scope) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        ConditionGroup safeWhere = Objects.requireNonNull(where, "where condition must not be null");
        DataScope safeScope = Objects.requireNonNull(scope, "data scope must not be null");
        return safeScope.condition()
                        .map(scopeWhere -> ConditionGroups.and(safeWhere, trustedScope(safeForm, scopeWhere)))
                        .orElse(safeWhere);
    }

    /** 只在接管字段校验的 schema 中存在字段时消费标记；普通无 Scope 路径不扫描条件值。 */
    static ConditionGroup unwrapTrustedValues(DynamicForm form, ConditionGroup where, DataScope scope) {
        return form.protections().encryptedFields().isEmpty() && scope.condition().isEmpty()
                ? where : unwrapTrustedValues(where, form);
    }

    /** 在完整物理表单接管校验前，只移除本类创建的可信值标记。 */
    static ConditionGroup unwrapTrustedValues(ConditionGroup group) {
        return unwrapTrustedValues(group, null);
    }

    private static ConditionGroup unwrapTrustedValues(ConditionGroup group, DynamicForm form) {
        ConditionGroup safeGroup = Objects.requireNonNull(group, "condition group must not be null");
        return ConditionNodes.rewrite(safeGroup, term -> unwrapTrustedValue(term, form));
    }

    private static TermCondition unwrapTrustedValue(TermCondition term, DynamicForm form) {
        Object value = term.value();
        return value instanceof TrustedScopeValue trusted
                && (form == null || form.findField(term.field()).isPresent())
                ? TermCondition.of(term.identity(), term.operator(), trusted.value())
                : term;
    }

    private static ConditionGroup trustedScope(DynamicForm form, ConditionGroup group) {
        return ConditionNodes.rewrite(group, term -> form.findField(term.field()).isPresent()
                ? term : TermCondition.of(term.field(), term.operator(), new TrustedScopeValue(term.value())));
    }

    /** 仅由服务端 DataScope 合并边界创建，防止可信条件与业务条件共用无差别的缺字段放行。 */
    record TrustedScopeValue(Object value) {
    }
}
