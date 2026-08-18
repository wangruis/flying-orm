package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionGroups;
import com.flying.orm.core.condition.ConditionNode;
import com.flying.orm.core.condition.LogicalOperator;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;

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

    private static ConditionGroup trustedScope(DynamicForm form, ConditionGroup group) {
        ConditionGroup.Builder builder = group.operator() == LogicalOperator.AND
                ? ConditionGroup.and() : ConditionGroup.or();
        for (ConditionNode child : group.children()) {
            if (child instanceof ConditionGroup nested) {
                builder.add(trustedScope(form, nested));
                continue;
            }
            TermCondition term = (TermCondition) child;
            builder.add(form.findField(term.field()).isPresent()
                                ? term
                                : TermCondition.of(term.field(), term.operator(),
                                                   new TrustedScopeValue(term.value())));
        }
        return builder.build();
    }

    /** 仅由服务端 DataScope 合并边界创建，防止可信条件与业务条件共用无差别的缺字段放行。 */
    record TrustedScopeValue(Object value) {
    }
}
