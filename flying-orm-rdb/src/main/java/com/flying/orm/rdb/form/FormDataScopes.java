package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionGroups;
import com.flying.orm.core.scope.DataScope;

import java.util.Objects;

/**
 * 把可信的 DataScope 合进表单条件。调用条件即使包含 OR，也只能在这个范围以内筛选数据。
 */
final class FormDataScopes {

    private FormDataScopes() {
    }

    static ConditionGroup apply(ConditionGroup where, DataScope scope) {
        ConditionGroup safeWhere = Objects.requireNonNull(where, "where condition must not be null");
        DataScope safeScope = Objects.requireNonNull(scope, "data scope must not be null");
        return safeScope.condition()
                        .map(scopeWhere -> ConditionGroups.and(safeWhere, scopeWhere))
                        .orElse(safeWhere);
    }
}
