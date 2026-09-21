package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;

import java.util.Objects;

/**
 * Scope、安全字段裁剪和逻辑删除条件都处理完成后的只读查询快照。
 *
 * <p>它不包含执行器类型，JDBC 和 R2DBC 可以消费同一份表单及 WHERE，避免两条执行链分别拼接安全条件。</p>
 */
record ScopedRead(DynamicForm form, ConditionGroup where, DataScope scope) {

    ScopedRead {
        form = Objects.requireNonNull(form, "scoped dynamic form must not be null");
        where = Objects.requireNonNull(where, "scoped where condition must not be null");
        scope = Objects.requireNonNull(scope, "scoped data scope must not be null");
    }
}
