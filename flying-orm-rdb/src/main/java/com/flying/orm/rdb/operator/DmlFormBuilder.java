package com.flying.orm.rdb.operator;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionNode;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.LogicDeleteDefinition;
import com.flying.orm.core.sql.render.SqlIdentifiers;
import com.flying.orm.rdb.lock.OptimisticLockOptions;

import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Operator 是轻门面，没有完整表单元数据时，用本次链式命令出现过的字段拼一个最小 DynamicForm。
 * 这个表单只用于复用统一的 FormClient 安全与渲染流程，不会写入元数据缓存，也不代表数据库完整表结构。
 *
 * <p>临时字段使用通用逻辑类型，实际 Java 值仍按参数绑定交给驱动。需要 JSON、Array、LOB 等字段语义时，
 * 应使用完整 DynamicForm/Repository 入口。</p>
 */
final class DmlFormBuilder {

    private DmlFormBuilder() {
    }

    static DynamicForm form(String table, Set<String> fields, OptimisticLockOptions lock) {
        return form(table, fields, lock, null);
    }

    static DynamicForm form(String table,
                            Set<String> fields,
                            OptimisticLockOptions lock,
                            LogicDeleteDefinition logicDelete) {
        return form(table, fields, lock, logicDelete, ConditionGroup.and().build());
    }

    static DynamicForm form(String table,
                            Set<String> fields,
                            OptimisticLockOptions lock,
                            LogicDeleteDefinition logicDelete,
                            ConditionGroup where) {
        String safeTable = SqlIdentifiers.requireIdentifier(table, "operator table");
        // LinkedHashSet 既去重又保留 set 调用顺序，使生成的 UPDATE SET 顺序和参数顺序稳定。
        Set<String> names = new LinkedHashSet<>(Objects.requireNonNull(fields, "operator fields must not be null"));
        collectConditionFields(Objects.requireNonNull(where, "operator where must not be null"), names);
        if (lock != null) {
            names.add(lock.field());
        }
        if (logicDelete != null) {
            names.add(logicDelete.fieldName());
        }

        DynamicForm.Builder builder = DynamicForm.builder(safeTable, safeTable);
        for (String name : names) {
            builder.addField(DynamicField.of(SqlIdentifiers.requireIdentifier(name, "operator field"), "VARCHAR"));
        }
        if (logicDelete != null) {
            builder.logicDelete(logicDelete.fieldName(), logicDelete.notDeletedValue(), logicDelete.deletedValue());
        }
        return builder.build();
    }

    private static void collectConditionFields(ConditionGroup group, Set<String> names) {
        for (ConditionNode child : group.children()) {
            if (child instanceof ConditionGroup nested) {
                collectConditionFields(nested, names);
            } else {
                names.add(((TermCondition) child).field());
            }
        }
    }
}
