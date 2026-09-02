package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionGroups;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.LogicDeleteDefinition;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 表单级逻辑删除组合器。这里不渲染 SQL，只把“未删除”条件和“已删除”值补到正常表单操作里，
 * 从而继续复用 scope、字段权限、乐观锁和执行保护。
 */
final class FormLogicDeletes {

    private FormLogicDeletes() {
    }

    static ConditionGroup activeWhere(DynamicForm form, ConditionGroup where) {
        Optional<LogicDeleteDefinition> definition = form.logicDelete();
        if (definition.isEmpty()) {
            return where;
        }
        LogicDeleteDefinition logicDelete = definition.get();
        ConditionGroup active = ConditionGroup.and()
                                              .where(logicDelete.fieldName(), "=", logicDelete.notDeletedValue())
                                              .build();
        // ConditionGroups.and 会把外部 OR 条件整体包住，不能用 OR 绕过未删除约束。
        return ConditionGroups.and(where, active);
    }

    static Optional<Map<String, Object>> deleteValues(DynamicForm form) {
        return form.logicDelete().map(definition -> {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put(definition.fieldName(), definition.deletedValue());
            return values;
        });
    }
}
