package com.flying.orm.rdb.repository;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionNode;
import com.flying.orm.core.condition.LogicalOperator;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.mapping.EntityMetadata;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 统一实体 Repository 的逻辑删除规则：读、更新和逻辑删除都自动追加“未删除”条件，删除则生成标记值。
 *
 * <p>如果 DynamicForm 已经声明逻辑删除，以表单定义为准，实体注解不重复追加。这里只组合条件和数据，
 * 不渲染 SQL；最终仍由 FormClient 做字段、安全范围和参数绑定检查。</p>
 */
final class RepositoryLogicDeletes {

    private RepositoryLogicDeletes() {
    }

    static <T> ConditionGroup activeWhere(EntityMetadata<T> metadata, ConditionGroup where) {
        Optional<EntityFieldMetadata> field = logicDeleteField(metadata);
        if (field.isEmpty()) {
            return where;
        }
        ConditionGroup.Builder builder = ConditionGroup.and();
        // 原来就是 AND 的条件可以平铺进去；原来是 OR 的条件必须整体塞进去，
        // 否则 deleted = 0 可能只约束到最后一个分支，逻辑删除就被绕开了。
        if (where.operator() == LogicalOperator.AND) {
            for (ConditionNode child : where.children()) {
                builder.add(child);
            }
        } else {
            builder.add(where);
        }
        EntityFieldMetadata logicDelete = field.get();
        builder.where(logicDelete.columnName(), "=", logicDelete.logicNotDeletedValue());
        return builder.build();
    }

    static <T> ConditionGroup activeWhere(EntityMetadata<T> metadata, DynamicForm form, ConditionGroup where) {
        if (form.logicDelete().isPresent()) {
            return where;
        }
        return activeWhere(metadata, where);
    }

    static <T> Optional<Map<String, Object>> deleteValues(EntityMetadata<T> metadata) {
        // 用 Map 表达一次普通 update，复用 FieldScope、租户保护和执行观测，不另开删除旁路。
        return logicDeleteField(metadata).map(field -> {
            Map<String, Object> values = new LinkedHashMap<>();
            values.put(field.columnName(), field.logicDeletedValue());
            return values;
        });
    }

    static <T> Optional<Map<String, Object>> deleteValues(EntityMetadata<T> metadata, DynamicForm form) {
        if (form.logicDelete().isPresent()) {
            return Optional.empty();
        }
        return deleteValues(metadata);
    }

    private static <T> Optional<EntityFieldMetadata> logicDeleteField(EntityMetadata<T> metadata) {
        return metadata.logicDeleteField();
    }
}
