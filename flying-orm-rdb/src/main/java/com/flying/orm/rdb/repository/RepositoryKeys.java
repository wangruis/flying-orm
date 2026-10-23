package com.flying.orm.rdb.repository;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.mapping.EntityMetadata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** 实体单条及批量操作共用的完整主键定位；不执行 SQL，也不猜测缺失的键值。 */
final class RepositoryKeys {

    private RepositoryKeys() {
    }

    static ConditionGroup fromKey(EntityMetadata<?> metadata, Object key) {
        Objects.requireNonNull(key, "repository key must not be null");
        List<EntityFieldMetadata> fields = requireKeys(metadata);
        if (!(key instanceof Map<?, ?> supplied)) {
            if (fields.size() != 1) {
                throw new IllegalArgumentException("composite entity key requires a Map of all primary fields");
            }
            return ConditionGroup.and().where(fields.getFirst().columnName(), "=", key).build();
        }
        return fromValues(metadata, keyFields(metadata, supplied, fields));
    }

    /** 单主键使用 IN；复合键保持每组键之间的配对关系，不生成逐键执行任务。 */
    static ConditionGroup fromKeys(EntityMetadata<?> metadata, Collection<?> keys) {
        List<EntityFieldMetadata> fields = requireKeys(metadata);
        if (fields.size() == 1) {
            EntityFieldMetadata field = fields.getFirst();
            List<Object> values = new ArrayList<>(keys.size());
            for (Object key : keys) {
                Objects.requireNonNull(key, "repository key must not be null");
                Object value = key instanceof Map<?, ?> supplied
                        ? keyFields(metadata, supplied, fields).get(field.columnName()) : key;
                if (value == null) {
                    throw new IllegalArgumentException("entity id value must not be null: " + field.name());
                }
                values.add(value);
            }
            return ConditionGroup.and().where(field.columnName(), "in", values).build();
        }
        ConditionGroup.Builder where = ConditionGroup.or();
        for (Object key : keys) {
            where.add(fromKey(metadata, key));
        }
        return where.build();
    }

    private static Map<String, Object> keyFields(EntityMetadata<?> metadata, Map<?, ?> supplied,
                                                   List<EntityFieldMetadata> fields) {
        if (supplied.size() != fields.size()) {
            throw new IllegalArgumentException("entity key must contain exactly all primary fields");
        }
        Map<String, Object> canonical = new LinkedHashMap<>();
        for (Map.Entry<?, ?> entry : supplied.entrySet()) {
            if (!(entry.getKey() instanceof String name)) {
                throw new IllegalArgumentException("entity key field name must be a String");
            }
            EntityFieldMetadata field = metadata.field(name);
            if (!field.primaryKey() || canonical.containsKey(field.columnName())) {
                throw new IllegalArgumentException("entity key contains a non-primary or repeated field: " + name);
            }
            canonical.put(field.columnName(), entry.getValue());
        }
        return canonical;
    }

    /** 输入只来自实体定位计划或 keyFields，键已统一为数据库列名。 */
    static ConditionGroup fromValues(EntityMetadata<?> metadata, Map<String, Object> values) {
        ConditionGroup.Builder where = ConditionGroup.and();
        for (EntityFieldMetadata field : requireKeys(metadata)) {
            Object key = values.get(field.columnName());
            if (key == null) {
                throw new IllegalArgumentException("entity id value must not be null: " + field.name());
            }
            where.where(field.columnName(), "=", key);
        }
        return where.build();
    }

    private static List<EntityFieldMetadata> requireKeys(EntityMetadata<?> metadata) {
        List<EntityFieldMetadata> fields = metadata.primaryKeyFields();
        if (fields.isEmpty()) {
            throw new IllegalStateException("operation requires an entity id field: " + metadata.type().getName());
        }
        return fields;
    }
}
