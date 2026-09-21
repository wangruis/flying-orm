package com.flying.orm.rdb.repository;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.rdb.form.BatchOptimisticUpdate;
import com.flying.orm.rdb.internal.mapping.EntityFieldNames;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.mapping.EntityMetadata;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 把实体上的 {@code @Version} 约定转换成表单客户端可执行的 {@link OptimisticLockOptions}。
 *
 * <p>乐观锁是显式模型：只有实体声明版本字段或调用方传入 lock 才启用。自动模式只支持数值版本递增；
 * 非数值版本必须由业务明确使用 assign，框架不会猜下一版本值。</p>
 */
final class RepositoryOptimisticLocks {

    private RepositoryOptimisticLocks() {
    }

    static <T> Optional<OptimisticLockOptions> incrementLock(EntityMetadata<T> metadata,
                                                              Map<String, Object> values) {
        Objects.requireNonNull(metadata, "repository entity metadata must not be null");
        Optional<EntityFieldMetadata> versionField = metadata.versionField();
        if (versionField.isEmpty()) {
            return Optional.empty();
        }

        EntityFieldMetadata field = versionField.get();
        if (!supportsIncrement(field)) {
            throw new IllegalStateException("entity version field [" + field.name()
                                                    + "] cannot auto increment, use explicit OptimisticLockOptions.assign");
        }

        // expectedValue 是调用方读到的旧版本，真正的新版本由 SQL 的原子表达式生成。
        Object expectedValue = value(values, field);
        if (expectedValue == null) {
            throw new IllegalArgumentException("entity version value must not be null: " + field.name());
        }
        return Optional.of(OptimisticLockOptions.increment(field.columnName(), expectedValue));
    }

    static Map<String, Object> withoutLockField(Map<String, Object> values, OptimisticLockOptions lock) {
        // 版本列由 optimistic lock renderer 单独生成，普通 SET 列表必须去掉它，避免重复赋值。
        String lockName = EntityFieldNames.key(lock.field());
        Map<String, Object> filtered = new LinkedHashMap<>();
        values.forEach((name, value) -> {
            if (!EntityFieldNames.key(name).equals(lockName)) {
                filtered.put(name, value);
            }
        });
        return filtered;
    }

    /**
     * Repository 批量更新不让调用方重复写主键条件和版本条件：主键负责定位行，版本负责检测并发覆盖。
     */
    static <T> BatchOptimisticUpdate batchUpdate(EntityMetadata<T> metadata, Map<String, Object> values) {
        return batchUpdate(metadata, values, values);
    }

    static <T> BatchOptimisticUpdate batchUpdate(EntityMetadata<T> metadata,
                                                  Map<String, Object> identityValues,
                                                  Map<String, Object> updateValues) {
        Objects.requireNonNull(metadata, "repository entity metadata must not be null");
        List<EntityFieldMetadata> idFields = metadata.fields().stream()
                                                     .filter(EntityFieldMetadata::primaryKey)
                                                     .toList();
        if (idFields.isEmpty()) {
            throw new IllegalStateException("batch optimistic update requires an entity id field: "
                                                    + metadata.type().getName());
        }
        OptimisticLockOptions lock = incrementLock(metadata, identityValues)
                .orElseThrow(() -> new IllegalStateException(
                        "batch optimistic update requires an entity version field: "
                                + metadata.type().getName()));
        ConditionGroup.Builder identity = ConditionGroup.and();
        List<String> excludedFields = new ArrayList<>(idFields.size() + 1);
        for (EntityFieldMetadata idField : idFields) {
            Object idValue = value(identityValues, idField);
            if (idValue == null) {
                throw new IllegalArgumentException("entity id value must not be null: " + idField.name());
            }
            identity.where(idField.columnName(), "=", idValue);
            excludedFields.add(idField.columnName());
        }
        excludedFields.add(lock.field());
        // 主键只负责 where 定位，版本只负责比较和递增，两者都不能再进入普通更新字段。
        Map<String, Object> writableValues = withoutFields(updateValues, excludedFields.toArray(String[]::new));
        return new BatchOptimisticUpdate(writableValues, identity.build(), lock);
    }

    private static Map<String, Object> withoutFields(Map<String, Object> values, String... fieldNames) {
        Map<String, Object> filtered = new LinkedHashMap<>();
        values.forEach((name, value) -> {
            String normalizedName = EntityFieldNames.key(name);
            for (String fieldName : fieldNames) {
                if (normalizedName.equals(EntityFieldNames.key(fieldName))) {
                    return;
                }
            }
            filtered.put(name, value);
        });
        return filtered;
    }

    private static Object value(Map<String, Object> values, EntityFieldMetadata field) {
        Map<String, Object> safeValues = Objects.requireNonNull(values, "repository values must not be null");
        String javaName = EntityFieldNames.key(field.name());
        String columnName = EntityFieldNames.key(field.columnName());
        for (Map.Entry<String, Object> entry : safeValues.entrySet()) {
            String currentName = EntityFieldNames.key(entry.getKey());
            if (currentName.equals(javaName) || currentName.equals(columnName)) {
                return entry.getValue();
            }
        }
        throw new IllegalArgumentException("entity field value is missing: " + field.name());
    }

    private static boolean supportsIncrement(EntityFieldMetadata field) {
        return switch (field.databaseType().logicalType()) {
            case BIG_INTEGER, INTEGER, DECIMAL -> true;
            default -> false;
        };
    }

}
