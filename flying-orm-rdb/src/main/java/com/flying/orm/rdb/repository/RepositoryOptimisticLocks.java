package com.flying.orm.rdb.repository;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.rdb.form.BatchOptimisticUpdate;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.mapping.EntityMetadata;

import java.util.LinkedHashMap;
import java.util.Locale;
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
        if (!isNumber(field.dataType())) {
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
        String lockName = normalize(lock.field());
        Map<String, Object> filtered = new LinkedHashMap<>();
        values.forEach((name, value) -> {
            if (!normalize(name).equals(lockName)) {
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
        EntityFieldMetadata idField = metadata.idField()
                                              .orElseThrow(() -> new IllegalStateException(
                                                      "batch optimistic update requires an entity id field: "
                                                              + metadata.type().getName()));
        OptimisticLockOptions lock = incrementLock(metadata, identityValues)
                .orElseThrow(() -> new IllegalStateException(
                        "batch optimistic update requires an entity version field: "
                                + metadata.type().getName()));
        Object idValue = value(identityValues, idField);
        if (idValue == null) {
            throw new IllegalArgumentException("entity id value must not be null: " + idField.name());
        }
        // 主键只负责 where 定位，版本只负责比较和递增，两者都不能再进入普通更新字段。
        Map<String, Object> writableValues = withoutFields(updateValues, idField.columnName(), lock.field());
        return new BatchOptimisticUpdate(writableValues,
                                         ConditionGroup.and()
                                                       .where(idField.columnName(), "=", idValue)
                                                       .build(),
                                         lock);
    }

    private static Map<String, Object> withoutFields(Map<String, Object> values, String... fieldNames) {
        Map<String, Object> filtered = new LinkedHashMap<>();
        values.forEach((name, value) -> {
            String normalizedName = normalize(name);
            for (String fieldName : fieldNames) {
                if (normalizedName.equals(normalize(fieldName))) {
                    return;
                }
            }
            filtered.put(name, value);
        });
        return filtered;
    }

    private static Object value(Map<String, Object> values, EntityFieldMetadata field) {
        Map<String, Object> safeValues = Objects.requireNonNull(values, "repository values must not be null");
        String javaName = normalize(field.name());
        String columnName = normalize(field.columnName());
        for (Map.Entry<String, Object> entry : safeValues.entrySet()) {
            String currentName = normalize(entry.getKey());
            if (currentName.equals(javaName) || currentName.equals(columnName)) {
                return entry.getValue();
            }
        }
        throw new IllegalArgumentException("entity version value is missing: " + field.name());
    }

    private static boolean isNumber(String dataType) {
        return switch (dataType.toUpperCase(Locale.ROOT)) {
            case "BIGINT", "INTEGER", "DECIMAL" -> true;
            default -> false;
        };
    }

    private static String normalize(String name) {
        // 同时兼容 Java camelCase 与数据库 snake_case/kebab-case，只用于元数据匹配，不用于 SQL 输出。
        return Objects.requireNonNull(name, "repository field name must not be null")
                      .replace("_", "")
                      .replace("-", "")
                      .toLowerCase(Locale.ROOT);
    }
}
