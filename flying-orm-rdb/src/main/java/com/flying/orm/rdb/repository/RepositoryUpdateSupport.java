package com.flying.orm.rdb.repository;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.form.BatchUpdate;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.mapping.EntityMetadata;

import java.util.Map;
import java.util.Objects;

/**
 * 统一实体更新和删除的主键定位、字段排除和可选 {@code @Version} 规划。
 *
 * <p>乐观锁是显式模型：只有实体声明版本字段或调用方传入 lock 才启用。自动模式只支持数值版本递增；
 * 非数值版本必须由业务明确使用 assign，框架不会猜下一版本值。</p>
 */
final class RepositoryUpdateSupport {

    private RepositoryUpdateSupport() {
    }

    /** 在 PRE 之后读取实体；where 为空仅表示内部自动主键入口，显式条件由 Repository 校验。 */
    static <T> WriteSpec updateSpec(DynamicForm form, EntityMetadata<T> metadata,
                                    EntityValues<T> values, T entity, ConditionGroup where) {
        Map<String, Object> identity = where == null || metadata.versionField().isPresent()
                ? values.readIdentity(entity) : null;
        Map<String, Object> row = values.readForRepositoryUpdate(entity);
        ConditionGroup target = where == null ? RepositoryKeys.fromValues(metadata, identity) : where;
        OptimisticLockOptions lock = identity == null ? null : incrementLock(metadata, identity);
        WriteSpec spec = WriteSpec.updateOwned(form, row, target);
        return lock == null ? spec : spec.withLock(lock);
    }

    /** 删除只读取主键/版本计划，不读取或填充与定位无关的实体字段。 */
    static <T> WriteSpec deleteSpec(DynamicForm form, EntityMetadata<T> metadata,
                                    EntityValues<T> values, T entity, ConditionGroup where) {
        Map<String, Object> identity = where == null || metadata.versionField().isPresent()
                ? values.readIdentity(entity) : null;
        OptimisticLockOptions lock = identity == null ? null : incrementLock(metadata, identity);
        ConditionGroup target = where == null ? RepositoryKeys.fromValues(metadata, identity) : where;
        WriteSpec spec = WriteSpec.delete(form, target);
        return lock == null ? spec : spec.withLock(lock);
    }

    private static <T> OptimisticLockOptions incrementLock(EntityMetadata<T> metadata,
                                                              Map<String, Object> values) {
        EntityFieldMetadata field = metadata.versionField().orElse(null);
        if (field == null) return null;
        if (!supportsIncrement(field)) {
            throw new IllegalStateException("entity version field [" + field.name()
                                                    + "] cannot auto increment, use explicit OptimisticLockOptions.assign");
        }

        // expectedValue 是调用方读到的旧版本，真正的新版本由 SQL 的原子表达式生成。
        Object expectedValue = values.get(field.columnName());
        if (expectedValue == null) {
            throw new IllegalArgumentException("entity version value must not be null: " + field.name());
        }
        return OptimisticLockOptions.increment(field.columnName(), expectedValue);
    }

    /**
     * Repository 批量更新自动定位全部主键；仅声明版本字段时检测并发覆盖。
     */
    static <T> BatchUpdate batchUpdate(EntityMetadata<T> metadata, EntityValues<T> values, T entity) {
        Objects.requireNonNull(metadata, "repository entity metadata must not be null");
        Map<String, Object> identityValues = values.readIdentity(entity);
        Map<String, Object> writableValues = values.readForRepositoryUpdate(entity);
        ConditionGroup identity = RepositoryKeys.fromValues(metadata, identityValues);
        OptimisticLockOptions lock = incrementLock(metadata, identityValues);
        // 主键只负责 where 定位，版本只负责比较和递增，两者都不能再进入普通更新字段。
        return new BatchUpdate(writableValues, identity, lock);
    }

    private static boolean supportsIncrement(EntityFieldMetadata field) {
        return switch (field.databaseType().logicalType()) {
            case BIG_INTEGER, INTEGER, SMALL_INTEGER, DECIMAL -> true;
            default -> false;
        };
    }

}
