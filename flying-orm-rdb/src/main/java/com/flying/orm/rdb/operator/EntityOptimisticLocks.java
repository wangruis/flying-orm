package com.flying.orm.rdb.operator;

import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.MappingException;

import java.util.Locale;
import java.util.Objects;

/**
 * 为 Lambda 实体 DML 创建经过元数据校验的乐观锁选项。
 *
 * <p>自动递增只适用于数值版本列。该校验在 SQL 渲染前完成，避免把字符串、时间等版本字段错误地渲染为
 * {@code version = version + 1}。期望版本也不得为 {@code null}，否则数据库的三值逻辑会让冲突判断失真。</p>
 *
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
final class EntityOptimisticLocks {

    private EntityOptimisticLocks() {
    }

    /**
     * 根据实体的唯一版本字段创建数值递增锁。
     *
     * @param metadata 当前客户端缓存的实体元数据
     * @param expectedVersion 业务读取到的旧版本
     * @return 可直接交给表单客户端执行的乐观锁选项
     */
    static OptimisticLockOptions increment(EntityMetadata<?> metadata, Object expectedVersion) {
        EntityMetadata<?> safeMetadata = Objects.requireNonNull(metadata, "entity metadata must not be null");
        EntityFieldMetadata version = safeMetadata.versionField().orElseThrow(() -> new MappingException(
                "entity has no @Version field: " + safeMetadata.type().getName()));
        if (expectedVersion == null) {
            throw new MappingException("expected entity version must not be null: "
                                               + safeMetadata.type().getName() + "." + version.name());
        }
        if (!isNumeric(version.dataType())) {
            throw new MappingException("entity version field cannot be incremented automatically: "
                                               + safeMetadata.type().getName() + "." + version.name()
                                               + " (" + version.dataType() + ")");
        }
        return OptimisticLockOptions.increment(version.columnName(), expectedVersion);
    }

    private static boolean isNumeric(String dataType) {
        return switch (Objects.requireNonNull(dataType, "version data type must not be null")
                              .toUpperCase(Locale.ROOT)) {
            case "BIGINT", "INTEGER", "DECIMAL" -> true;
            default -> false;
        };
    }
}
