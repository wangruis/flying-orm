package com.flying.orm.rdb.internal.mapping;

import com.flying.orm.rdb.mapping.EntityMetadata;

import java.util.Objects;

/**
 * 实体元数据的公共解析入口。
 *
 * <p>这里只保留无缓存编译入口。注解读取、字段定义、租户声明和类型推断由包内协作者分别处理，
 * 注册表仍然决定何时缓存结果，因此实体注解和访问器语义不会因拆分发生变化。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public final class EntityMetadataResolver {

    private EntityMetadataResolver() {
    }

    /** 注册表缓存未命中时编译一份不可变实体元数据。 */
    public static <T> EntityMetadata<T> createUncached(Class<T> type) {
        return new EntityMetadataCompiler(EntityNamingStrategy.SNAKE_CASE).compile(
                Objects.requireNonNull(type, "entity type must not be null"));
    }
}
