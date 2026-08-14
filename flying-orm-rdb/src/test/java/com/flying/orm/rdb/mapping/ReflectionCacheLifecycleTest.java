package com.flying.orm.rdb.mapping;

import com.flying.orm.rdb.internal.mapping.EntityMetadataResolver;
import com.flying.orm.rdb.internal.mapping.EntityValues;

import org.junit.jupiter.api.Test;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * 反射计划会引用实体 Class，缓存也必须跟着 Class 生命周期释放，不能用永久静态 Map 留住动态类加载器。
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
class ReflectionCacheLifecycleTest {

    @Test
    void reflectionCachesAreOwnedByTheConfiguredClientRegistry() {
        assertNoClassValueCache(MappingPlan.class);
        assertNoClassValueCache(EntityValues.class);
        assertNoClassValueCache(EntityMetadataResolver.class);
    }

    private static void assertNoClassValueCache(Class<?> owner) {
        assertFalse(Arrays.stream(owner.getDeclaredFields())
                          .anyMatch(field -> field.getType() == ClassValue.class),
                    owner.getSimpleName());
    }
}
