package com.flying.orm.rdb.reactive;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * 锁住响应式执行层的包边界，业务只依赖 ReactiveSqlExecutor 和 R2dbcSqlExecutor。
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
class ReactiveImplementationVisibilityTest {

    /** 包装器和批量 writer 都是内部拼装细节，不能进入 V1 公共 API。 */
    @Test
    void keepsExecutorImplementationHelpersPackagePrivate() {
        assertPackagePrivate(DefaultOptionsReactiveSqlExecutor.class);
        assertPackagePrivate(ObservedReactiveSqlExecutor.class);
        assertPackagePrivate(R2dbcBatchWriter.class);
        assertPackagePrivate(BatchReceiptStore.class);
        assertPackagePrivate(BatchPayloadHasher.class);
    }

    private static void assertPackagePrivate(Class<?> type) {
        assertFalse(Modifier.isPublic(type.getModifiers()), () -> type.getName() + " must not be public");
        assertFalse(Modifier.isProtected(type.getModifiers()), () -> type.getName() + " must not be protected");
        assertFalse(Modifier.isPrivate(type.getModifiers()), () -> type.getName() + " must use package visibility");
    }
}
