package com.flying.orm.rdb.metadata;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;

import static org.junit.jupiter.api.Assertions.assertFalse;

/**
 * 数据库专用 reader 由统一工厂管理，不能让业务代码绑死具体实现。
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
class MetadataImplementationVisibilityTest {

    /** 五个内置方言实现保持包级，公开扩展点只有 ReactiveFormMetadataReader。 */
    @Test
    void keepsDialectReadersPackagePrivate() {
        assertPackagePrivate(H2ReactiveFormMetadataReader.class);
        assertPackagePrivate(MySqlReactiveFormMetadataReader.class);
        assertPackagePrivate(PostgreSqlReactiveFormMetadataReader.class);
        assertPackagePrivate(OracleReactiveFormMetadataReader.class);
        assertPackagePrivate(SqlServerReactiveFormMetadataReader.class);
    }

    private static void assertPackagePrivate(Class<?> type) {
        assertFalse(Modifier.isPublic(type.getModifiers()), () -> type.getName() + " must not be public");
        assertFalse(Modifier.isProtected(type.getModifiers()), () -> type.getName() + " must not be protected");
        assertFalse(Modifier.isPrivate(type.getModifiers()), () -> type.getName() + " must use package visibility");
    }
}
