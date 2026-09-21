package com.flying.orm.rdb.architecture;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MappingAndOperatorSurfaceConvergenceTest {

    @Test
    void existingOwnersDoNotKeepSecondPackageLocalImplementations() {
        assertAll(
                () -> assertClassAbsent("com.flying.orm.rdb.internal.mapping.EntityMetadataCompiler"),
                () -> assertClassAbsent("com.flying.orm.rdb.operator.EntityWhereBuilder"));
    }

    private static void assertClassAbsent(String className) {
        assertThrows(ClassNotFoundException.class, () -> Class.forName(className));
    }
}
