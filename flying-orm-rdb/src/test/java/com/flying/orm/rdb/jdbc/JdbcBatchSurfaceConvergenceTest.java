package com.flying.orm.rdb.jdbc;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 批量执行的专属算法和生命周期只保留在其实际 JDBC owner 中。 */
class JdbcBatchSurfaceConvergenceTest {

    @Test
    void singleOwnerBatchHelpersAreAbsent() {
        assertAll(
                () -> assertTypeAbsent("com.flying.orm.rdb.jdbc.JdbcBatchAtomicSupport"),
                () -> assertTypeAbsent("com.flying.orm.rdb.jdbc.JdbcBatchGeneratedKeyReader"),
                () -> assertTypeAbsent("com.flying.orm.rdb.jdbc.JdbcExternalBatchCompletion"));
    }

    private static void assertTypeAbsent(String typeName) {
        assertThrows(ClassNotFoundException.class,
                     () -> Class.forName(typeName, false,
                                         JdbcBatchSurfaceConvergenceTest.class.getClassLoader()));
    }
}
