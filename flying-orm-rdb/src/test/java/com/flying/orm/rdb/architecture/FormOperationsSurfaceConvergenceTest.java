package com.flying.orm.rdb.architecture;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FormOperationsSurfaceConvergenceTest {

    @Test
    void syncAndReactiveOperationsOwnTheirExecutionPathsDirectly() {
        assertAll(
                () -> assertClassAbsent("com.flying.orm.rdb.form.SyncFormAggregateOperations"),
                () -> assertClassAbsent("com.flying.orm.rdb.form.ReactiveFormAggregateOperations"),
                () -> assertClassAbsent("com.flying.orm.rdb.form.SyncFormWriteOperations"),
                () -> assertClassAbsent("com.flying.orm.rdb.form.SyncJoinOperations"),
                () -> assertClassAbsent("com.flying.orm.rdb.form.ReactiveJoinQueryOperations"));
    }

    private static void assertClassAbsent(String className) {
        assertThrows(ClassNotFoundException.class, () -> Class.forName(className));
    }
}
