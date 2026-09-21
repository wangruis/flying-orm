package com.flying.orm.rdb.reactive;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReactiveOwnerSurfaceConvergenceTest {

    @Test
    void ownerOnlyLeafTypesAreNotTopLevelProductionClasses() {
        assertAll(
                () -> assertClassAbsent(
                        "com.flying.orm.rdb.reactive.ScopedForwardingReactiveSqlExecutor"),
                () -> assertClassAbsent(
                        "com.flying.orm.rdb.reactive.ReactiveSqlObservation"));
    }

    private static void assertClassAbsent(String className) {
        assertThrows(ClassNotFoundException.class, () -> Class.forName(className));
    }
}
