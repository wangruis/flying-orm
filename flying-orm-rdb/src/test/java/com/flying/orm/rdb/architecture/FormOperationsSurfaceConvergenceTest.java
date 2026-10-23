package com.flying.orm.rdb.architecture;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;

class FormOperationsSurfaceConvergenceTest {

    @Test
    void writeGovernanceHasOnePlanningOwner() throws ClassNotFoundException {
        Class<?> planner = Class.forName("com.flying.orm.rdb.form.FormOperationPlanner");
        java.util.Set<String> duplicated = java.util.Set.of(
                "insertGoverned", "updateGoverned", "deleteGoverned", "physicalDeleteGoverned");
        for (java.lang.reflect.Method method : planner.getDeclaredMethods()) {
            assertFalse(duplicated.contains(method.getName()),
                    "write governance must share the planner entry: " + method.getName());
        }
    }

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
