package com.flying.orm.rdb.aggregate;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 防止聚合规划器已经收回的单调用方算法再次扩散为独立生产类型。 */
class AggregateSurfaceConvergenceTest {

    @Test
    void statelessSingleCallerPlanningTypesAreAbsent() {
        assertAll(
                () -> assertTypeAbsent("com.flying.orm.rdb.aggregate.AggregateFunctionSqlRenderer"),
                () -> assertTypeAbsent("com.flying.orm.rdb.aggregate.AggregateResultVisibilityGuard"));
    }

    private static void assertTypeAbsent(String typeName) {
        assertThrows(ClassNotFoundException.class,
                     () -> Class.forName(typeName, false,
                                         AggregateSurfaceConvergenceTest.class.getClassLoader()));
    }
}
