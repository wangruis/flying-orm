package com.flying.orm.rdb.reactive;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReactiveOwnerSurfaceConvergenceTest {

    @Test
    void sessionDoesNotKeepUnusedRawSqlOrParameterTypeOverloads() {
        assertAll(
                () -> assertThrows(NoSuchMethodException.class, () -> R2dbcExecutionSession.class
                        .getDeclaredMethod("bind", io.r2dbc.spi.Statement.class, java.util.List.class,
                                java.util.List.class)),
                () -> assertThrows(NoSuchMethodException.class, () -> R2dbcExecutionSession.class
                        .getDeclaredMethod("prepareStatement", io.r2dbc.spi.Connection.class, String.class,
                                int.class, com.flying.orm.core.sql.render.SqlBindMarkerStyle.class,
                                java.util.List.class)),
                () -> assertThrows(NoSuchMethodException.class, () -> R2dbcExecutionSession.class
                        .getDeclaredMethod("sqlForDriver", String.class, int.class,
                                com.flying.orm.core.sql.render.SqlBindMarkerStyle.class)));
    }

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
