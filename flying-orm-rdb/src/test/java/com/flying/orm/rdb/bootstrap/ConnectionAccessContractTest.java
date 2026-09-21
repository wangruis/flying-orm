package com.flying.orm.rdb.bootstrap;

import com.flying.orm.core.sql.render.SqlRequest;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.SignalType;

import java.lang.reflect.Modifier;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConnectionAccessContractTest {

    @Test
    void jdbcAccessOnlyDelegatesAcquisitionAndRelease() throws Exception {
        Class<?> access = requirePort("com.flying.orm.rdb.jdbc.JdbcConnectionAccess");
        var acquire = access.getDeclaredMethod("getConnection", SqlRequest.class);
        var release = access.getDeclaredMethod("releaseConnection", java.sql.Connection.class, SqlRequest.class);
        assertEquals(java.sql.Connection.class, acquire.getReturnType());
        assertEquals(void.class, release.getReturnType());
        assertEquals(Set.of(SQLException.class), Set.of(acquire.getExceptionTypes()));
        assertEquals(Set.of(SQLException.class), Set.of(release.getExceptionTypes()));
    }

    @Test
    void reactiveAccessComposesAcquisitionAndTerminalRelease() throws Exception {
        Class<?> access = requirePort("com.flying.orm.rdb.reactive.R2dbcConnectionAccess");
        var acquire = access.getDeclaredMethod("getConnection", SqlRequest.class);
        var release = access.getDeclaredMethod("releaseConnection", SignalType.class,
                io.r2dbc.spi.Connection.class, SqlRequest.class);
        assertEquals(Publisher.class, acquire.getReturnType());
        assertEquals(Publisher.class, release.getReturnType());
        assertEquals("org.reactivestreams.Publisher<? extends io.r2dbc.spi.Connection>",
                acquire.getGenericReturnType().getTypeName());
        assertEquals("org.reactivestreams.Publisher<java.lang.Void>",
                release.getGenericReturnType().getTypeName());
    }

    private static Class<?> requirePort(String name) {
        Class<?> access = assertDoesNotThrow(() -> Class.forName(name),
                "upper-layer connection access port must exist");
        assertTrue(access.isInterface());
        assertEquals(0, access.getDeclaredFields().length);
        assertEquals(0, access.getDeclaredClasses().length);
        assertEquals(2, access.getDeclaredMethods().length);
        assertEquals(Set.of("getConnection", "releaseConnection"),
                Arrays.stream(access.getDeclaredMethods()).map(method -> method.getName())
                        .collect(Collectors.toSet()));
        assertTrue(Arrays.stream(access.getDeclaredMethods())
                .allMatch(method -> Modifier.isPublic(method.getModifiers())
                        && Modifier.isAbstract(method.getModifiers())));
        return access;
    }
}
