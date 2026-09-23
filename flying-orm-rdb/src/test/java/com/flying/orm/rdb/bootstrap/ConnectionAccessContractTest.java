package com.flying.orm.rdb.bootstrap;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.jdbc.JdbcConnectionAccess;
import com.flying.orm.rdb.reactive.R2dbcConnectionAccess;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Mono;

import java.lang.reflect.Modifier;
import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Test
    void jdbcFactoryDelegatesWithoutOwningTheConnection() throws Exception {
        var connection = connection(java.sql.Connection.class);
        var request = new SqlRequest("select 1", List.of());
        var calls = new AtomicInteger();
        var access = JdbcConnectionAccess.of(sql -> {
            assertSame(request, sql);
            calls.incrementAndGet();
            return connection;
        }, (value, sql) -> {
            assertSame(request, sql);
            assertSame(connection, value);
            calls.incrementAndGet();
        });
        assertEquals(0, calls.get());
        assertSame(connection, access.getConnection(request));
        access.releaseConnection(connection, request);
        assertEquals(2, calls.get());
    }

    @Test
    void jdbcFactoryPreservesFailuresAndValidatesCallbacks() {
        var failure = new SQLException("upper-layer failure");
        var access = JdbcConnectionAccess.of(sql -> { throw failure; }, (value, sql) -> { throw failure; });
        assertSame(failure, assertThrows(SQLException.class, () -> access.getConnection(null)));
        assertSame(failure, assertThrows(SQLException.class, () -> access.releaseConnection(null, null)));
        assertThrows(NullPointerException.class, () -> JdbcConnectionAccess.of(null, (value, sql) -> {}));
        assertThrows(NullPointerException.class, () -> JdbcConnectionAccess.of(sql -> null, null));
    }

    @Test
    void reactiveFactoryDoesNotSubscribeAndPreservesReleaseSignals() {
        var connection = connection(io.r2dbc.spi.Connection.class);
        var request = new SqlRequest("select 1", List.of());
        var subscriptions = new AtomicInteger();
        for (var signal : List.of(SignalType.ON_COMPLETE, SignalType.ON_ERROR, SignalType.CANCEL)) {
            subscriptions.set(0);
            var access = R2dbcConnectionAccess.of(sql -> {
                assertSame(request, sql);
                return Mono.fromSupplier(() -> { subscriptions.incrementAndGet(); return connection; });
            }, (terminal, value, sql) -> {
                assertSame(signal, terminal);
                assertSame(connection, value);
                assertSame(request, sql);
                return Mono.fromRunnable(subscriptions::incrementAndGet);
            });
            var acquired = access.getConnection(request);
            var released = access.releaseConnection(signal, connection, request);
            assertEquals(0, subscriptions.get());
            assertSame(connection, Mono.from(acquired).block(Duration.ofSeconds(5)));
            Mono.from(released).block(Duration.ofSeconds(5));
            assertEquals(2, subscriptions.get());
        }
    }

    @Test
    void reactiveFactoryPreservesPublisherFailuresAndValidatesCallbacks() {
        var failure = new IllegalStateException("upper-layer failure");
        var access = R2dbcConnectionAccess.of(sql -> Mono.error(failure),
                (signal, value, sql) -> Mono.error(failure));
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> Mono.from(access.getConnection(null)).block(Duration.ofSeconds(5))));
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> Mono.from(access.releaseConnection(SignalType.ON_ERROR, null, null)).block(Duration.ofSeconds(5))));
        assertThrows(NullPointerException.class, () -> R2dbcConnectionAccess.of(null,
                (signal, value, sql) -> Mono.empty()));
        assertThrows(NullPointerException.class, () -> R2dbcConnectionAccess.of(sql -> Mono.empty(), null));
    }

    private static <T> T connection(Class<T> type) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type},
                (proxy, method, args) -> { throw new AssertionError("factory must not operate on connection"); }));
    }

    private static Class<?> requirePort(String name) {
        Class<?> access = assertDoesNotThrow(() -> Class.forName(name),
                "upper-layer connection access port must exist");
        assertTrue(access.isInterface());
        assertEquals(0, access.getDeclaredFields().length);
        var callbacks = access.getDeclaredClasses();
        assertEquals(name.contains("Jdbc") ? Set.of("Acquirer", "Releaser") : Set.of("Releaser"),
                Arrays.stream(callbacks).map(Class::getSimpleName).collect(Collectors.toSet()));
        for (var callback : callbacks) {
            assertTrue(callback.isInterface());
            assertTrue(callback.isAnnotationPresent(FunctionalInterface.class));
            assertEquals(0, callback.getDeclaredFields().length);
            assertEquals(1, callback.getDeclaredMethods().length);
        }
        // Coverage instrumentation adds a synthetic bootstrap method; it is not part of the API.
        var methods = Arrays.stream(access.getDeclaredMethods()).filter(method -> !method.isSynthetic()).toList();
        assertEquals(3, methods.size());
        assertEquals(Set.of("getConnection", "releaseConnection", "of"),
                methods.stream().map(method -> method.getName())
                        .collect(Collectors.toSet()));
        assertTrue(methods.stream()
                .allMatch(method -> Modifier.isPublic(method.getModifiers())
                        && (method.getName().equals("of")
                            ? Modifier.isStatic(method.getModifiers()) && method.getReturnType() == access
                            : Modifier.isAbstract(method.getModifiers()))));
        return access;
    }
}
