package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class R2dbcBatchConnectionHandleTest {

    @Test
    void keepsLargeObjectScopeAbsentUntilFirstLobUse() throws IllegalAccessException {
        R2dbcExecutionSession.Resources handle = resources(connection(new AtomicInteger()));

        assertNull(allocatedLargeObjectScope(handle));

        R2dbcLargeObjectScope created = handle.largeObjects();
        assertSame(created, allocatedLargeObjectScope(handle));
        assertSame(created, handle.largeObjects());
    }

    @Test
    void ordinaryAndBatchWorkUseTheUpperReleasePort() {
        AtomicInteger closes = new AtomicInteger();
        R2dbcExecutionSession.Resources resource = resources(connection(closes));
        session().release(resource, SqlExecutionOperation.BATCH_WRITE,
                reactor.core.publisher.SignalType.ON_COMPLETE, null).block();
        assertEquals(1, closes.get());
    }

    @Test
    void ordinaryCancellationDiscardsALobRegisteredAfterCleanupStarts() {
        checkLateLob(SqlExecutionOperation.QUERY);
    }

    @Test
    void batchCancellationDiscardsALobRegisteredAfterCleanupStarts() {
        checkLateLob(SqlExecutionOperation.BATCH_WRITE);
    }

    private static void checkLateLob(SqlExecutionOperation operation) {
        AtomicInteger closes = new AtomicInteger();
        AtomicInteger discards = new AtomicInteger();
        R2dbcExecutionSession.Resources resource = resources(connection(closes));
        session().release(resource, operation, reactor.core.publisher.SignalType.CANCEL, null).block();
        assertLateLobIsDiscarded(resource.largeObjectsIfCreated(), discards);
        assertEquals(1, closes.get());
    }

    private static R2dbcExecutionSession.Resources resources(Connection connection) {
        return new R2dbcExecutionSession.Resources(connection,
                new com.flying.orm.core.sql.render.SqlRequest("select 1", java.util.List.of()));
    }

    private static R2dbcExecutionSession session() {
        return new R2dbcExecutionSession(
                com.flying.orm.rdb.execution.ConnectionAccessTestSupport.reactive(unusedConnectionFactory()),
                R2dbcBindMarkers.from(com.flying.orm.rdb.dialect.RdbDialect.h2()),
                SqlExecutionObserver.noop(), null);
    }

    private static void assertLateLobIsDiscarded(R2dbcLargeObjectScope scope, AtomicInteger discards) {
        assertNotNull(scope, "cancellation must close a scope before a mapper can register a late LOB");
        Blob blob = new Blob() {
            @Override
            public Publisher<ByteBuffer> stream() {
                return Mono.never();
            }

            @Override
            public Publisher<Void> discard() {
                return Mono.fromRunnable(discards::incrementAndGet);
            }
        };
        DynamicRow row = DynamicRow.copyOf(Map.of("content", blob));

        assertThrows(IllegalStateException.class,
                () -> scope.materialize(row, SqlExecutionOptions.safeDefaults()).block());
        assertEquals(1, discards.get());
    }

    private static ConnectionFactory unusedConnectionFactory() {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return Mono.error(new AssertionError("connection is supplied directly by the test"));
            }

            @Override
            public io.r2dbc.spi.ConnectionFactoryMetadata getMetadata() {
                return () -> "H2";
            }
        };
    }

    private static R2dbcLargeObjectScope allocatedLargeObjectScope(Object owner) throws IllegalAccessException {
        Class<?> type = owner.getClass();
        while (type != null) {
            for (Field field : type.getDeclaredFields()) {
                if (field.getType() == R2dbcLargeObjectScope.class) {
                    field.setAccessible(true);
                    return (R2dbcLargeObjectScope) field.get(owner);
                }
            }
            type = type.getSuperclass();
        }
        return null;
    }

    private static Connection connection(AtomicInteger closes) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "close" -> Mono.fromRunnable(closes::incrementAndGet);
                    case "toString" -> "batch-connection-handle-test";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
