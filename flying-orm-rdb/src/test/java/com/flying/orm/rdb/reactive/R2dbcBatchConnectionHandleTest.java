package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipant;
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
        R2dbcBatchConnectionHandle handle = new R2dbcBatchConnectionHandle(connection(new AtomicInteger()));

        assertNull(allocatedLargeObjectScope(handle));

        R2dbcLargeObjectScope created = handle.largeObjects();
        assertSame(created, allocatedLargeObjectScope(handle));
        assertSame(created, handle.largeObjects());
    }

    @Test
    void ordinaryConnectionCleanupAcceptsAConfirmedBatchLease() throws Exception {
        AtomicInteger closes = new AtomicInteger();
        R2dbcBatchConnectionHandle handle = new R2dbcBatchConnectionHandle(connection(closes));
        R2dbcConnectionLeaseCleanup cleanup = new R2dbcConnectionLeaseCleanup(SqlExecutionObserver.noop());
        Method sharedCleanup = Arrays.stream(R2dbcConnectionLeaseCleanup.class.getDeclaredMethods())
                .filter(method -> method.getName().equals("closeAfterResult"))
                .filter(method -> method.getParameterCount() == 4)
                .filter(method -> method.getParameterTypes()[0].isInstance(handle))
                .filter(method -> method.getParameterTypes()[2] == SqlExecutionOptions.class)
                .findFirst()
                .orElse(null);

        assertNotNull(sharedCleanup, "ordinary and batch execution must share the same connection cleanup contract");
        sharedCleanup.setAccessible(true);
        Mono<?> release = (Mono<?>) sharedCleanup.invoke(
                cleanup,
                handle,
                SqlExecutionOperation.CHUNKED_BATCH_WRITE,
                SqlExecutionOptions.safeDefaults(),
                true);
        release.block();

        assertEquals(1, closes.get());
    }

    @Test
    void ordinaryCancellationDiscardsALobRegisteredAfterCleanupStarts() {
        AtomicInteger closes = new AtomicInteger();
        AtomicInteger discards = new AtomicInteger();
        R2dbcExecutionSession.ConnectionLease lease = new R2dbcExecutionSession.ConnectionLease(
                connection(closes), false);
        R2dbcConnectionLeaseCleanup cleanup = new R2dbcConnectionLeaseCleanup(SqlExecutionObserver.noop());

        cleanup.cancelAfterResult(
                lease, SqlExecutionOperation.QUERY, SqlExecutionOptions.safeDefaults()).block();

        assertLateLobIsDiscarded(lease.largeObjectsIfCreated(), discards);
        assertEquals(1, closes.get());
    }

    @Test
    void batchCancellationDiscardsALobRegisteredAfterCleanupStarts() {
        AtomicInteger closes = new AtomicInteger();
        AtomicInteger discards = new AtomicInteger();
        R2dbcBatchConnectionHandle handle = new R2dbcBatchConnectionHandle(connection(closes));
        R2dbcBatchConnectionLifecycle lifecycle = new R2dbcBatchConnectionLifecycle(
                unusedConnectionFactory(), SqlExecutionObserver.noop(), R2dbcTransactionParticipant.none());

        lifecycle.cancel(handle, "ATOMIC").block();

        assertLateLobIsDiscarded(handle.largeObjectsIfCreated(), discards);
        assertEquals(1, closes.get());
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
