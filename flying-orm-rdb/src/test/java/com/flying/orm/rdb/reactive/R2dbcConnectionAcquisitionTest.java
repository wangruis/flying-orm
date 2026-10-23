package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import io.r2dbc.spi.Connection;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.lang.management.ManagementFactory;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class R2dbcConnectionAcquisitionTest {
    private static volatile R2dbcExecutionSession.Resources sink;

    @Test
    void successfulAcquisitionDoesNotAllocateAnUnusedExceptionStack() {
        java.lang.management.ThreadMXBean management = ManagementFactory.getThreadMXBean();
        assumeTrue(management instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean allocation = (com.sun.management.ThreadMXBean) management;
        assumeTrue(allocation.isThreadAllocatedMemorySupported());
        allocation.setThreadAllocatedMemoryEnabled(true);
        Connection connection = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                    throw new AssertionError("Acquisition must not invoke a connection method");
                });
        R2dbcExecutionSession session = session(R2dbcConnectionAccess.of(
                request -> Mono.just(connection), (signal, value, request) -> Mono.empty()));
        Mono<R2dbcExecutionSession.Resources> acquire = session.acquireConnection(request());
        for (int index = 0; index < 20_000; index++) sink = acquire.block();
        long thread = Thread.currentThread().threadId();
        long before = allocation.getThreadAllocatedBytes(thread);
        for (int index = 0; index < 20_000; index++) sink = acquire.block();
        long bytesPerAcquisition = (allocation.getThreadAllocatedBytes(thread) - before) / 20_000;
        assertSame(connection, sink.connection());
        // Successful acquisition needs small Reactor/lease objects, not a Throwable with its backtrace.
        assertTrue(bytesPerAcquisition < 512,
                () -> "successful acquisition allocated " + bytesPerAcquisition + " bytes per call");
    }

    @Test
    void emptyAcquisitionRemainsDeferredAndFailsSeparatelyForEachSubscription() {
        AtomicInteger acquired = new AtomicInteger();
        AtomicInteger released = new AtomicInteger();
        R2dbcExecutionSession session = session(R2dbcConnectionAccess.of(request -> {
            acquired.incrementAndGet();
            return Mono.empty();
        }, (signal, connection, request) -> {
            released.incrementAndGet();
            return Mono.empty();
        }));
        Mono<R2dbcExecutionSession.Resources> acquire = session.acquireConnection(request());
        assertEquals(0, acquired.get());
        IllegalStateException first = assertThrows(IllegalStateException.class, acquire::block);
        IllegalStateException second = assertThrows(IllegalStateException.class, acquire::block);
        assertEquals("connection publisher completed empty", first.getMessage());
        assertEquals(first.getMessage(), second.getMessage());
        assertNotSame(first, second);
        assertEquals(2, acquired.get());
        assertEquals(0, released.get());
    }

    @Test
    void propagatesTheOriginalAcquisitionFailure() {
        IllegalArgumentException failure = new IllegalArgumentException("external acquisition failed");
        R2dbcExecutionSession session = session(R2dbcConnectionAccess.of(
                request -> Mono.error(failure), (signal, connection, request) -> Mono.empty()));
        assertSame(failure, assertThrows(IllegalArgumentException.class,
                () -> session.acquireConnection(request()).block()));
    }

    private static SqlRequest request() {
        return new SqlRequest("select 1", List.of());
    }

    private static R2dbcExecutionSession session(R2dbcConnectionAccess access) {
        return new R2dbcExecutionSession(access, R2dbcBindMarkers.from(RdbDialect.postgresql()),
                SqlExecutionObserver.noop(), null);
    }
}
