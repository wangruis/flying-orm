package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.codec.SqlTypedValue;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionPhase;
import com.flying.orm.rdb.execution.SqlExecutionSequence;
import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class R2dbcSequenceCleanupOwnershipTest {

    private static final String WORK_FAILURE = "update sample set value = 1 /* work-failure */";

    private static final String WORK_PENDING = "update sample set value = 1 /* work-pending */";

    private static final String CLEANUP = "update sample set value = ? /* cleanup */";

    private static final String CLEANUP_PENDING = "update sample set value = 2 /* cleanup-pending */";

    private static final String CLEANUP_TAIL = "update sample set value = 3 /* cleanup-tail */";

    private static final String CLEANUP_FAILURE = "update sample set value = 4 /* cleanup-failure */";

    private static final String ORDINARY_BEFORE = "update sample set value = ? /* ordinary-before */";

    private static final String MUTABLE_WRAPPER = "update sample set value = ? /* mutable-wrapper */";

    private static final String ORDINARY_AFTER = "update sample set value = ? /* ordinary-after */";

    @Test
    void cleanupWrapperKeepsItsSnapshotWhenWorkFails() {
        byte[] cleanupPayload = {1, 2};
        Fixture fixture = new Fixture();
        Mono<?> execution = fixture.executor.executeInConnection(new SqlExecutionSequence(
                List.of(), List.of(request(WORK_FAILURE)),
                List.of(request(CLEANUP, new SqlTypedValue(SqlTypedValue.Kind.BLOB, cleanupPayload)))),
                SqlExecutionOptions.safeDefaults());
        cleanupPayload[0] = 9;

        SqlExecutionSequenceException failure = assertThrows(
                SqlExecutionSequenceException.class, () -> execution.block(Duration.ofSeconds(2)));

        assertAll(
                () -> assertEquals(SqlExecutionPhase.WORK, failure.phase()),
                () -> assertArrayEquals(new byte[]{1, 2}, fixture.cleanupBytes()),
                fixture::assertCleanupOnceAndOwnedConnectionClosed);
    }

    @Test
    void cleanupWrapperKeepsItsSnapshotWhenStartedWorkIsCancelled() throws Exception {
        byte[] cleanupPayload = {3, 4};
        Fixture fixture = new Fixture();
        Mono<?> execution = fixture.executor.executeInConnection(new SqlExecutionSequence(
                List.of(), List.of(request(WORK_PENDING)),
                List.of(request(CLEANUP, new SqlTypedValue(SqlTypedValue.Kind.BLOB, cleanupPayload)))),
                SqlExecutionOptions.safeDefaults());
        cleanupPayload[0] = 9;

        Disposable subscription = execution.subscribe();
        try {
            assertTrue(fixture.workStarted.await(2, TimeUnit.SECONDS), "work must be running before cancellation");
            subscription.dispose();
            assertTrue(fixture.connectionClosed.await(2, TimeUnit.SECONDS),
                       "cancellation must finish cleanup before the owned connection closes");
        } finally {
            subscription.dispose();
        }

        assertAll(
                () -> assertArrayEquals(new byte[]{3, 4}, fixture.cleanupBytes()),
                fixture::assertCleanupOnceAndOwnedConnectionClosed);
    }

    @Test
    void cancellationDuringCleanupDoesNotRepeatCompletedCleanupStatements() throws Exception {
        Fixture fixture = new Fixture();
        Disposable subscription = fixture.executor.executeInConnection(new SqlExecutionSequence(
                List.of(), List.of(request(ORDINARY_BEFORE, 1)),
                List.of(request(CLEANUP, 1), request(CLEANUP_PENDING), request(CLEANUP_TAIL))),
                SqlExecutionOptions.safeDefaults()).subscribe();
        try {
            assertTrue(fixture.cleanupStarted.await(2, TimeUnit.SECONDS));
            assertEquals(1, fixture.cleanupExecutions.get());
            subscription.dispose();
            fixture.pendingCleanup.tryEmitValue(1L);
            assertTrue(fixture.connectionClosed.await(2, TimeUnit.SECONDS));
            fixture.assertCleanupOnceAndOwnedConnectionClosed();
            assertEquals(1, fixture.cleanupTailExecutions.get());
        } finally {
            subscription.dispose();
            fixture.pendingCleanup.tryEmitValue(1L);
        }
    }

    @Test
    void cleanupFailurePreservesItsPhaseAndCompletedWork() {
        Fixture fixture = new Fixture();
        SqlExecutionSequenceException failure = assertThrows(SqlExecutionSequenceException.class,
                () -> fixture.executor.executeInConnection(new SqlExecutionSequence(
                        List.of(), List.of(request(ORDINARY_BEFORE, 1)),
                        List.of(request(CLEANUP, 1), request(CLEANUP_FAILURE))),
                        SqlExecutionOptions.safeDefaults()).block(Duration.ofSeconds(2)));

        assertEquals(SqlExecutionPhase.CLEANUP, failure.phase());
        assertEquals(1, failure.stepIndex());
        assertEquals(1, failure.completedWorkSteps().size());
        fixture.assertCleanupOnceAndOwnedConnectionClosed();
    }

    @Test
    void keepsOrdinaryRequestsAlignedAroundMutableWrapper() {
        byte[] before = {5};
        byte[] wrapped = {6};
        byte[] after = {7};
        Fixture fixture = new Fixture();
        SqlRequest beforeRequest = request(ORDINARY_BEFORE, before);
        SqlRequest wrappedRequest = request(MUTABLE_WRAPPER, new SqlTypedValue(SqlTypedValue.Kind.BLOB, wrapped));
        SqlRequest afterRequest = request(ORDINARY_AFTER, after);
        Mono<?> execution = fixture.executor.executeInConnection(new SqlExecutionSequence(
                List.of(), List.of(beforeRequest, wrappedRequest, afterRequest), List.of()),
                SqlExecutionOptions.safeDefaults());
        wrapped[0] = 9;

        execution.block(Duration.ofSeconds(2));

        assertAll(
                () -> assertEquals(List.of(ORDINARY_BEFORE, MUTABLE_WRAPPER, ORDINARY_AFTER),
                                   fixture.sqls()),
                () -> assertSame(beforeRequest.parameters().getFirst(), fixture.bound(ORDINARY_BEFORE).getFirst()),
                () -> assertArrayEquals(new byte[]{6}, fixture.bytes(MUTABLE_WRAPPER)),
                () -> assertSame(afterRequest.parameters().getFirst(), fixture.bound(ORDINARY_AFTER).getFirst()));
    }

    private static SqlRequest request(String sql, Object... parameters) {
        return new SqlRequest(sql, List.of(parameters));
    }

    private static final class Fixture {

        private final AtomicInteger cleanupExecutions = new AtomicInteger();

        private final AtomicInteger cleanupTailExecutions = new AtomicInteger();

        private final AtomicInteger connectionCloses = new AtomicInteger();

        private final CountDownLatch workStarted = new CountDownLatch(1);

        private final CountDownLatch connectionClosed = new CountDownLatch(1);

        private final CountDownLatch cleanupStarted = new CountDownLatch(1);

        private final Sinks.One<Long> pendingCleanup = Sinks.one();

        private final List<RecordedStatement> statements = new ArrayList<>();

        private final R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(factory());

        private ConnectionFactory factory() {
            Connection connection = proxy(Connection.class, (proxy, method, arguments) -> switch (method.getName()) {
                case "createStatement" -> statement((String) arguments[0]);
                case "close" -> Mono.fromRunnable(() -> {
                    connectionCloses.incrementAndGet();
                    connectionClosed.countDown();
                });
                default -> throw new UnsupportedOperationException(method.getName());
            });
            return new ConnectionFactory() {
                @Override
                public Publisher<? extends Connection> create() {
                    return Mono.just(connection);
                }

                @Override
                public ConnectionFactoryMetadata getMetadata() {
                    return () -> "H2";
                }
            };
        }

        private Statement statement(String sql) {
            RecordedStatement recorded = new RecordedStatement(sql, new ArrayList<>());
            statements.add(recorded);
            return proxy(Statement.class, (proxy, method, arguments) -> switch (method.getName()) {
                case "bind" -> {
                    recorded.values.add(arguments[1]);
                    yield proxy;
                }
                case "execute" -> execute(recorded.sql);
                default -> throw new UnsupportedOperationException(method.getName());
            });
        }

        private Publisher<? extends Result> execute(String sql) {
            if (WORK_FAILURE.equals(sql)) {
                return Flux.error(new IllegalStateException("work failure"));
            }
            if (WORK_PENDING.equals(sql)) {
                workStarted.countDown();
                return Flux.just(result(Mono.never()));
            }
            if (CLEANUP.equals(sql)) {
                cleanupExecutions.incrementAndGet();
            }
            if (CLEANUP_PENDING.equals(sql)) {
                cleanupStarted.countDown();
                return Flux.just(result(pendingCleanup.asMono()));
            }
            if (CLEANUP_TAIL.equals(sql)) {
                cleanupTailExecutions.incrementAndGet();
            }
            if (CLEANUP_FAILURE.equals(sql)) {
                return Flux.error(new IllegalStateException("cleanup failed"));
            }
            return Flux.just(result(Mono.just(1L)));
        }

        private List<String> sqls() {
            return statements.stream().map(RecordedStatement::sql).toList();
        }

        private List<Object> bound(String sql) {
            return statements.stream()
                             .filter(statement -> statement.sql.equals(sql))
                             .findFirst()
                             .orElseThrow()
                             .values;
        }

        private byte[] cleanupBytes() {
            return bytes(CLEANUP);
        }

        private byte[] bytes(String sql) {
            ByteBuffer content = Flux.from(((Blob) bound(sql).getFirst()).stream()).single()
                                     .block(Duration.ofSeconds(2));
            ByteBuffer view = content.duplicate();
            byte[] bytes = new byte[view.remaining()];
            view.get(bytes);
            return bytes;
        }

        private void assertCleanupOnceAndOwnedConnectionClosed() {
            assertEquals(1, cleanupExecutions.get(), "cleanup must execute exactly once");
            assertEquals(1, connectionCloses.get(), "the owned connection must close exactly once");
        }
    }

    private static Result result(Publisher<Long> rowsUpdated) {
        return proxy(Result.class, (proxy, method, arguments) -> {
            if ("getRowsUpdated".equals(method.getName())) {
                return rowsUpdated;
            }
            throw new UnsupportedOperationException(method.getName());
        });
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }

    private record RecordedStatement(String sql, List<Object> values) {
    }
}
