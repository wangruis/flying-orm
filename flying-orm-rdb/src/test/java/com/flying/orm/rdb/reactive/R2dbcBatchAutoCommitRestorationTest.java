package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipant;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.lang.reflect.Field;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** ORM 自管批量事务结束后恢复由 beginTransaction 改变的自动提交状态。 */
class R2dbcBatchAutoCommitRestorationTest {

    @Test
    void restoresAutoCommitBeforeClosingCommittedOwnedBatch() {
        verifyRestored(true);
    }

    @Test
    void restoresAutoCommitBeforeClosingRolledBackOwnedBatch() {
        verifyRestored(false);
    }

    @Test
    void restoresAutoCommitBeforeClosingAfterLargeObjectCleanupFailure() {
        AtomicBoolean autoCommit = new AtomicBoolean(true);
        List<String> calls = new ArrayList<>();
        Connection connection = connection(autoCommit, calls);
        R2dbcBatchConnectionHandle handle = new R2dbcBatchConnectionHandle(connection);
        R2dbcBatchConnectionLifecycle lifecycle = new R2dbcBatchConnectionLifecycle(
                connectionFactory(connection),
                SqlExecutionObserver.noop(),
                R2dbcTransactionParticipant.none());

        lifecycle.begin(handle).then(lifecycle.commit(handle)).block(Duration.ofSeconds(2));
        handle.largeObjects().discardCaptured(
                List.of(failingBlob()),
                SqlExecutionOptions.safeDefaults(),
                new IllegalStateException("result mapping failed"))
                .block(Duration.ofSeconds(2));
        lifecycle.closeAfterOutcome(handle).block(Duration.ofSeconds(2));

        assertTrue(autoCommit.get());
        assertEquals(List.of("begin", "commit", "autoCommit=true", "close"), calls);
    }

    @Test
    void keepsAnInitiallyDisabledAutoCommitState() {
        AtomicBoolean autoCommit = new AtomicBoolean(false);
        List<String> calls = new ArrayList<>();
        Connection connection = connection(autoCommit, calls);
        R2dbcBatchConnectionHandle handle = new R2dbcBatchConnectionHandle(connection);
        R2dbcBatchConnectionLifecycle lifecycle = new R2dbcBatchConnectionLifecycle(
                connectionFactory(connection),
                SqlExecutionObserver.noop(),
                R2dbcTransactionParticipant.none());

        lifecycle.begin(handle)
                 .then(lifecycle.commit(handle))
                 .then(lifecycle.closeAfterOutcome(handle))
                 .block(Duration.ofSeconds(2));

        assertFalse(autoCommit.get());
        assertEquals(List.of("begin", "commit", "close"), calls);
    }

    @Test
    void closesAndReportsAConfirmedCleanupFailureWhenAutoCommitRestoreFails() {
        AtomicBoolean autoCommit = new AtomicBoolean(true);
        List<String> calls = new ArrayList<>();
        List<ResourceCleanupObservation> observations = new ArrayList<>();
        Connection connection = connection(autoCommit, calls, true);
        R2dbcBatchConnectionHandle handle = new R2dbcBatchConnectionHandle(connection);
        SqlExecutionObserver observer = new SqlExecutionObserver() {
            @Override
            public void onExecution(SqlExecutionObservation observation) {
                // 本测试只记录资源清理事实。
            }

            @Override
            public void onResourceCleanup(ResourceCleanupObservation observation) {
                observations.add(observation);
            }
        };
        R2dbcBatchConnectionLifecycle lifecycle = new R2dbcBatchConnectionLifecycle(
                connectionFactory(connection),
                observer,
                R2dbcTransactionParticipant.none());

        lifecycle.begin(handle)
                 .then(lifecycle.commit(handle))
                 .then(lifecycle.closeAfterOutcome(handle))
                 .block(Duration.ofSeconds(2));

        assertFalse(autoCommit.get());
        assertEquals(List.of("begin", "commit", "autoCommit=true", "close"), calls);
        assertEquals(1, observations.size());
        assertTrue(observations.getFirst().outcomeConfirmed());
        assertEquals(ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                     observations.getFirst().phase());
    }

    @Test
    void waitsForNormalPoolReturnAfterTheTransactionOutcomeIsKnown() throws InterruptedException {
        Sinks.Empty<Void> closeCompletion = Sinks.empty();
        CountDownLatch terminated = new CountDownLatch(1);
        Connection connection = connectionWithDelayedClose(closeCompletion);
        R2dbcBatchConnectionHandle handle = new R2dbcBatchConnectionHandle(
                connection, Duration.ofMillis(10));
        handle.markCommitted();
        R2dbcBatchConnectionLifecycle lifecycle = new R2dbcBatchConnectionLifecycle(
                connectionFactory(connection),
                SqlExecutionObserver.noop(),
                R2dbcTransactionParticipant.none());

        lifecycle.closeAfterOutcome(handle).doFinally(ignored -> terminated.countDown()).subscribe();

        assertFalse(terminated.await(100, TimeUnit.MILLISECONDS));
        closeCompletion.tryEmitEmpty();
        assertTrue(terminated.await(1, TimeUnit.SECONDS));
        assertNull(cleanupDeadline(handle),
                   "confirmed scalar close must not enter the cleanup-deadline path");
    }

    private static R2dbcCleanupDeadline cleanupDeadline(R2dbcBatchConnectionHandle handle) {
        try {
            Field field = R2dbcBatchConnectionHandle.class.getDeclaredField("cleanupDeadline");
            field.setAccessible(true);
            return (R2dbcCleanupDeadline) field.get(handle);
        } catch (ReflectiveOperationException error) {
            throw new AssertionError(error);
        }
    }

    private static Blob failingBlob() {
        return new Blob() {
            @Override
            public Publisher<ByteBuffer> stream() {
                return Flux.empty();
            }

            @Override
            public Publisher<Void> discard() {
                return Mono.error(new IllegalStateException("LOB discard failed"));
            }
        };
    }

    private static void verifyRestored(boolean commit) {
        AtomicBoolean autoCommit = new AtomicBoolean(true);
        List<String> calls = new ArrayList<>();
        Connection connection = connection(autoCommit, calls);
        R2dbcBatchConnectionHandle handle = new R2dbcBatchConnectionHandle(connection);
        R2dbcBatchConnectionLifecycle lifecycle = new R2dbcBatchConnectionLifecycle(
                connectionFactory(connection),
                SqlExecutionObserver.noop(),
                R2dbcTransactionParticipant.none());

        lifecycle.begin(handle)
                 .then(commit ? lifecycle.commit(handle) : lifecycle.rollback(handle))
                 .then(lifecycle.closeAfterOutcome(handle))
                 .block(Duration.ofSeconds(2));

        assertTrue(autoCommit.get());
        assertEquals(commit
                             ? List.of("begin", "commit", "autoCommit=true", "close")
                             : List.of("begin", "rollback", "autoCommit=true", "close"),
                     calls);
    }

    private static ConnectionFactory connectionFactory(Connection connection) {
        return new ConnectionFactory() {
            @Override
            public Mono<? extends Connection> create() {
                return Mono.just(connection);
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "test";
            }
        };
    }

    private static Connection connection(AtomicBoolean autoCommit, List<String> calls) {
        return connection(autoCommit, calls, false);
    }

    private static Connection connection(AtomicBoolean autoCommit,
                                         List<String> calls,
                                         boolean failAutoCommitRestore) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "isAutoCommit" -> autoCommit.get();
                    case "beginTransaction" -> {
                        yield Mono.fromRunnable(() -> {
                            calls.add("begin");
                            autoCommit.set(false);
                        });
                    }
                    case "commitTransaction" -> {
                        yield Mono.fromRunnable(() -> calls.add("commit"));
                    }
                    case "rollbackTransaction" -> {
                        yield Mono.fromRunnable(() -> calls.add("rollback"));
                    }
                    case "setAutoCommit" -> {
                        boolean configured = (boolean) args[0];
                        yield Mono.defer(() -> {
                            calls.add("autoCommit=" + configured);
                            if (failAutoCommitRestore) {
                                return Mono.error(new IllegalStateException("auto-commit restore failed"));
                            }
                            autoCommit.set(configured);
                            return Mono.empty();
                        });
                    }
                    case "close" -> {
                        yield Mono.fromRunnable(() -> calls.add("close"));
                    }
                    case "toString" -> "test-connection";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Connection connectionWithDelayedClose(Sinks.Empty<Void> closeCompletion) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "isAutoCommit" -> true;
                    case "close" -> closeCompletion.asMono();
                    case "toString" -> "delayed-close-connection";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }
}
