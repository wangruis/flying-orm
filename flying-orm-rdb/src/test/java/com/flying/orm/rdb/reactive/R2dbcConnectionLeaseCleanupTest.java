package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.dialect.RdbDialect;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class R2dbcConnectionLeaseCleanupTest {

    @Test
    void disabledObserverSkipsCleanupEventsAcrossSuccessAndError() {
        RecordingObserver observer = new RecordingObserver(false);
        R2dbcExecutionSession cleanup = session(observer);
        AtomicInteger closes = new AtomicInteger();
        IllegalStateException releaseFailure = new IllegalStateException("release failed");
        var resource = resources(connection(closes, Mono.error(releaseFailure)));
        assertSame(releaseFailure, assertThrows(IllegalStateException.class, () -> cleanup.release(
                resource, SqlExecutionOperation.QUERY, reactor.core.publisher.SignalType.ON_COMPLETE, null).block()));
        IllegalStateException primary = new IllegalStateException("SQL failed");
        cleanup.release(resources(connection(closes, Mono.error(releaseFailure))),
                SqlExecutionOperation.QUERY, reactor.core.publisher.SignalType.ON_ERROR, primary).block();
        assertSame(releaseFailure, primary.getSuppressed()[0]);
        assertEquals(2, closes.get());
        assertTrue(observer.cleanup.isEmpty());
    }

    @Test
    void disabledObservationDoesNotSuppressFatalCleanupFailure() {
        RecordingObserver observer = new RecordingObserver(false);
        R2dbcExecutionSession cleanup = session(observer);
        VirtualMachineError fatal = new VirtualMachineError("driver fatal") { };
        AtomicInteger closes = new AtomicInteger();
        R2dbcExecutionSession.Resources lease = resources(
                connection(closes, Mono.error(new CompletionException(fatal))));

        Throwable terminal = assertThrows(VirtualMachineError.class, () -> cleanup.release(
                lease, SqlExecutionOperation.QUERY, reactor.core.publisher.SignalType.ON_ERROR,
                new IllegalStateException("LOB cleanup failed")).block());

        assertSame(fatal, terminal);
        assertEquals(1, closes.get());
        assertTrue(observer.cleanup.isEmpty());
    }

    @Test
    void errorCleanupReleasesLobsAndClosesOnceWithoutClassifyingTheSqlError() {
        AtomicInteger discards = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        TrackingCompletionException failure = new TrackingCompletionException(
                new IllegalStateException("database failed"));
        R2dbcExecutionSession cleanup = session(SqlExecutionObserver.noop());
        R2dbcExecutionSession.Resources lease = resources(
                connection(closes, Mono.empty()));
        Blob blob = new Blob() {
            @Override
            public Publisher<ByteBuffer> stream() {
                return Flux.empty();
            }

            @Override
            public Publisher<Void> discard() {
                discards.incrementAndGet();
                return Mono.empty();
            }
        };
        lease.largeObjects().materialize(
                DynamicRow.copyOf(Map.of("payload", blob)), SqlExecutionOptions.safeDefaults());

        cleanup.release(lease, SqlExecutionOperation.QUERY, reactor.core.publisher.SignalType.ON_ERROR, failure).block();

        assertEquals(1, discards.get(), "pending LOB locators must be released before connection return");
        assertEquals(1, closes.get(), "the owned connection must be returned exactly once");
        assertEquals(0, failure.causeReads(), "cleanup must not classify the SQL failure for pool reuse");
    }

    @Test
    void delegatesOrdinaryConnectionReleaseToTheDriverBoundary() throws InterruptedException {
        AtomicInteger closes = new AtomicInteger();
        Connection connection = connection(closes, Mono.never());
        R2dbcExecutionSession cleanup = session(SqlExecutionObserver.noop());
        R2dbcExecutionSession.Resources lease =
                resources(connection);

        CountDownLatch terminated = new CountDownLatch(1);
        Disposable subscription = cleanup.release(lease, SqlExecutionOperation.QUERY, reactor.core.publisher.SignalType.ON_COMPLETE, null)
                .doFinally(ignored -> terminated.countDown())
                .subscribe();
        try {
            assertFalse(terminated.await(1, TimeUnit.SECONDS),
                    "ordinary driver release must not be terminated by an ORM cleanup timer");
            assertEquals(1, closes.get(), "the owned connection must be released exactly once");
        } finally {
            subscription.dispose();
        }
    }

    @Test
    void upperBorrowedConnectionReleaseDoesNotPhysicallyClose() {
        AtomicInteger closes = new AtomicInteger();
        Connection connection = connection(closes, Mono.empty());
        R2dbcExecutionSession session = new R2dbcExecutionSession(
                com.flying.orm.rdb.execution.ConnectionAccessTestSupport.borrowed(connection),
                R2dbcBindMarkers.from(RdbDialect.postgresql()), SqlExecutionObserver.noop(), null);
        var lease = session.acquireConnection(new com.flying.orm.core.sql.render.SqlRequest("select 1", List.of())).block();
        session.release(lease, SqlExecutionOperation.QUERY,
                reactor.core.publisher.SignalType.ON_COMPLETE, null).block();
        assertEquals(0, closes.get());
    }

    @Test
    void observesFailedOrdinaryReleaseWithoutSubscribingCloseAgain() {
        AtomicInteger closeSubscriptions = new AtomicInteger();
        IllegalStateException closeFailure = new IllegalStateException("close failed");
        Mono<Void> failedClose = Mono.defer(() -> {
            closeSubscriptions.incrementAndGet();
            return Mono.error(closeFailure);
        });
        RecordingObserver observer = new RecordingObserver();
        R2dbcExecutionSession cleanup = session(observer);
        R2dbcExecutionSession.Resources lease =
                resources(connection(new AtomicInteger(), failedClose));

        assertThrows(IllegalStateException.class, () -> cleanup.release(lease, SqlExecutionOperation.QUERY,
                reactor.core.publisher.SignalType.ON_COMPLETE, null).block());

        assertEquals(1, closeSubscriptions.get());
        assertEquals(1, observer.cleanup.size());
        ResourceCleanupObservation observation = observer.cleanup.getFirst();
        assertEquals(ResourceCleanupObservation.Phase.CONNECTION_RELEASE, observation.phase());
        assertTrue(observation.executionCompleted());
    }

    @Test
    void observesFailedCancelledQueryReleaseWithoutSubscribingCloseAgain() {
        AtomicInteger closeSubscriptions = new AtomicInteger();
        Mono<Void> failedClose = Mono.defer(() -> {
            closeSubscriptions.incrementAndGet();
            return Mono.error(new IllegalStateException("cancelled query close failed"));
        });
        RecordingObserver observer = new RecordingObserver();
        R2dbcExecutionSession cleanup = session(observer);
        R2dbcExecutionSession.Resources lease =
                resources(connection(new AtomicInteger(), failedClose));

        assertThrows(IllegalStateException.class, () -> cleanup.release(lease, SqlExecutionOperation.QUERY,
                reactor.core.publisher.SignalType.CANCEL, null).block());

        assertEquals(1, closeSubscriptions.get());
        assertEquals(1, observer.cleanup.size());
        assertFalse(observer.cleanup.getFirst().executionCompleted());
    }

    private static R2dbcExecutionSession.Resources resources(Connection connection) {
        return new R2dbcExecutionSession.Resources(connection,
                new com.flying.orm.core.sql.render.SqlRequest("select 1", List.of()));
    }

    private static R2dbcExecutionSession session(SqlExecutionObserver observer) {
        return new R2dbcExecutionSession(new R2dbcConnectionAccess() {
            public Publisher<? extends Connection> getConnection(com.flying.orm.core.sql.render.SqlRequest request) {
                return Mono.error(new AssertionError("test supplies connection directly"));
            }
            public Publisher<Void> releaseConnection(reactor.core.publisher.SignalType signal,
                    Connection connection, com.flying.orm.core.sql.render.SqlRequest request) {
                return connection.close();
            }
        }, R2dbcBindMarkers.from(RdbDialect.h2()), observer, null);
    }

    private static Connection connection(AtomicInteger closes, Mono<Void> closeResult) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> {
                    if ("close".equals(method.getName())) {
                        closes.incrementAndGet();
                        return closeResult;
                    }
                    return null;
                });
    }

    private static final class RecordingObserver implements SqlExecutionObserver {
        private final List<ResourceCleanupObservation> cleanup = new ArrayList<>();
        private final boolean enabled;

        private RecordingObserver() {
            this(true);
        }

        private RecordingObserver(boolean enabled) {
            this.enabled = enabled;
        }

        @Override
        public boolean enabled() {
            return enabled;
        }

        @Override
        public void onExecution(SqlExecutionObservation observation) {
            // SQL result observation is outside this cleanup boundary test.
        }

        @Override
        public void onResourceCleanup(ResourceCleanupObservation observation) {
            cleanup.add(observation);
        }
    }

    private static final class TrackingCompletionException extends CompletionException {
        private final AtomicInteger causeReads = new AtomicInteger();

        private TrackingCompletionException(Throwable cause) {
            super(cause);
        }

        @Override
        public synchronized Throwable getCause() {
            causeReads.incrementAndGet();
            return super.getCause();
        }

        private int causeReads() {
            return causeReads.get();
        }
    }
}
