package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
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
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletionException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class R2dbcConnectionLeaseCleanupTest {

    @Test
    void errorCleanupReleasesLobsAndClosesOnceWithoutClassifyingTheSqlError() {
        AtomicInteger discards = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        TrackingCompletionException failure = new TrackingCompletionException(
                new IllegalStateException("database failed"));
        R2dbcConnectionLeaseCleanup cleanup = new R2dbcConnectionLeaseCleanup(SqlExecutionObserver.noop());
        R2dbcExecutionSession.ConnectionLease lease = new R2dbcExecutionSession.ConnectionLease(
                connection(closes, Mono.empty()), false);
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

        cleanup.closeAfterError(
                lease, SqlExecutionOperation.QUERY, SqlExecutionOptions.safeDefaults(), failure).block();

        assertEquals(1, discards.get(), "pending LOB locators must be released before connection return");
        assertEquals(1, closes.get(), "the owned connection must be returned exactly once");
        assertEquals(0, failure.causeReads(), "cleanup must not classify the SQL failure for pool reuse");
    }

    @Test
    void delegatesOrdinaryConnectionReleaseToTheDriverBoundary() throws InterruptedException {
        AtomicInteger closes = new AtomicInteger();
        Connection connection = connection(closes, Mono.never());
        R2dbcConnectionLeaseCleanup cleanup = new R2dbcConnectionLeaseCleanup(SqlExecutionObserver.noop());
        R2dbcExecutionSession.ConnectionLease lease =
                new R2dbcExecutionSession.ConnectionLease(connection, false);
        SqlExecutionOptions options = SqlExecutionOptions.safeDefaults()
                .withCleanupTimeout(Duration.ofMillis(50));

        CountDownLatch terminated = new CountDownLatch(1);
        Disposable subscription = cleanup.closeAfterResult(
                        lease, SqlExecutionOperation.QUERY, options, true)
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
    void neverClosesConnectionOwnedByExternalTransaction() {
        AtomicInteger creates = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        Connection connection = connection(closes, Mono.empty());
        ConnectionFactory connectionFactory = new ConnectionFactory() {
            @Override
            public org.reactivestreams.Publisher<? extends Connection> create() {
                creates.incrementAndGet();
                return Mono.error(new AssertionError("external transaction must bypass the connection factory"));
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "PostgreSQL";
            }
        };
        R2dbcExecutionSession session = new R2dbcExecutionSession(
                connectionFactory,
                R2dbcBindMarkers.from(connectionFactory),
                SqlExecutionObserver.noop(),
                () -> Mono.just(R2dbcTransactionContext.external(connection)));

        R2dbcExecutionSession.ConnectionLease lease = session.acquireConnection().block();
        assertTrue(lease.external());
        session.closeAfterResult(
                lease, SqlExecutionOperation.QUERY, SqlExecutionOptions.safeDefaults(), true).block();

        assertEquals(0, creates.get());
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
        R2dbcConnectionLeaseCleanup cleanup = new R2dbcConnectionLeaseCleanup(observer);
        R2dbcExecutionSession.ConnectionLease lease =
                new R2dbcExecutionSession.ConnectionLease(connection(new AtomicInteger(), failedClose), false);

        cleanup.closeAfterResult(
                lease, SqlExecutionOperation.QUERY, SqlExecutionOptions.safeDefaults(), true).block();

        assertEquals(1, closeSubscriptions.get());
        assertEquals(1, observer.cleanup.size());
        ResourceCleanupObservation observation = observer.cleanup.getFirst();
        assertEquals(ResourceCleanupObservation.Phase.CONNECTION_CLOSE, observation.phase());
        assertTrue(observation.outcomeConfirmed());
    }

    @Test
    void observesFailedCancelledQueryReleaseWithoutSubscribingCloseAgain() {
        AtomicInteger closeSubscriptions = new AtomicInteger();
        Mono<Void> failedClose = Mono.defer(() -> {
            closeSubscriptions.incrementAndGet();
            return Mono.error(new IllegalStateException("cancelled query close failed"));
        });
        RecordingObserver observer = new RecordingObserver();
        R2dbcConnectionLeaseCleanup cleanup = new R2dbcConnectionLeaseCleanup(observer);
        R2dbcExecutionSession.ConnectionLease lease =
                new R2dbcExecutionSession.ConnectionLease(connection(new AtomicInteger(), failedClose), false);

        cleanup.cancelAfterResult(
                lease, SqlExecutionOperation.QUERY, SqlExecutionOptions.safeDefaults()).block();

        assertEquals(1, closeSubscriptions.get());
        assertEquals(1, observer.cleanup.size());
        assertFalse(observer.cleanup.getFirst().outcomeConfirmed());
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

        @Override
        public boolean enabled() {
            return true;
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
