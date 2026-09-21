package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.*;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.lifecycle.*;
import com.flying.orm.rdb.observation.*;
import io.r2dbc.spi.*;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.*;
import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntFunction;
import static org.junit.jupiter.api.Assertions.*;

class R2dbcOrdinaryBatchConnectionTest {
    @Test void ordinaryBatchUsesOneColdAcquisitionAndActualFirstBinding() {
        Harness h = new Harness(index -> Flux.just(2L));
        Mono<BatchExecutionEvidence> operation = h.executor().writeBatch(request(5, 2, BatchRowCountPolicy.ANY));
        assertEquals(0, h.gets);
        BatchExecutionEvidence first = operation.block();
        assertEquals(5, first.successfulCount());
        assertEquals(5, first.inputCount());
        assertEquals(1, h.gets);
        assertEquals(List.of(0), h.first.parameters());
        assertEquals(List.of(SignalType.ON_COMPLETE), h.releases);
        assertEquals(3, h.executions);
        operation.block();
        assertEquals(2, h.gets);
        assertEquals(2, h.releases.size());
    }

    @Test void emptyInputNeverAcquires() {
        Harness h = new Harness(index -> Flux.just(1L));
        assertEquals(0, h.executor().writeBatch(request(0, 2, BatchRowCountPolicy.ANY)).block().inputCount());
        assertEquals(0, h.gets);
        assertTrue(h.releases.isEmpty());
    }

    @Test void absentAndNegativeCountsStillProveSuccessfulSqlWithUnknownRows() {
        for (Publisher<Long> counts : List.of(Flux.<Long>empty(), Flux.just(-1L))) {
            Harness h = new Harness(index -> counts);
            BatchExecutionEvidence fact = h.executor().writeBatch(request(2, 2, BatchRowCountPolicy.ANY)).block();
            assertEquals(2, fact.successfulCount());
            assertFalse(fact.affectedRows().isKnown());
            assertEquals(BatchExecutionState.SUCCESS, fact.state());
        }
    }

    @Test void incompleteCountStreamDoesNotInventSuccessfulPositions() {
        Harness h = new Harness(index -> Flux.concat(Flux.just(1L), Flux.error(new IllegalStateException("driver"))));
        BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                () -> h.executor().writeBatch(request(2, 2, BatchRowCountPolicy.ANY)).block());
        assertEquals(2, failure.evidence().inputCount());
        assertEquals(0, failure.evidence().successfulCount());
        assertEquals(0, failure.evidence().failedCount());
        assertEquals(List.of(SignalType.ON_ERROR), h.releases);
    }

    @Test void strictConflictsKeepEverySparsePosition() {
        Harness h = new Harness(index -> Flux.just(index == 1 || index == 3 ? 0L : 1L));
        BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                () -> h.executor().writeBatch(request(5, 5, BatchRowCountPolicy.EXACTLY_ONE)).block());
        assertArrayEquals(new long[]{0, 2, 4}, failure.evidence().successfulOffsets().toArray());
        assertArrayEquals(new long[]{1, 3}, failure.evidence().failedOffsets().toArray());
        assertEquals(2, failure.evidence().conflicts().size());
    }

    @Test void postFailureStaysPrimaryRunsCompletedWindowAndStopsNextWindow() {
        Harness h = new Harness(index -> Flux.just(3L));
        List<Long> callbacks = new ArrayList<>();
        EntityPostWriteException primary = new EntityPostWriteException(
                EntityLifecyclePhase.POST_PERSIST, 0L, new IllegalArgumentException("listener"));
        Throwable failure = assertThrows(EntityPostWriteException.class, () -> h.executor().writeBatch(
                request(6, 3, BatchRowCountPolicy.ANY), offset -> {
                    callbacks.add(offset);
                    return offset == 0 ? Mono.error(primary) : Mono.empty();
                }).block());
        assertSame(primary, failure);
        assertEquals(List.of(0L, 1L, 2L), callbacks);
        assertEquals(1, h.executions);
        BatchExecutionEvidenceException attached = assertInstanceOf(
                BatchExecutionEvidenceException.class, primary.getSuppressed()[0]);
        assertNull(attached.getCause());
        assertEquals(3, attached.evidence().successfulCount());
    }

    @Test void releaseFailureIsVisibleAndDoesNotEraseSqlEvidence() {
        Harness h = new Harness(index -> Flux.just(2L));
        h.releaseFailure = new IllegalStateException("release");
        BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                () -> h.executor().writeBatch(request(2, 2, BatchRowCountPolicy.ANY)).block());
        assertEquals(2, failure.evidence().successfulCount());
        assertSame(h.releaseFailure, failure.getCause());
        assertEquals(1, h.releases.size());
    }

    @Test void getFailureDoesNotRelease() {
        Harness h = new Harness(index -> Flux.just(1L));
        h.getFailure = new IllegalArgumentException("get");
        BatchExecutionEvidenceException error = assertThrows(BatchExecutionEvidenceException.class,
                () -> h.executor().writeBatch(request(3, 2, BatchRowCountPolicy.ANY)).block());
        assertEquals(2, error.evidence().inputCount());
        assertTrue(h.releases.isEmpty());
        assertEquals(0, h.executions);
    }

    @Test void stalledExecutionDoesNotPrefetchAnotherWindowAndCancelReleasesOnce() {
        Harness h = new Harness(index -> Flux.never());
        AtomicInteger accepted = new AtomicInteger();
        AtomicReferenceEvidence observer = new AtomicReferenceEvidence();
        BatchWriteRequest request = request(Flux.range(0, 20).map(i -> new Object[]{i})
                .doOnNext(ignored -> accepted.incrementAndGet()), 2, BatchRowCountPolicy.ANY);
        var subscription = h.executor().withBatchObserver(observer).writeBatch(request).subscribe();
        assertEquals(2, accepted.get());
        subscription.dispose();
        assertEquals(List.of(SignalType.CANCEL), h.releases);
        assertNotNull(observer.fact);
        assertEquals(BatchExecutionState.CANCELLED, observer.fact.state());
        assertEquals(2, observer.fact.inputCount());
    }

    @Test void sameConnectionScopePreservesDecoratorsAndReleasesAfterBothStatements() {
        Harness h = new Harness(index -> Flux.just(1L));
        List<SqlExecutionObservation> events = new ArrayList<>();
        ReactiveSqlExecutor executor = h.executor().withDefaultExecutionOptions(SqlExecutionOptions.unlimited())
                .withObserver(events::add).withBatchMemoryLimits(BatchMemoryLimits.defaults());
        SqlRequest first = new SqlRequest("update sample set value = ?", List.of(1));
        assertEquals(2L, executor.withConnection(first, bound ->
                bound.rowsUpdated(first).flatMap(a -> bound.rowsUpdated(first).map(b -> a + b))).block());
        assertEquals(1, h.gets);
        assertEquals(1, h.releases.size());
        assertEquals(2, events.size());
    }

    static BatchWriteRequest request(int size, int buffer, BatchRowCountPolicy policy) {
        return request(Flux.range(0, size).map(i -> new Object[]{i}), buffer, policy);
    }

    static BatchWriteRequest request(Publisher<Object[]> rows, int buffer, BatchRowCountPolicy policy) {
        return new BatchWriteRequest(SqlStatementPlan.canonical(
                "insert into sample(value) values (?)", SqlBindMarkerStyle.CANONICAL, 1),
                List.of(Integer.class), rows, BatchWriteOptions.of(buffer), policy, BatchGeneratedKeys.none());
    }

    private static final class AtomicReferenceEvidence implements BatchExecutionObserver {
        volatile BatchExecutionEvidence fact;
        public void onExecution(BatchExecutionObservation observation) {}
        public void onExecutionEvidence(BatchExecutionEvidence evidence) { fact = evidence; }
    }

    static final class Harness implements R2dbcConnectionAccess {
        final IntFunction<Publisher<Long>> counts;
        final List<SignalType> releases = new ArrayList<>();
        int gets;
        int executions;
        SqlRequest first;
        Throwable releaseFailure;
        Throwable getFailure;
        int failBeforeExecutionAt = -1;
        final Connection connection = proxy(Connection.class, (object, method, args) -> {
            if (method.getName().equals("createStatement")) {
                if (executions == failBeforeExecutionAt) throw new IllegalStateException("create statement");
                return statement();
            }
            if (method.getName().equals("toString")) return "supplied-connection";
            throw new AssertionError("ORM called connection lifecycle method: " + method.getName());
        });

        Harness(IntFunction<Publisher<Long>> counts) { this.counts = counts; }
        R2dbcSqlExecutor executor() { return R2dbcSqlExecutor.create(this, RdbDialect.postgresql()); }

        public Publisher<? extends Connection> getConnection(SqlRequest request) {
            return Mono.defer(() -> {
                gets++;
                first = request;
                return getFailure == null ? Mono.just(connection) : Mono.error(getFailure);
            });
        }

        public Publisher<Void> releaseConnection(SignalType signal, Connection connection, SqlRequest request) {
            return Mono.defer(() -> {
                assertSame(this.connection, connection);
                assertSame(first, request);
                releases.add(signal);
                return releaseFailure == null ? Mono.empty() : Mono.error(releaseFailure);
            });
        }

        Statement statement() {
            return proxy(Statement.class, (object, method, args) -> switch (method.getName()) {
                case "bind", "bindNull", "add", "fetchSize", "returnGeneratedValues" -> object;
                case "execute" -> {
                    Publisher<Long> rowCounts = counts.apply(executions++);
                    yield Flux.just(proxy(Result.class, (r, m, a) -> {
                        if (m.getName().equals("getRowsUpdated")) return rowCounts;
                        throw new UnsupportedOperationException(m.getName());
                    }));
                }
                default -> throw new UnsupportedOperationException(method.getName());
            });
        }
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler);
    }
}
