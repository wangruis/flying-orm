package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.rdb.batch.*;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import io.r2dbc.spi.*;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.IntFunction;

import static org.junit.jupiter.api.Assertions.*;

class R2dbcBatchSymmetryTest {
    @Test void laterGeneratedExecuteFailureMakesAffectedTotalUnknown() {
        Harness h = new Harness(index -> {
            if (index == 1) throw new IllegalStateException("generated execute");
            return 10L;
        });
        var assigned = new ArrayList<Long>();
        var posted = new ArrayList<Long>();
        var error = assertThrows(BatchExecutionEvidenceException.class, () -> h.executor().writeBatch(
                request(3, false, assigned), offset -> Mono.fromRunnable(() -> posted.add(offset))).block());
        assertFalse(error.evidence().affectedRows().isKnown());
        assertEquals(List.of(0L), assigned);
        assertEquals(List.of(0L), posted);
        assertEquals(1, error.evidence().successfulCount());
    }

    @Test void protectedUpdateUsesOwnerReadForAcquisitionAndRelease() {
        Harness h = new Harness(index -> null);
        SqlRequest owner = new SqlRequest("select id from samples where id = ?", List.of(17L));
        SqlRequest main = new SqlRequest("update samples set value_col = ? where id = ?", List.of("next", 17L));
        ProtectedWriteWork work = work(ProtectedWriteWork.Kind.UPDATE, main, owner);
        BatchWriteRequest request = new BatchWriteRequest(main.statement(), List.of(String.class, Long.class),
                Flux.<Object[]>just(ProtectedBatchRows.extend(main.parameters().toArray(), work)),
                BatchWriteOptions.of(1), BatchRowCountPolicy.ANY, BatchGeneratedKeys.none());
        assertThrows(BatchExecutionEvidenceException.class, () -> h.executor().writeProtectedBatch(request).block());
        assertTrue(h.sql.getFirst().startsWith("select"));
        assertSame(owner, h.acquired);
        assertEquals(List.of(17L), h.acquired.parameters());
        assertEquals(1, h.released);
    }

    @Test void laterInvalidKeyPreservesEarlierCompletedPostAndSqlFacts() {
        Harness h = new Harness(index -> index == 2 ? null : index + 10L);
        var assigned = new ArrayList<Long>();
        var posted = new ArrayList<Long>();
        BatchExecutionEvidenceException error = assertThrows(BatchExecutionEvidenceException.class,
                () -> h.executor().writeBatch(request(4, false, assigned), offset -> {
                    posted.add(offset);
                    assertEquals(0, h.released);
                    return Mono.empty();
                }).block());
        assertEquals(List.of(0L, 1L), assigned);
        assertEquals(List.of(0L, 1L), posted);
        assertEquals(3, error.evidence().successfulCount());
        assertEquals(3, error.evidence().affectedRows().value());
        assertEquals(4, error.evidence().inputCount());
        assertEquals(3, h.generated);
        assertEquals(1, h.released);
    }

    @Test void failedTokenFlushDoesNotPublishPostForAppliedKeys() {
        Harness h = new Harness(index -> index + 10L);
        h.failTokens = true;
        var assigned = new ArrayList<Long>();
        var posted = new ArrayList<Long>();
        BatchExecutionEvidenceException error = assertThrows(BatchExecutionEvidenceException.class,
                () -> h.executor().writeProtectedBatch(request(2, true, assigned),
                        offset -> Mono.fromRunnable(() -> posted.add(offset))).block());
        assertEquals(List.of(0L, 1L), assigned);
        assertTrue(posted.isEmpty());
        assertEquals(2, error.evidence().successfulCount());
        assertEquals(2, error.evidence().affectedRows().value());
        assertEquals(1, h.released);
    }

    @Test void failedLobCleanupPreventsPartialPost() {
        Harness h = new Harness(index -> index == 0 ? 10L : brokenBlob());
        var assigned = new ArrayList<Long>();
        var posted = new ArrayList<Long>();
        BatchExecutionEvidenceException error = assertThrows(BatchExecutionEvidenceException.class,
                () -> h.executor().writeBatch(request(3, false, assigned),
                        offset -> Mono.fromRunnable(() -> posted.add(offset))).block());
        assertEquals(List.of(0L), assigned);
        assertTrue(posted.isEmpty());
        assertEquals(2, error.evidence().successfulCount());
        assertEquals(2, error.evidence().affectedRows().value());
        assertEquals(1, h.released);
    }

    @Test void earlierPostWaitsForLaterFailedKeyLobDiscard() {
        Sinks.Empty<Void> discarded = Sinks.empty();
        Blob blob = new Blob() {
            public Publisher<ByteBuffer> stream() { return Flux.error(new IllegalStateException("LOB read")); }
            public Publisher<Void> discard() { return discarded.asMono(); }
        };
        Harness h = new Harness(index -> index == 0 ? 10L : blob);
        var assigned = new ArrayList<Long>();
        var posted = new ArrayList<Long>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        var subscription = h.executor().writeBatch(request(3, false, assigned),
                        offset -> Mono.fromRunnable(() -> posted.add(offset)))
                .subscribe(value -> fail("failed key must not succeed"), failure::set);
        assertEquals(List.of(0L), assigned);
        assertTrue(posted.isEmpty());
        assertEquals(0, h.released);
        assertNull(failure.get());
        assertEquals(Sinks.EmitResult.OK, discarded.tryEmitEmpty());
        assertInstanceOf(BatchExecutionEvidenceException.class, failure.get());
        assertEquals(List.of(0L), posted);
        assertEquals(1, h.released);
        assertTrue(subscription.isDisposed());
    }

    @Test void partialPostFailureDoesNotSkipOtherEligibleRowsOrReplaceSqlFailure() {
        Harness h = new Harness(index -> index == 2 ? null : index + 10L);
        var assigned = new ArrayList<Long>();
        var posted = new ArrayList<Long>();
        var listener = new IllegalArgumentException("POST listener");
        BatchExecutionEvidenceException error = assertThrows(BatchExecutionEvidenceException.class,
                () -> h.executor().writeBatch(request(4, false, assigned), offset -> {
                    posted.add(offset);
                    return offset == 0 ? Mono.error(listener) : Mono.empty();
                }).block());
        assertEquals(List.of(0L, 1L), posted);
        assertInstanceOf(IllegalStateException.class, error.getCause());
        assertTrue(List.of(error.getCause().getSuppressed()).contains(listener));
        assertEquals(3, error.evidence().successfulCount());
        assertEquals(1, h.released);
    }

    @Test void partialCompletionUsesAbsoluteOffsetsAfterEarlierWindow() {
        Harness h = new Harness(index -> index == 3 ? null : index + 10L);
        var assigned = new ArrayList<Long>();
        var posted = new ArrayList<Long>();
        var base = request(4, false, assigned);
        var request = new BatchWriteRequest(base.statement(), base.parameterTypes(), base.rows(),
                BatchWriteOptions.of(2), base.rowCountPolicy(), base.generatedKeys());
        var error = assertThrows(BatchExecutionEvidenceException.class, () -> h.executor().writeBatch(request,
                offset -> Mono.fromRunnable(() -> posted.add(offset))).block());
        assertEquals(List.of(0L, 1L, 2L), posted);
        assertEquals(4, error.evidence().successfulCount());
        assertEquals(1, h.released);
    }

    private static Blob brokenBlob() {
        return new Blob() {
            public Publisher<ByteBuffer> stream() { return Flux.error(new IllegalStateException("LOB read")); }
            public Publisher<Void> discard() { return Mono.error(new IllegalStateException("LOB discard")); }
        };
    }

    private static BatchWriteRequest request(int size, boolean protectedRows, List<Long> assigned) {
        String sql = "insert into samples(value_col) values (?)";
        return new BatchWriteRequest(SqlStatementPlan.canonical(sql, SqlBindMarkerStyle.CANONICAL, 1),
                List.of(String.class), Flux.range(0, size).map(index -> {
                    Object[] values = {"value-" + index};
                    return protectedRows ? ProtectedBatchRows.extend(values, work(ProtectedWriteWork.Kind.INSERT,
                            new SqlRequest(sql, List.of(values)), null)) : values;
                }), BatchWriteOptions.of(size), BatchRowCountPolicy.ANY,
                BatchGeneratedKeys.required("id", (offset, key) -> assigned.add(offset)));
    }

    private static ProtectedWriteWork work(ProtectedWriteWork.Kind kind, SqlRequest main, SqlRequest owner) {
        return new ProtectedWriteWork(kind, main, owner, List.of("id"), Map.of(), "id = ?",
                "delete from token_index where id = ? and field_tag = ?",
                "insert into token_index(id,field_tag,token) values (?,?,?)",
                List.of(new ProtectedWriteWork.FieldTokens("value_col", List.of(new byte[]{1}))));
    }

    private static final class Harness implements R2dbcConnectionAccess {
        private final IntFunction<Object> keys;
        private final List<String> sql = new ArrayList<>();
        private int generated;
        private int released;
        private SqlRequest acquired;
        private boolean failTokens;
        private final Connection connection = proxy(Connection.class, (p, m, a) -> {
            if (!m.getName().equals("createStatement")) throw new AssertionError(m.getName());
            String text = (String) a[0];
            sql.add(text);
            return proxy(Statement.class, (statement, method, arguments) -> {
                if (!method.getName().equals("execute")) return statement;
                if (text.startsWith("select")) return Flux.empty();
                if (text.contains("token_index")) return failTokens
                        ? Flux.error(new IllegalStateException("token flush")) : Flux.empty();
                return Flux.just(nextResult());
            });
        });

        private Harness(IntFunction<Object> keys) { this.keys = keys; }
        private Result nextResult() { return result(keys.apply(generated++)); }
        private R2dbcSqlExecutor executor() { return R2dbcSqlExecutor.create(this, RdbDialect.h2()); }
        public Publisher<? extends Connection> getConnection(SqlRequest request) {
            return Mono.fromSupplier(() -> { acquired = request; return connection; });
        }
        public Publisher<Void> releaseConnection(SignalType signal, Connection actual, SqlRequest request) {
            return Mono.fromRunnable(() -> {
                assertSame(connection, actual);
                assertSame(acquired, request);
                released++;
            });
        }
    }

    @SuppressWarnings("unchecked")
    private static Result result(Object value) {
        ColumnMetadata column = proxy(ColumnMetadata.class, (p, m, a) -> switch (m.getName()) {
            case "getName" -> "id";
            case "getType" -> value instanceof Blob ? R2dbcType.BLOB : R2dbcType.BIGINT;
            case "getJavaType" -> value instanceof Blob ? Blob.class : Long.class;
            default -> null;
        });
        ColumnMetadata invalid = proxy(ColumnMetadata.class, (p, m, a) -> switch (m.getName()) {
            case "getName" -> "invalid";
            case "getType" -> R2dbcType.BIGINT;
            case "getJavaType" -> Long.class;
            default -> null;
        });
        List<ColumnMetadata> columns = value instanceof Blob ? List.of(column, invalid) : List.of(column);
        RowMetadata metadata = proxy(RowMetadata.class, (p, m, a) ->
                m.getName().equals("getColumnMetadatas") ? columns : column);
        Row row = proxy(Row.class, (p, m, a) -> {
            if (m.getName().equals("getMetadata")) return metadata;
            if (value instanceof Blob && a[0].equals(1)) throw new IllegalStateException("key row read");
            return value;
        });
        Result.RowSegment key = () -> row;
        Result.UpdateCount count = () -> 1L;
        return proxy(Result.class, (p, m, a) -> {
            if (!m.getName().equals("flatMap")) throw new AssertionError(m.getName());
            Function<Result.Segment, Publisher<Object>> consumer =
                    (Function<Result.Segment, Publisher<Object>>) a[0];
            return Flux.<Result.Segment>just(count, key).concatMap(segment -> Flux.from(consumer.apply(segment)));
        });
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler);
    }
}
