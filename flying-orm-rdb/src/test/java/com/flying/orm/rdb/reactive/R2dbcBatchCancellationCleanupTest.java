package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.*;
import com.flying.orm.rdb.batch.*;
import com.flying.orm.rdb.dialect.RdbDialect;
import io.r2dbc.spi.*;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.*;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import static org.junit.jupiter.api.Assertions.*;

class R2dbcBatchCancellationCleanupTest {
    @Test
    @SuppressWarnings("unchecked")
    void cancellationWaitsForOwnedLobDiscardBeforeOneUpperRelease() {
        Sinks.Empty<Void> discardDone = Sinks.empty();
        AtomicInteger discards = new AtomicInteger();
        AtomicInteger releases = new AtomicInteger();
        AtomicInteger posts = new AtomicInteger();
        Blob blob = new Blob() {
            public Publisher<ByteBuffer> stream() { return Mono.never(); }
            public Publisher<Void> discard() {
                return Mono.defer(() -> {
                    discards.incrementAndGet();
                    return discardDone.asMono();
                });
            }
        };
        ColumnMetadata column = proxy(ColumnMetadata.class, (p, m, a) -> switch (m.getName()) {
            case "getName" -> "id";
            case "getType" -> R2dbcType.BLOB;
            case "getJavaType" -> Blob.class;
            default -> null;
        });
        Blob streaming = new Blob() {
            public Publisher<ByteBuffer> stream() { return Mono.never(); }
            public Publisher<Void> discard() { return Mono.empty(); }
        };
        ColumnMetadata firstColumn = proxy(ColumnMetadata.class, (p, m, a) -> switch (m.getName()) {
            case "getName" -> "first";
            case "getType" -> R2dbcType.BLOB;
            case "getJavaType" -> Blob.class;
            default -> null;
        });
        RowMetadata metadata = proxy(RowMetadata.class, (p, m, a) ->
                m.getName().equals("getColumnMetadatas") ? List.of(firstColumn, column)
                        : ((Integer) a[0] == 0 ? firstColumn : column));
        Row row = proxy(Row.class, (p, m, a) -> m.getName().equals("getMetadata") ? metadata
                : ((Integer) a[0] == 0 ? streaming : blob));
        Result.RowSegment segment = () -> row;
        Result result = proxy(Result.class, (p, m, a) -> {
            if (m.getName().equals("flatMap")) {
                Function<Result.Segment, Publisher<Object>> mapper = (Function<Result.Segment, Publisher<Object>>) a[0];
                return Flux.just(segment).concatMap(value -> Flux.from(mapper.apply(value)));
            }
            throw new UnsupportedOperationException(m.getName());
        });
        Statement statement = proxy(Statement.class, (p, m, a) -> switch (m.getName()) {
            case "bind", "bindNull", "returnGeneratedValues" -> p;
            case "execute" -> Flux.just(result);
            default -> throw new UnsupportedOperationException(m.getName());
        });
        Connection connection = proxy(Connection.class, (p, m, a) -> {
            if (m.getName().equals("createStatement")) return statement;
            throw new AssertionError("unexpected connection ownership call " + m.getName());
        });
        R2dbcConnectionAccess access = new R2dbcConnectionAccess() {
            public Publisher<? extends Connection> getConnection(SqlRequest request) { return Mono.just(connection); }
            public Publisher<Void> releaseConnection(SignalType signal, Connection actual, SqlRequest request) {
                return Mono.fromRunnable(() -> {
                    assertEquals(SignalType.CANCEL, signal);
                    assertSame(connection, actual);
                    releases.incrementAndGet();
                });
            }
        };
        BatchWriteRequest request = new BatchWriteRequest(SqlStatementPlan.canonical(
                "insert into sample(value) values (?)", SqlBindMarkerStyle.CANONICAL, 1),
                List.of(Integer.class), Flux.<Object[]>just(new Object[]{1}), BatchWriteOptions.of(1),
                BatchRowCountPolicy.ANY, BatchGeneratedKeys.required("id", (offset, key) -> {}));
        var subscription = R2dbcSqlExecutor.create(access, RdbDialect.h2()).writeBatch(
                request, offset -> Mono.fromRunnable(posts::incrementAndGet)).subscribe();
        subscription.dispose();

        assertEquals(1, discards.get());
        assertEquals(0, releases.get(), "upper release must await pending LOB discard completion");
        assertEquals(Sinks.EmitResult.OK, discardDone.tryEmitEmpty());
        assertEquals(1, releases.get());
        assertEquals(1, discards.get(), "owned LOB discard must only be subscribed once");
        assertEquals(0, posts.get());
    }

    @SuppressWarnings("unchecked")
    private static <T> T proxy(Class<T> type, java.lang.reflect.InvocationHandler handler) {
        return (T) Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler);
    }
}
