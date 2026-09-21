package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.dialect.RdbDialect;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.codec.SqlTypedValue;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class R2dbcBatchOwnedLobBindingTest {

    @Test
    void scalarBatchDoesNotCreateLargeObjectScope() {
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into samples(id) values (?)",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(new Object[]{7}),
                BatchWriteOptions.of(1));
        R2dbcExecutionSession.Resources handle = new R2dbcExecutionSession.Resources(scalarConnection(false), new com.flying.orm.core.sql.render.SqlRequest(request.statement(), List.of("sample")));

        executeSingleChunk(request, handle);

        assertNull(handle.largeObjectsIfCreated());
    }

    @Test
    void scalarGeneratedKeyDoesNotCreateLargeObjectScope() {
        AtomicInteger generatedKey = new AtomicInteger();
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into samples(name) values (?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(new Object[]{"sample"}),
                BatchWriteOptions.of(1),
                BatchRowCountPolicy.ANY,
                BatchGeneratedKeys.required("id", (offset, row) ->
                        generatedKey.set((Integer) row.value(0))));
        R2dbcExecutionSession.Resources handle = new R2dbcExecutionSession.Resources(scalarConnection(true), new com.flying.orm.core.sql.render.SqlRequest(request.statement(), List.of("sample")));

        executeSingleChunk(request, handle);

        assertEquals(7, generatedKey.get());
        assertNull(handle.largeObjectsIfCreated());
    }

    @Test
    void generatedKeyCallbackFailureKeepsTheCompletedMainWriteFact() {
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into samples(name) values (?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(new Object[]{"sample"}),
                BatchWriteOptions.of(1),
                BatchRowCountPolicy.ANY,
                BatchGeneratedKeys.required("id", (offset, row) -> {
                    throw new IllegalStateException("consumer failed");
                }));
        R2dbcBindMarkers bindMarkers = R2dbcBindMarkers.from(RdbDialect.h2());
        R2dbcBatchWriterChunks chunks = new R2dbcBatchWriterChunks(bindMarkers);
        R2dbcBatchWriterChunks.BatchChunk chunk = R2dbcBatchChunker.chunks(request).blockFirst();
        assertNotNull(chunk);

        BatchExecutionEvidenceException failure = assertThrows(
                BatchExecutionEvidenceException.class, () -> R2dbcSqlExecutor.create(
                        com.flying.orm.rdb.execution.ConnectionAccessTestSupport.borrowed(scalarConnection(true)),
                        RdbDialect.h2()).writeBatch(request).block());
        assertEquals(List.of(0L), failure.evidence().successfulOffsets().boxed().toList());
        assertTrue(failure.evidence().affectedRows().isKnown());
        assertEquals(1L, failure.evidence().affectedRows().value());
    }

    @Test
    void bindsOwnedSqlTypedBlobWithoutASecondPayloadSnapshot() {
        byte[] ownedPayload = {1, 2, 3};
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into samples(content) values (?)",
                1,
                List.of(byte[].class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(new Object[]{
                        new SqlTypedValue(SqlTypedValue.Kind.BLOB, ownedPayload)}),
                BatchWriteOptions.of(1));
        AtomicInteger observedFirstByte = new AtomicInteger(-1);
        R2dbcBindMarkers bindMarkers = R2dbcBindMarkers.from(RdbDialect.h2());
        R2dbcBatchWriterChunks chunks = new R2dbcBatchWriterChunks(bindMarkers);
        R2dbcBatchWriterChunks.BatchChunk chunk = R2dbcBatchChunker.chunks(request).blockFirst();
        assertNotNull(chunk);

        R2dbcBatchEvidenceCounts facts = new R2dbcBatchEvidenceCounts(1, 0, request.rowCountPolicy(), false);
        chunks.execute(connection(ownedPayload, observedFirstByte), request, chunk,
                bindMarkers.adapt(request), R2dbcLargeObjectScope::new, facts).block();

        assertEquals(9, observedFirstByte.get());
    }

    private static void executeSingleChunk(BatchWriteRequest request,
                                           R2dbcExecutionSession.Resources handle) {
        R2dbcBindMarkers bindMarkers = R2dbcBindMarkers.from(RdbDialect.h2());
        R2dbcBatchWriterChunks chunks = new R2dbcBatchWriterChunks(bindMarkers);
        R2dbcBatchWriterChunks.BatchChunk chunk = R2dbcBatchChunker.chunks(request).blockFirst();
        assertNotNull(chunk);
        chunks.execute(handle.connection(), request, chunk, bindMarkers.adapt(request),
                handle::largeObjects, new R2dbcBatchEvidenceCounts(
                        chunk.rows().size(), chunk.startOffset(), request.rowCountPolicy(), false)).block();
    }

    private static Connection scalarConnection(boolean generatedKey) {
        Statement statement = (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class<?>[]{Statement.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "bind", "bindNull", "add", "returnGeneratedValues" -> proxy;
                    case "execute" -> Flux.just(generatedKey ? generatedKeyResult() : successfulResult());
                    case "toString" -> "scalar statement";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "createStatement" -> statement;
                    case "toString" -> "scalar connection";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    @SuppressWarnings("unchecked")
    private static Result generatedKeyResult() {
        RowMetadata metadata = new RowMetadata() {
            private final ColumnMetadata column = new ColumnMetadata() {
                @Override
                public R2dbcType getType() {
                    return R2dbcType.INTEGER;
                }

                @Override
                public String getName() {
                    return "id";
                }
            };

            @Override
            public ColumnMetadata getColumnMetadata(int index) {
                return column;
            }

            @Override
            public ColumnMetadata getColumnMetadata(String name) {
                return column;
            }

            @Override
            public List<? extends ColumnMetadata> getColumnMetadatas() {
                return List.of(column);
            }
        };
        Row row = new Row() {
            @Override
            public RowMetadata getMetadata() {
                return metadata;
            }

            @Override
            public <T> T get(int index, Class<T> type) {
                return type.cast(7);
            }

            @Override
            public <T> T get(String name, Class<T> type) {
                return type.cast(7);
            }
        };
        Result.RowSegment segment = () -> row;
        return (Result) Proxy.newProxyInstance(
                Result.class.getClassLoader(),
                new Class<?>[]{Result.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "flatMap" -> {
                        Function<Result.Segment, Publisher<?>> mapper =
                                (Function<Result.Segment, Publisher<?>>) arguments[0];
                        yield Flux.from(mapper.apply(segment));
                    }
                    case "toString" -> "scalar generated-key result";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Connection connection(byte[] ownedPayload, AtomicInteger observedFirstByte) {
        Statement statement = (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(),
                new Class<?>[]{Statement.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "bind" -> {
                        Blob blob = (Blob) arguments[1];
                        ownedPayload[0] = 9;
                        ByteBuffer content = Flux.from(blob.stream()).blockFirst();
                        assertNotNull(content);
                        assertTrue(content.isReadOnly());
                        observedFirstByte.set(content.get(0));
                        yield proxy;
                    }
                    case "bindNull", "add" -> proxy;
                    case "execute" -> Flux.just(successfulResult());
                    case "toString" -> "owned-blob statement";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "createStatement" -> statement;
                    case "toString" -> "owned-blob connection";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Result successfulResult() {
        return (Result) Proxy.newProxyInstance(
                Result.class.getClassLoader(),
                new Class<?>[]{Result.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getRowsUpdated" -> Mono.just(1L);
                    case "toString" -> "one-row result";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

}
