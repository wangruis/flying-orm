package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteCompletion;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.codec.SqlTypedValue;
import io.r2dbc.spi.Blob;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
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
                BatchWriteOptions.atomic(1));
        R2dbcBatchConnectionHandle handle = new R2dbcBatchConnectionHandle(scalarConnection(false));

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
                BatchWriteOptions.atomic(1),
                BatchRowCountPolicy.ANY,
                BatchGeneratedKeys.required("id", (offset, row) ->
                        generatedKey.set((Integer) row.value(0))),
                BatchWriteCompletion.noop());
        R2dbcBatchConnectionHandle handle = new R2dbcBatchConnectionHandle(scalarConnection(true));

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
                BatchWriteOptions.atomic(1),
                BatchRowCountPolicy.ANY,
                BatchGeneratedKeys.required("id", (offset, row) -> {
                    throw new IllegalStateException("consumer failed");
                }),
                BatchWriteCompletion.noop());
        R2dbcBindMarkers bindMarkers = R2dbcBindMarkers.from(metadataOnlyFactory());
        R2dbcBatchWriterChunks chunks = new R2dbcBatchWriterChunks(bindMarkers);
        R2dbcBatchWriterChunks.BatchChunk chunk = chunks.chunks(request).blockFirst();
        assertNotNull(chunk);

        R2dbcBatchEvidenceFailure failure = assertThrows(
                R2dbcBatchEvidenceFailure.class,
                () -> chunks.executeBatchEvidence(
                        new R2dbcBatchConnectionHandle(scalarConnection(true)),
                        request,
                        chunk,
                        bindMarkers.adapt(request),
                        new R2dbcBatchEvidenceCounts()).block());

        assertEquals(List.of(0L), failure.fact().successfulOffsets());
        assertTrue(failure.fact().affectedRows().isKnown());
        assertEquals(1L, failure.fact().affectedRows().value());
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
                BatchWriteOptions.atomic(1));
        AtomicInteger observedFirstByte = new AtomicInteger(-1);
        R2dbcBindMarkers bindMarkers = R2dbcBindMarkers.from(metadataOnlyFactory());
        R2dbcBatchWriterChunks chunks = new R2dbcBatchWriterChunks(bindMarkers);
        R2dbcBatchWriterChunks.BatchChunk chunk = chunks.chunks(request).blockFirst();
        assertNotNull(chunk);

        BatchChunkResult result = chunks.executeChunk(
                new R2dbcBatchConnectionHandle(connection(ownedPayload, observedFirstByte)),
                request,
                chunk,
                bindMarkers.adapt(request)).block();

        assertNotNull(result);
        assertEquals(BatchChunkResult.Status.COMMITTED, result.status());
        assertEquals(9, observedFirstByte.get());
    }

    private static void executeSingleChunk(BatchWriteRequest request,
                                           R2dbcBatchConnectionHandle handle) {
        R2dbcBindMarkers bindMarkers = R2dbcBindMarkers.from(metadataOnlyFactory());
        R2dbcBatchWriterChunks chunks = new R2dbcBatchWriterChunks(bindMarkers);
        R2dbcBatchWriterChunks.BatchChunk chunk = chunks.chunks(request).blockFirst();
        assertNotNull(chunk);
        assertNotNull(chunks.executeChunk(handle, request, chunk, bindMarkers.adapt(request)).block());
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

    private static ConnectionFactory metadataOnlyFactory() {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return Mono.error(new AssertionError("connection must be supplied by the test"));
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "H2";
            }
        };
    }
}
