package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class R2dbcProtectedBatchSideIndexBatchingTest {

    private static final int ROW_COUNT = 8;

    @Test
    void ordinaryChunkDoesNotPreparePerRowProtectionState() {
        List<ProtectedBatchRows.RowView> rows = new ArrayList<>();
        for (int index = 0; index < 500; index++) {
            rows.add(ProtectedBatchRows.decode(new Object[]{"plain", (long) index}, 2));
        }
        R2dbcBatchWriterChunks.BatchChunk chunk = new R2dbcBatchWriterChunks.BatchChunk(0, 0L, rows, 0L);
        R2dbcProtectedBatchSideIndex.Prepared prepared = new R2dbcProtectedBatchSideIndex(
                R2dbcBindMarkers.from(connectionFactory(null))).prepare(null, request(), chunk,
                        () -> { throw new AssertionError("plain chunk must not acquire LOB state"); }, null)
                .block(Duration.ofSeconds(5));

        assertEquals(List.of(), prepared.rows());
    }

    @Test
    void ownerPreReadsUseOneBoundedStatementForTheChunk() {
        StatementRecorder recorder = new StatementRecorder();
        Connection connection = recorder.connection();
        ConnectionFactory factory = connectionFactory(connection);
        List<ProtectedBatchRows.RowView> rows = new ArrayList<>();
        for (int index = 0; index < ROW_COUNT; index++) {
            rows.add(row(updateWork(1_000L + index)));
        }
        R2dbcBatchWriterChunks.BatchChunk chunk = new R2dbcBatchWriterChunks.BatchChunk(
                0, 0, rows, 0L);

        new R2dbcProtectedBatchSideIndex(R2dbcBindMarkers.from(factory))
                .prepare(connection, request(), chunk, R2dbcLargeObjectScope::new, null)
                .block(Duration.ofSeconds(5));

        assertEquals(1, recorder.statements.get(),
                     "owner pre-read Statements must be bounded by the chunk, not multiplied by rows");
    }

    @Test
    void sideIndexDeletesUseOneBoundedStatementForTheChunk() {
        StatementRecorder recorder = new StatementRecorder();
        Connection connection = recorder.connection();
        ConnectionFactory factory = connectionFactory(connection);
        List<R2dbcProtectedBatchSideIndex.RowState> states = new ArrayList<>();
        for (int index = 0; index < ROW_COUNT; index++) {
            states.add(new R2dbcProtectedBatchSideIndex.RowState(
                    upsertWork(1_000L + index), List.of()));
        }

        new R2dbcProtectedBatchSideIndex(R2dbcBindMarkers.from(factory))
                .complete(connection, new R2dbcProtectedBatchSideIndex.Prepared(states),
                          BatchChunkResult.committed(0, 0, ROW_COUNT, ROW_COUNT))
                .block(Duration.ofSeconds(5));

        assertEquals(1, recorder.statements.get(),
                     "side-index DELETE Statements must be bounded by the chunk, not multiplied by owner fields");
    }

    @Test
    void updateReplacementBatchesNonEmptyTokensAcrossOwners() {
        assertReplacementBatchesAcrossOwners(ProtectedWriteWork.Kind.UPDATE);
    }

    @Test
    void upsertReplacementBatchesNonEmptyTokensAcrossOwners() {
        assertReplacementBatchesAcrossOwners(ProtectedWriteWork.Kind.UPSERT);
    }

    private static void assertReplacementBatchesAcrossOwners(ProtectedWriteWork.Kind kind) {
        StatementRecorder recorder = new StatementRecorder();
        List<R2dbcProtectedBatchSideIndex.RowState> states = new ArrayList<>();
        for (int index = 0; index < 500; index++) {
            states.add(state(kind, 1_000L + index, List.of(tokens("phone", index))));
        }

        complete(recorder, states);

        assertEquals(List.of("DELETE:500", "INSERT:500"), recorder.executionShapes(),
                     "replacement token execute calls must be batched across owners");
        Execution insert = recorder.executions.get(1);
        for (int index = 0; index < 500; index++) {
            assertEquals(List.of(1_000L + index, "phone", token(index)), insert.parameters().get(index));
        }
    }

    @Test
    void replacementTokensAcrossOwnersAndFieldsUseBoundedBatches() {
        StatementRecorder recorder = new StatementRecorder();
        List<byte[]> fieldTokens = java.util.stream.IntStream.range(0, 260)
                .mapToObj(index -> token(index).array()).toList();
        List<ProtectedWriteWork.FieldTokens> fields = List.of(
                new ProtectedWriteWork.FieldTokens("phone", fieldTokens),
                new ProtectedWriteWork.FieldTokens("email", fieldTokens));

        complete(recorder, List.of(
                state(ProtectedWriteWork.Kind.UPSERT, 7L, fields),
                state(ProtectedWriteWork.Kind.UPSERT, 8L, fields)));

        assertEquals(List.of("DELETE:4", "INSERT:500", "INSERT:500", "INSERT:40"),
                     recorder.executionShapes());
        List<List<Object>> parameters = recorder.executions.stream().skip(1)
                .flatMap(execution -> execution.parameters().stream()).toList();
        int offset = 0;
        for (long owner : List.of(7L, 8L)) {
            for (String field : List.of("phone", "email")) {
                for (int index = 0; index < 260; index++) {
                    assertEquals(List.of(owner, field, token(index)), parameters.get(offset++));
                }
            }
        }
    }

    @Test
    void repeatedOwnerFieldCompletesItsInsertBeforeTheNextDelete() {
        StatementRecorder recorder = new StatementRecorder();

        complete(recorder, List.of(
                state(ProtectedWriteWork.Kind.UPSERT, 7L, List.of(tokens("phone", 1))),
                state(ProtectedWriteWork.Kind.UPSERT, 8L, List.of(tokens("phone", 2))),
                state(ProtectedWriteWork.Kind.UPSERT, 7L, List.of(tokens("phone", 3)))));

        assertEquals(List.of("DELETE:2", "INSERT:2", "DELETE:1", "INSERT:1"),
                     recorder.executionShapes());
        assertEquals(List.of(List.of(7L, "phone", token(1)), List.of(8L, "phone", token(2))),
                     recorder.executions.get(1).parameters());
        assertEquals(List.of(List.of(7L, "phone", token(3))),
                     recorder.executions.get(3).parameters());
    }

    @Test
    void incorrectReplacementInsertCountStopsBeforeTheNextSegment() {
        StatementRecorder recorder = new StatementRecorder();
        recorder.insertCountAdjustment = -1L;
        List<R2dbcProtectedBatchSideIndex.RowState> states = List.of(
                state(ProtectedWriteWork.Kind.UPSERT, 7L, List.of(tokens("phone", 1))),
                state(ProtectedWriteWork.Kind.UPSERT, 8L, List.of(tokens("phone", 2))),
                state(ProtectedWriteWork.Kind.UPSERT, 7L, List.of(tokens("phone", 3))));

        IllegalStateException error = assertThrows(IllegalStateException.class, () -> complete(recorder, states));

        assertEquals("protected side index insert batch must affect one row per token", error.getMessage());
        assertEquals(List.of("DELETE:2", "INSERT:2"), recorder.executionShapes());
    }

    private static void complete(StatementRecorder recorder,
                                 List<R2dbcProtectedBatchSideIndex.RowState> states) {
        Connection connection = recorder.connection();
        new R2dbcProtectedBatchSideIndex(R2dbcBindMarkers.from(connectionFactory(connection)))
                .complete(connection, new R2dbcProtectedBatchSideIndex.Prepared(states),
                          BatchChunkResult.committed(0, 0, states.size(), states.size()))
                .block(Duration.ofSeconds(5));
    }

    private static R2dbcProtectedBatchSideIndex.RowState state(ProtectedWriteWork.Kind kind,
                                                               long ownerId,
                                                               List<ProtectedWriteWork.FieldTokens> fields) {
        boolean update = kind == ProtectedWriteWork.Kind.UPDATE;
        ProtectedWriteWork work = work(kind, ownerId,
                update ? new SqlRequest("select id from business_row where id = ?", List.of(ownerId)) : null,
                fields);
        return new R2dbcProtectedBatchSideIndex.RowState(
                work, update ? List.of(Map.of("id", ownerId)) : List.of());
    }

    private static ProtectedWriteWork.FieldTokens tokens(String field, int value) {
        return new ProtectedWriteWork.FieldTokens(field, List.of(token(value).array()));
    }

    private static ByteBuffer token(int value) {
        return ByteBuffer.allocate(Integer.BYTES).putInt(0, value);
    }

    private static BatchWriteRequest request() {
        return com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "update business_row set value_col = ? where id = ?",
                2,
                List.of(String.class, Long.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.empty(),
                BatchWriteOptions.defaults());
    }

    private static ProtectedBatchRows.RowView row(ProtectedWriteWork work) {
        Object[] parameters = work.writeRequest().parameters().toArray();
        return ProtectedBatchRows.decode(
                ProtectedBatchRows.extend(parameters, work), parameters.length);
    }

    private static ProtectedWriteWork updateWork(long ownerId) {
        return work(
                ProtectedWriteWork.Kind.UPDATE,
                ownerId,
                new SqlRequest("select id from business_row where id = ?", List.of(ownerId)));
    }

    private static ProtectedWriteWork upsertWork(long ownerId) {
        return work(ProtectedWriteWork.Kind.UPSERT, ownerId, null);
    }

    private static ProtectedWriteWork work(ProtectedWriteWork.Kind kind,
                                             long ownerId,
                                             SqlRequest ownerQuery) {
        return work(kind, ownerId, ownerQuery,
                    List.of(new ProtectedWriteWork.FieldTokens("phone", List.of())));
    }

    private static ProtectedWriteWork work(ProtectedWriteWork.Kind kind,
                                             long ownerId,
                                             SqlRequest ownerQuery,
                                             List<ProtectedWriteWork.FieldTokens> fields) {
        return new ProtectedWriteWork(
                kind,
                new SqlRequest("update business_row set value_col = ? where id = ?",
                               List.of("value", ownerId)),
                ownerQuery,
                List.of("id"),
                kind == ProtectedWriteWork.Kind.UPDATE ? Map.of() : Map.of("id", ownerId),
                "id = ?",
                "delete from token_index where id = ? and field_tag = ?",
                "insert into token_index(id, field_tag, token) values (?, ?, ?)",
                fields);
    }

    private static ConnectionFactory connectionFactory(Connection connection) {
        return new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return Mono.just(connection);
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "PostgreSQL";
            }
        };
    }

    private record Execution(String sql, List<List<Object>> parameters) {
    }

    private static final class StatementRecorder {

        private final AtomicInteger statements = new AtomicInteger();
        private final List<Execution> executions = new ArrayList<>();
        private long insertCountAdjustment;

        private List<String> executionShapes() {
            return executions.stream().map(execution ->
                    (execution.sql().startsWith("delete") ? "DELETE:" : "INSERT:")
                            + execution.parameters().size()).toList();
        }

        private Connection connection() {
            return (Connection) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{Connection.class},
                    (proxy, method, arguments) -> switch (method.getName()) {
                        case "createStatement" -> statement((String) arguments[0]);
                        case "toString" -> "protected-batch-counting-connection";
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
        }

        private Statement statement(String sql) {
            statements.incrementAndGet();
            Map<Integer, Object> bindings = new TreeMap<>();
            List<List<Object>> parameterSets = new ArrayList<>();
            Statement[] statement = new Statement[1];
            statement[0] = (Statement) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{Statement.class},
                    (proxy, method, arguments) -> switch (method.getName()) {
                        case "bind" -> {
                            bindings.put((Integer) arguments[0], arguments[1]);
                            yield statement[0];
                        }
                        case "bindNull" -> throw new AssertionError("side-index parameters must not be null");
                        case "add" -> {
                            parameterSets.add(List.copyOf(bindings.values()));
                            bindings.clear();
                            yield statement[0];
                        }
                        case "execute" -> {
                            parameterSets.add(List.copyOf(bindings.values()));
                            executions.add(new Execution(sql, List.copyOf(parameterSets)));
                            long count = parameterSets.size()
                                    + (sql.startsWith("insert") ? insertCountAdjustment : 0L);
                            yield Mono.just(result(count));
                        }
                        case "toString" -> "protected-batch-counting-statement";
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
            return statement[0];
        }

        private Result result(long count) {
            return (Result) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{Result.class},
                    (proxy, method, arguments) -> switch (method.getName()) {
                        case "map" -> Flux.empty();
                        case "getRowsUpdated" -> Mono.just(count);
                        case "toString" -> "protected-batch-counting-result";
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
        }
    }
}
