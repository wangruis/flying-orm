package com.flying.orm.rdb.execution;

import com.flying.orm.rdb.dialect.RdbDialect;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteRequests;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchAffectedRows;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.jdbc.JdbcBatchWriter;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.R2dbcType;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Row;
import io.r2dbc.spi.RowMetadata;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Types;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtectedBatchGeneratedKeyBatchingTest {

    private static final int ROWS = 500;

    @TestFactory
    Stream<DynamicTest> legalInputBudgetPreservesAllGeneratedOwnerTokens() {
        return Stream.of(false, true).map(reactive -> DynamicTest.dynamicTest(
                "small input budget retains generated owner tokens [reactive=" + reactive + "]", () -> {
                    Recorder recorder = new Recorder();
                    List<Key> callbacks = new ArrayList<>();
                    ProtectedWriteWork base = generatedInsertWork("cipher");
                    ProtectedWriteWork work = new ProtectedWriteWork(base.kind(), base.writeRequest(), null,
                            base.ownerFields(), Map.of(), base.ownerPredicateSql(), base.deleteSql(), base.insertSql(),
                            List.of(new ProtectedWriteWork.FieldTokens("secret",
                                    IntStream.range(0, 120).mapToObj(index -> new byte[4]).toList())));
                    Object[] row = ProtectedBatchRows.extend(new Object[]{"cipher"}, work);
                    assertTrue(ProtectedBatchRows.estimateRowBytes(row, 1) < 8_000L);
                    BatchWriteRequest request = BatchWriteRequests.request(
                            base.writeRequest().sql(), 1, List.of(String.class), SqlBindMarkerStyle.CANONICAL,
                            Flux.<Object[]>just(row), new BatchWriteOptions(1, 1, 8_000L, 8_000L),
                            BatchRowCountPolicy.EXACTLY_ONE,
                            BatchGeneratedKeys.required("id", (offset, key) ->
                                    callbacks.add(new Key(offset, ((Number) key.value(0)).longValue()))));

                    BatchExecutionEvidence result = execute(reactive, recorder, request);

                    assertEquals(BatchExecutionState.SUCCESS, result.state());
                    assertEquals(List.of(new Key(0, 1)), callbacks);
                    assertEquals(120, recorder.tokenParameterSets);
                    assertTrue(recorder.tokenBatchSizes.stream()
                            .allMatch(size -> size > 0 && size <= 500 && size * 3 <= 2_000));
                    assertEquals(java.util.Collections.nCopies(120, 1L), recorder.tokenOwners);
                    assertEquals(0, recorder.begins);
                    assertEquals(0, recorder.commits);
                    assertEquals(0, recorder.rollbacks);
                    assertEquals(0, recorder.closes);
                }));
    }

    @TestFactory
    Stream<DynamicTest> batchesGeneratedOwnerTokensOnBothExecutors() {
        return Stream.of(false, true)
                .map(reactive -> DynamicTest.dynamicTest(
                        "batches generated owner tokens [reactive=" + reactive + "]",
                        () -> assertBatchedGeneratedOwners(reactive)));
    }

    @TestFactory
    Stream<DynamicTest> invalidGeneratedOwnerTokenCountsPreserveExternalOwnershipOnBothExecutors() {
        return Stream.of(false, true)
                .map(reactive -> DynamicTest.dynamicTest(
                        "rejects invalid generated owner token counts [reactive="
                                + reactive + "]",
                        () -> {
                            Recorder recorder = new Recorder();
                            recorder.invalidTokenCounts = true;
                            List<Key> callbacks = new ArrayList<>();

                            BatchExecutionEvidenceException failure = assertThrows(
                                    BatchExecutionEvidenceException.class,
                                    () -> execute(
                                            reactive, recorder, request(callbacks)));

                            assertEquals(BatchExecutionState.PARTIAL, failure.evidence().state());
                            assertEquals(ROWS, failure.evidence().successfulCount());
                            assertEquals(BatchAffectedRows.known(ROWS), failure.evidence().affectedRows());
                            assertEquals(ROWS, recorder.businessWrites);
                            assertEquals(1, recorder.tokenExecutions);
                            assertEquals(ROWS, recorder.tokenParameterSets);
                            assertEquals(0, recorder.commits);
                            assertEquals(0, recorder.rollbacks);
                            assertEquals(0, recorder.closes);
                            assertEquals(ROWS, callbacks.size());
                        }));
    }

    private static void assertBatchedGeneratedOwners(boolean reactive) {
        Recorder recorder = new Recorder();
        List<Key> callbacks = new ArrayList<>();

        BatchExecutionEvidence result = execute(reactive, recorder, request(callbacks));

        assertEquals(BatchExecutionState.SUCCESS, result.state());
        assertEquals(ROWS, result.inputCount());
        // Single-row inserts can prove one affected row through the returned generated key alone.
        assertEquals(BatchAffectedRows.known(ROWS), result.affectedRows());
        assertEquals(ROWS, result.successfulCount());
        assertEquals(0, result.failedCount());
        assertEquals(0, recorder.begins);
        assertEquals(ROWS, recorder.businessWrites);
        assertEquals(1, recorder.tokenExecutions);
        assertEquals(ROWS, recorder.tokenParameterSets);
        assertEquals(0, recorder.commits);
        assertEquals(0, recorder.rollbacks);
        assertEquals(0, recorder.closes);
        assertEquals(
                IntStream.range(0, ROWS)
                        .mapToObj(index -> new Key(index, index + 1L))
                        .toList(),
                callbacks);
    }

    private static BatchWriteRequest request(List<Key> callbacks) {
        Publisher<Object[]> rows = Flux.range(0, ROWS)
                .map(index -> {
                    String value = "cipher-" + index;
                    return ProtectedBatchRows.extend(
                            new Object[]{value}, generatedInsertWork(value));
                });
        return BatchWriteRequests.request(
                "insert into business_row(secret) values (?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                rows,
                BatchWriteOptions.of(ROWS),
                BatchRowCountPolicy.EXACTLY_ONE,
                BatchGeneratedKeys.required("id", (offset, row) ->
                        callbacks.add(new Key(offset, ((Number) row.value(0)).longValue()))));
    }

    private static ProtectedWriteWork generatedInsertWork(String value) {
        return new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into business_row(secret) values (?)", List.of(value)),
                null,
                List.of("id"),
                Map.of(),
                "id = ?",
                "delete from token_index where id = ? and field_tag = ?",
                "insert into token_index(id, field_tag, token_hash) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens(
                        "secret", List.of(new byte[32]))));
    }

    private static BatchExecutionEvidence execute(boolean reactive,
                                            Recorder recorder,
                                            BatchWriteRequest request) {
        if (!reactive) {
            java.sql.Connection connection = jdbcConnection(recorder);
            return JdbcBatchWriter.create(ConnectionAccessTestSupport.borrowed(connection), RdbDialect.postgresql())
                    .writeBatch(request);
        }
        Connection connection = r2dbcConnection(recorder);
        return R2dbcSqlExecutor.create(ConnectionAccessTestSupport.borrowed(connection), RdbDialect.postgresql())
                .writeBatch(request)
                .block(Duration.ofSeconds(10));
    }

    private static java.sql.Connection jdbcConnection(Recorder recorder) {
        return proxy(
                java.sql.Connection.class,
                (ignored, method, arguments) -> switch (method.getName()) {
                    case "getAutoCommit" -> true;
                    case "setAutoCommit" -> {
                        recorder.begins++;
                        yield null;
                    }
                    case "prepareStatement" -> jdbcStatement((String) arguments[0], recorder);
                    case "commit" -> {
                        recorder.commits++;
                        yield null;
                    }
                    case "rollback" -> {
                        recorder.rollbacks++;
                        yield null;
                    }
                    case "close" -> {
                        recorder.closes++;
                        yield null;
                    }
                    case "isClosed" -> false;
                    case "toString" -> "generated-key-jdbc-connection";
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static PreparedStatement jdbcStatement(String sql, Recorder recorder) {
        if (sql.startsWith("insert into business_row")) {
            return proxy(
                    PreparedStatement.class,
                    (ignored, method, arguments) -> switch (method.getName()) {
                        case "executeLargeUpdate", "executeUpdate" -> {
                            recorder.businessWrites++;
                            recorder.currentKey = recorder.businessWrites;
                            yield method.getReturnType() == long.class ? 1L : 1;
                        }
                        case "getGeneratedKeys" -> jdbcGeneratedKey(recorder.currentKey);
                        case "isClosed" -> false;
                        case "toString" -> sql;
                        default -> defaultValue(method.getReturnType());
                    });
        }
        AtomicInteger parameterSets = new AtomicInteger();
        List<Long> owners = new ArrayList<>();
        long[] currentOwner = new long[1];
        return proxy(
                PreparedStatement.class,
                (ignored, method, arguments) -> switch (method.getName()) {
                    case "addBatch" -> {
                        parameterSets.incrementAndGet();
                        owners.add(currentOwner[0]);
                        yield null;
                    }
                    case "setObject", "setLong" -> {
                        if ((Integer) arguments[0] == 1) currentOwner[0] = ((Number) arguments[1]).longValue();
                        yield null;
                    }
                    case "executeBatch" -> {
                        recorder.tokenExecutions++;
                        recorder.tokenParameterSets += parameterSets.get();
                        recorder.tokenBatchSizes.add(parameterSets.get());
                        recorder.tokenOwners.addAll(owners);
                        yield IntStream.range(0, parameterSets.get())
                                .map(index -> recorder.invalidTokenCounts ? 0 : 1)
                                .toArray();
                    }
                    case "isClosed" -> false;
                    case "toString" -> sql;
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static ResultSet jdbcGeneratedKey(long value) {
        ResultSetMetaData metadata = proxy(
                ResultSetMetaData.class,
                (ignored, method, arguments) -> switch (method.getName()) {
                    case "getColumnCount" -> 1;
                    case "getColumnLabel", "getColumnName" -> "id";
                    case "getColumnType" -> Types.BIGINT;
                    case "getColumnTypeName" -> "BIGINT";
                    case "getColumnClassName" -> Long.class.getName();
                    default -> defaultValue(method.getReturnType());
                });
        AtomicInteger next = new AtomicInteger();
        return proxy(
                ResultSet.class,
                (ignored, method, arguments) -> switch (method.getName()) {
                    case "getMetaData" -> metadata;
                    case "next" -> next.getAndIncrement() == 0;
                    case "getObject", "getLong" -> value;
                    case "findColumn" -> 1;
                    case "wasNull", "isClosed" -> false;
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static Connection r2dbcConnection(Recorder recorder) {
        return proxy(
                Connection.class,
                (ignored, method, arguments) -> switch (method.getName()) {
                    case "isAutoCommit" -> true;
                    case "beginTransaction" -> {
                        recorder.begins++;
                        yield Mono.empty();
                    }
                    case "commitTransaction" -> {
                        recorder.commits++;
                        yield Mono.empty();
                    }
                    case "rollbackTransaction" -> {
                        recorder.rollbacks++;
                        yield Mono.empty();
                    }
                    case "close" -> {
                        recorder.closes++;
                        yield Mono.empty();
                    }
                    case "setAutoCommit" -> Mono.empty();
                    case "createStatement" ->
                            r2dbcStatement((String) arguments[0], recorder);
                    case "toString" -> "generated-key-r2dbc-connection";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Statement r2dbcStatement(String sql, Recorder recorder) {
        if (sql.startsWith("insert into business_row")) {
            return proxy(
                    Statement.class,
                    (ignored, method, arguments) -> switch (method.getName()) {
                        case "bind", "bindNull", "fetchSize", "returnGeneratedValues" -> ignored;
                        case "execute" -> {
                            recorder.businessWrites++;
                            yield Flux.just(r2dbcGeneratedKey(recorder.businessWrites));
                        }
                        case "toString" -> sql;
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
        }
        AtomicInteger adds = new AtomicInteger();
        List<Long> owners = new ArrayList<>();
        long[] currentOwner = new long[1];
        return proxy(
                Statement.class,
                (ignored, method, arguments) -> switch (method.getName()) {
                    case "bind" -> {
                        if ((Integer) arguments[0] == 0) currentOwner[0] = ((Number) arguments[1]).longValue();
                        yield ignored;
                    }
                    case "bindNull", "fetchSize" -> ignored;
                    case "add" -> {
                        adds.incrementAndGet();
                        owners.add(currentOwner[0]);
                        yield ignored;
                    }
                    case "execute" -> {
                        int parameterSets = adds.get() + 1;
                        recorder.tokenExecutions++;
                        recorder.tokenParameterSets += parameterSets;
                        recorder.tokenBatchSizes.add(parameterSets);
                        owners.add(currentOwner[0]);
                        recorder.tokenOwners.addAll(owners);
                        yield Flux.just(rowsUpdated(
                                recorder.invalidTokenCounts ? 0 : parameterSets));
                    }
                    case "toString" -> sql;
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    @SuppressWarnings("unchecked")
    private static Result r2dbcGeneratedKey(long value) {
        ColumnMetadata column = new ColumnMetadata() {
            @Override
            public R2dbcType getType() {
                return R2dbcType.BIGINT;
            }

            @Override
            public String getName() {
                return "id";
            }
        };
        RowMetadata metadata = new RowMetadata() {
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
                return type.cast(value);
            }

            @Override
            public <T> T get(String name, Class<T> type) {
                return type.cast(value);
            }
        };
        Result.RowSegment segment = () -> row;
        return proxy(
                Result.class,
                (ignored, method, arguments) -> switch (method.getName()) {
                    case "flatMap" -> {
                        Function<Result.Segment, Publisher<?>> mapper =
                                (Function<Result.Segment, Publisher<?>>) arguments[0];
                        yield Flux.from(mapper.apply(segment));
                    }
                    case "toString" -> "generated-key-result";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Result rowsUpdated(long count) {
        return proxy(
                Result.class,
                (ignored, method, arguments) -> switch (method.getName()) {
                    case "getRowsUpdated" -> Mono.just(count);
                    case "toString" -> "rows-updated";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(
                type.getClassLoader(), new Class<?>[]{type}, handler));
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == char.class) {
            return '\0';
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0F;
        }
        if (type == double.class) {
            return 0D;
        }
        return 0;
    }

    private record Key(long offset, long value) {
    }

    private static final class Recorder {
        int businessWrites;
        long currentKey;
        int tokenExecutions;
        int tokenParameterSets;
        int begins;
        int commits;
        int rollbacks;
        int closes;
        boolean invalidTokenCounts;
        final List<Integer> tokenBatchSizes = new ArrayList<>();
        final List<Long> tokenOwners = new ArrayList<>();
    }
}
