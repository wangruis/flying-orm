package com.flying.orm.rdb.execution;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteCompletion;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteRequests;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.jdbc.JdbcBatchWriter;
import com.flying.orm.rdb.reactive.R2dbcSqlExecutor;
import io.r2dbc.spi.ColumnMetadata;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
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

import javax.sql.DataSource;
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

class ProtectedBatchGeneratedKeyBatchingTest {

    private static final int ROWS = 500;

    @TestFactory
    Stream<DynamicTest> batchesGeneratedOwnerTokensOnBothExecutors() {
        return Stream.of(false, true)
                .map(reactive -> DynamicTest.dynamicTest(
                        "batches generated owner tokens [reactive=" + reactive + "]",
                        () -> assertBatchedGeneratedOwners(reactive)));
    }

    @TestFactory
    Stream<DynamicTest> invalidGeneratedOwnerTokenCountsRollbackOnBothExecutors() {
        return Stream.of(false, true)
                .map(reactive -> DynamicTest.dynamicTest(
                        "rolls back invalid generated owner token counts [reactive="
                                + reactive + "]",
                        () -> {
                            Recorder recorder = new Recorder();
                            recorder.invalidTokenCounts = true;
                            List<Key> callbacks = new ArrayList<>();

                            assertThrows(
                                    RuntimeException.class,
                                    () -> execute(
                                            reactive, recorder, request(callbacks)));

                            assertEquals(ROWS, recorder.businessWrites);
                            assertEquals(1, recorder.tokenExecutions);
                            assertEquals(ROWS, recorder.tokenParameterSets);
                            assertEquals(0, recorder.commits);
                            assertEquals(1, recorder.rollbacks);
                            assertEquals(1, recorder.closes);
                            assertEquals(ROWS, callbacks.size());
                        }));
    }

    private static void assertBatchedGeneratedOwners(boolean reactive) {
        Recorder recorder = new Recorder();
        List<Key> callbacks = new ArrayList<>();

        BatchWriteResult result = execute(reactive, recorder, request(callbacks));

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(ROWS, result.inputCount());
        assertEquals(ROWS, result.affectedRows());
        assertEquals(ROWS, recorder.businessWrites);
        assertEquals(1, recorder.tokenExecutions);
        assertEquals(ROWS, recorder.tokenParameterSets);
        assertEquals(1, recorder.commits);
        assertEquals(0, recorder.rollbacks);
        assertEquals(1, recorder.closes);
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
                BatchWriteOptions.atomic(ROWS),
                BatchRowCountPolicy.EXACTLY_ONE,
                BatchGeneratedKeys.required("id", (offset, row) ->
                        callbacks.add(new Key(offset, ((Number) row.value(0)).longValue()))),
                BatchWriteCompletion.noop());
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

    private static BatchWriteResult execute(boolean reactive,
                                            Recorder recorder,
                                            BatchWriteRequest request) {
        if (!reactive) {
            java.sql.Connection connection = jdbcConnection(recorder);
            DataSource dataSource = proxy(
                    DataSource.class,
                    (ignored, method, arguments) -> switch (method.getName()) {
                        case "getConnection" -> connection;
                        default -> defaultValue(method.getReturnType());
                    });
            return JdbcBatchWriter.create(dataSource).writeBatch(request);
        }
        Connection connection = r2dbcConnection(recorder);
        ConnectionFactory factory = new ConnectionFactory() {
            @Override
            public Publisher<? extends Connection> create() {
                return Mono.just(connection);
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "PostgreSQL";
            }
        };
        return R2dbcSqlExecutor.create(factory)
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
        return proxy(
                PreparedStatement.class,
                (ignored, method, arguments) -> switch (method.getName()) {
                    case "addBatch" -> {
                        parameterSets.incrementAndGet();
                        yield null;
                    }
                    case "executeBatch" -> {
                        recorder.tokenExecutions++;
                        recorder.tokenParameterSets += parameterSets.get();
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
        return proxy(
                Statement.class,
                (ignored, method, arguments) -> switch (method.getName()) {
                    case "bind", "bindNull", "fetchSize" -> ignored;
                    case "add" -> {
                        adds.incrementAndGet();
                        yield ignored;
                    }
                    case "execute" -> {
                        int parameterSets = adds.get() + 1;
                        recorder.tokenExecutions++;
                        recorder.tokenParameterSets += parameterSets;
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
    }
}
