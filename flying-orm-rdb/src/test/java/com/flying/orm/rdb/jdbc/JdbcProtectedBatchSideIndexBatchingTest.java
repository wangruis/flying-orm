package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;

import java.lang.reflect.Proxy;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcProtectedBatchSideIndexBatchingTest {

    private static final int ROW_COUNT = 8;

    @Test
    void compositeOwnerTokenBatchesRespectParameterAndOperationLimits() throws Exception {
        StatementRecorder recorder = new StatementRecorder();
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into business_row(a, b, c) values (?, ?, ?)", List.of(7L, 8L, 9L)),
                null, List.of("a", "b", "c"), Map.of("a", 7L, "b", 8L, "c", 9L),
                "a = ? and b = ? and c = ?",
                "delete from token_index where a = ? and b = ? and c = ? and field_tag = ?",
                "insert into token_index(a, b, c, field_tag, token) values (?, ?, ?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("phone",
                        java.util.stream.IntStream.range(0, 501)
                                .mapToObj(index -> ByteBuffer.allocate(4).putInt(0, index).array()).toList())));

        new JdbcProtectedBatchSideIndex().complete(recorder.connection(),
                new JdbcProtectedBatchSideIndex.Prepared(List.of(
                        new JdbcProtectedBatchSideIndex.RowState(work, List.of()))));

        assertTrue(recorder.executions.stream().allMatch(parameters -> parameters.size() <= 500
                && parameters.stream().mapToInt(List::size).sum() <= 2_000));
        assertEquals(java.util.stream.IntStream.range(0, 501)
                        .mapToObj(index -> List.of(7L, 8L, 9L, "phone", ByteBuffer.allocate(4).putInt(0, index)))
                        .toList(),
                recorder.executions.stream().flatMap(List::stream).toList());
    }


    @Test
    void ordinaryChunkDoesNotPreparePerRowProtectionState() throws Exception {
        List<ProtectedBatchRows.RowView> rows = new ArrayList<>();
        for (int index = 0; index < 500; index++) {
            rows.add(ProtectedBatchRows.decode(new Object[]{"plain", (long) index}, 2));
        }
        JdbcProtectedBatchSideIndex.Prepared prepared = new JdbcProtectedBatchSideIndex().prepare(
                null, request(), rows, null);

        assertEquals(List.of(), prepared.rows());
    }

    @Test
    void ownerPreReadsUseOneBoundedStatementForTheChunk() throws Exception {
        StatementRecorder recorder = new StatementRecorder();
        List<ProtectedBatchRows.RowView> rows = new ArrayList<>();
        for (int index = 0; index < ROW_COUNT; index++) {
            rows.add(row(updateWork(1_000L + index)));
        }

        new JdbcProtectedBatchSideIndex().prepare(
                recorder.connection(), request(), rows, null);

        assertEquals(1, recorder.preparedStatements.get(),
                     "owner pre-read Statements must be bounded by the chunk, not multiplied by rows");
    }

    @Test
    void sideIndexDeletesUseOneBoundedStatementForTheChunk() throws Exception {
        StatementRecorder recorder = new StatementRecorder();
        List<JdbcProtectedBatchSideIndex.RowState> states = new ArrayList<>();
        for (int index = 0; index < ROW_COUNT; index++) {
            states.add(new JdbcProtectedBatchSideIndex.RowState(
                    upsertWork(1_000L + index), List.of()));
        }

        new JdbcProtectedBatchSideIndex().complete(
                recorder.connection(), new JdbcProtectedBatchSideIndex.Prepared(states));

        assertEquals(1, recorder.preparedStatements.get(),
                     "side-index DELETE Statements must be bounded by the chunk, not multiplied by owner fields");
    }

    @Test
    void decimalOwnerScaleDoesNotReorderReplacementStatements() throws Exception {
        StatementRecorder recorder = new StatementRecorder();
        var fields = List.of(new ProtectedWriteWork.FieldTokens("phone", List.of(new byte[]{1})));
        var states = List.of(
                new JdbcProtectedBatchSideIndex.RowState(work(
                        ProtectedWriteWork.Kind.UPSERT, new BigDecimal("1.0"), null, fields), List.of()),
                new JdbcProtectedBatchSideIndex.RowState(work(
                        ProtectedWriteWork.Kind.UPSERT, new BigDecimal("1.00"), null, fields), List.of()));

        new JdbcProtectedBatchSideIndex().complete(recorder.connection(),
                new JdbcProtectedBatchSideIndex.Prepared(states));

        assertEquals(List.of("DELETE:1", "INSERT:1", "DELETE:1", "INSERT:1"),
                     recorder.executionShapes);
    }

    @TestFactory
    List<DynamicTest> numericOwnerIdentityControlsReplacementSegmentation() {
        // Floating-point equality is limited to exact integers/zero, not database rounding rules.
        record Owners(String name, Number first, Number second, boolean same) { }
        return List.of(
                new Owners("Integer-Long", 7, 7L, true),
                new Owners("BigInteger-Long", BigInteger.valueOf(7), 7L, true),
                new Owners("BigDecimal-Long", new BigDecimal("7.00"), 7L, true),
                new Owners("Float-Double-exact-integer", 7.0F, 7.0D, true),
                new Owners("negative-double-zero-Long", -0.0D, 0L, true),
                new Owners("negative-float-zero-Double", -0.0F, 0.0D, true),
                new Owners("different-integral-values", 7, 8L, false),
                new Owners("different-decimal-values", new BigDecimal("7.01"), 7L, false),
                new Owners("different-longs-above-double-precision", 9_007_199_254_740_992L,
                        9_007_199_254_740_993L, false),
                new Owners("opposite-infinities", Float.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY, false),
                new Owners("NaN-and-finite", Double.NaN, 7L, false))
                .stream().map(owners -> DynamicTest.dynamicTest(owners.name(), () -> {
                    StatementRecorder recorder = new StatementRecorder();
                    var firstFields = List.of(new ProtectedWriteWork.FieldTokens("phone", List.of(new byte[]{1})));
                    var secondFields = List.of(new ProtectedWriteWork.FieldTokens("phone", List.of(new byte[]{2})));
                    var states = List.of(
                            new JdbcProtectedBatchSideIndex.RowState(work(ProtectedWriteWork.Kind.UPSERT,
                                    owners.first(), null, firstFields), List.of()),
                            new JdbcProtectedBatchSideIndex.RowState(work(ProtectedWriteWork.Kind.UPSERT,
                                    owners.second(), null, secondFields), List.of()));

                    new JdbcProtectedBatchSideIndex().complete(recorder.connection(),
                            new JdbcProtectedBatchSideIndex.Prepared(states));

                    assertEquals(owners.same()
                                    ? List.of("DELETE:1", "INSERT:1", "DELETE:1", "INSERT:1")
                                    : List.of("DELETE:2", "INSERT:2"), recorder.executionShapes);
                    List<List<Object>> inserts = new ArrayList<>();
                    List<List<Object>> deletes = new ArrayList<>();
                    for (int index = 0; index < recorder.executions.size(); index++) {
                        (recorder.executionShapes.get(index).startsWith("INSERT:") ? inserts : deletes)
                                .addAll(recorder.executions.get(index));
                    }
                    assertEquals(List.of(List.of(owners.first(), "phone"), List.of(owners.second(), "phone")),
                            deletes, "identity normalization must not rewrite owner bindings");
                    assertEquals(List.of(List.of(owners.first(), "phone", ByteBuffer.wrap(new byte[]{1})),
                            List.of(owners.second(), "phone", ByteBuffer.wrap(new byte[]{2}))), inserts);
                })).toList();
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
                                             Object ownerId,
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

    private static final class StatementRecorder {

        private final AtomicInteger preparedStatements = new AtomicInteger();
        private final List<String> executionShapes = new ArrayList<>();
        private final List<List<List<Object>>> executions = new ArrayList<>();

        private Connection connection() {
            return (Connection) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{Connection.class},
                    (proxy, method, arguments) -> switch (method.getName()) {
                        case "prepareStatement" -> statement((String) arguments[0]);
                        case "toString" -> "protected-batch-counting-connection";
                        default -> defaultValue(method.getReturnType());
                    });
        }

        private PreparedStatement statement(String sql) {
            preparedStatements.incrementAndGet();
            AtomicInteger batchSize = new AtomicInteger();
            Map<Integer, Object> bindings = new TreeMap<>();
            List<List<Object>> parameters = new ArrayList<>();
            PreparedStatement[] statement = new PreparedStatement[1];
            statement[0] = (PreparedStatement) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{PreparedStatement.class},
                    (proxy, method, arguments) -> switch (method.getName()) {
                        case "executeQuery" -> emptyResultSet();
                        case "executeUpdate" -> 1;
                        case "addBatch" -> {
                            batchSize.incrementAndGet();
                            parameters.add(List.copyOf(bindings.values()));
                            yield null;
                        }
                        case "executeBatch" -> {
                            executions.add(List.copyOf(parameters));
                            executionShapes.add((sql.startsWith("delete") ? "DELETE:" : "INSERT:")
                                    + batchSize.get());
                            int[] counts = new int[batchSize.get()];
                            java.util.Arrays.fill(counts, 1);
                            yield counts;
                        }
                        case "setObject" -> {
                            bindings.put((Integer) arguments[0], arguments[1]);
                            yield null;
                        }
                        case "setBinaryStream" -> {
                            bindings.put((Integer) arguments[0], ByteBuffer.wrap(
                                    ((java.io.InputStream) arguments[1]).readAllBytes()));
                            yield null;
                        }
                        case "setNull", "setQueryTimeout", "close" -> null;
                        case "toString" -> "protected-batch-counting-statement";
                        default -> defaultValue(method.getReturnType());
                    });
            return statement[0];
        }

        private ResultSet emptyResultSet() {
            ResultSetMetaData metadata = (ResultSetMetaData) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{ResultSetMetaData.class},
                    (proxy, method, arguments) -> switch (method.getName()) {
                        case "getColumnCount" -> 1;
                        case "getColumnLabel", "getColumnName" -> "id";
                        case "toString" -> "owner-result-metadata";
                        default -> defaultValue(method.getReturnType());
                    });
            return (ResultSet) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{ResultSet.class},
                    (proxy, method, arguments) -> switch (method.getName()) {
                        case "getMetaData" -> metadata;
                        case "next" -> false;
                        case "close" -> null;
                        case "toString" -> "empty-owner-result";
                        default -> defaultValue(method.getReturnType());
                    });
        }
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
        if (type == byte.class) {
            return (byte) 0;
        }
        if (type == short.class) {
            return (short) 0;
        }
        if (type == int.class) {
            return 0;
        }
        if (type == long.class) {
            return 0L;
        }
        if (type == float.class) {
            return 0F;
        }
        return 0D;
    }
}
