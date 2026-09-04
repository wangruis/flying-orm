package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class JdbcProtectedBatchSideIndexBatchingTest {

    private static final int ROW_COUNT = 8;

    @Test
    void ordinaryChunkDoesNotPreparePerRowProtectionState() throws Exception {
        List<ProtectedBatchRows.RowView> rows = new ArrayList<>();
        for (int index = 0; index < 500; index++) {
            rows.add(ProtectedBatchRows.decode(new Object[]{"plain", (long) index}, 2));
        }
        JdbcProtectedBatchSideIndex.Prepared prepared = new JdbcProtectedBatchSideIndex().prepare(
                null, request(), rows, JdbcBatchSupport.BatchDeadline.start(Duration.ZERO), null);

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
                recorder.connection(), request(), rows,
                JdbcBatchSupport.BatchDeadline.start(Duration.ZERO), null);

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
                recorder.connection(), new JdbcProtectedBatchSideIndex.Prepared(states),
                BatchChunkResult.committed(0, 0, ROW_COUNT, ROW_COUNT),
                JdbcBatchSupport.BatchDeadline.start(Duration.ZERO));

        assertEquals(1, recorder.preparedStatements.get(),
                     "side-index DELETE Statements must be bounded by the chunk, not multiplied by owner fields");
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
                List.of(new ProtectedWriteWork.FieldTokens("phone", List.of())));
    }

    private static final class StatementRecorder {

        private final AtomicInteger preparedStatements = new AtomicInteger();

        private Connection connection() {
            return (Connection) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{Connection.class},
                    (proxy, method, arguments) -> switch (method.getName()) {
                        case "prepareStatement" -> statement();
                        case "toString" -> "protected-batch-counting-connection";
                        default -> defaultValue(method.getReturnType());
                    });
        }

        private PreparedStatement statement() {
            preparedStatements.incrementAndGet();
            AtomicInteger batchSize = new AtomicInteger();
            PreparedStatement[] statement = new PreparedStatement[1];
            statement[0] = (PreparedStatement) Proxy.newProxyInstance(
                    getClass().getClassLoader(), new Class<?>[]{PreparedStatement.class},
                    (proxy, method, arguments) -> switch (method.getName()) {
                        case "executeQuery" -> emptyResultSet();
                        case "executeUpdate" -> 1;
                        case "addBatch" -> {
                            batchSize.incrementAndGet();
                            yield null;
                        }
                        case "executeBatch" -> {
                            int[] counts = new int[batchSize.get()];
                            java.util.Arrays.fill(counts, 1);
                            yield counts;
                        }
                        case "setObject", "setNull", "setBinaryStream", "setQueryTimeout", "close" -> null;
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
