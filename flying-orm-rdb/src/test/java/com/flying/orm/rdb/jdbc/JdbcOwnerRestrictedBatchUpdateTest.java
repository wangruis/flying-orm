package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JdbcOwnerRestrictedBatchUpdateTest {

    private static final String UPDATE_SQL =
            "update business_row set value_col = ? where id = ?";

    @Test
    void batchesConsecutiveOwnerRestrictedUpdatesWithTheSameSqlShape() throws Exception {
        BatchRecorder recorder = new BatchRecorder(null);
        try (Connection database = database()) {
            BatchChunkResult result = execute(database, recorder, List.of(
                    work(1L, "first"), work(2L, "second")));

            assertEquals(BatchChunkResult.Status.COMMITTED, result.status());
            assertEquals(2L, result.affectedRows());
            assertEquals(1, recorder.prepares.get());
            assertEquals(2, recorder.addBatches.get());
            assertEquals(1, recorder.executeBatches.get());
            assertEquals(0, recorder.executeUpdates.get());
            assertEquals(List.of("first", "second"), values(database));
        }
    }

    @Test
    void rejectsInvalidDriverCountsForOwnerRestrictedBatches() throws Exception {
        for (int[] invalidCounts : List.of(
                new int[]{1},
                new int[]{1, Statement.EXECUTE_FAILED},
                new int[]{1, Statement.SUCCESS_NO_INFO})) {
            try (Connection database = database()) {
                BatchRecorder recorder = new BatchRecorder(invalidCounts);

                assertThrows(SQLException.class, () -> execute(database, recorder, List.of(
                        work(1L, "first"), work(2L, "second"))));
                assertEquals(1, recorder.executeBatches.get());
            }
        }
    }

    @Test
    void mapsDriverCountsBackToTheOriginalInputOffsets() throws Exception {
        BatchRecorder recorder = new BatchRecorder(new int[]{1, 0});
        try (Connection database = database()) {
            BatchChunkResult result = execute(database, recorder, List.of(
                    work(1L, "first"), work(2L, "second")));

            assertEquals(BatchChunkResult.Status.CONFLICTED, result.status());
            assertEquals(1, result.conflicts().size());
            assertEquals(1L, result.conflicts().getFirst().inputOffset());
        }
    }

    @Test
    void doesNotMergeRequestsAcrossAnEmptyOwner() throws Exception {
        BatchRecorder recorder = new BatchRecorder(null);
        try (Connection database = database(); Statement statement = database.createStatement()) {
            statement.executeUpdate("delete from business_row where id = 2");
            statement.executeUpdate("insert into business_row(id, value_col) values (3, 'old-3')");

            BatchChunkResult result = execute(database, recorder, List.of(
                    work(1L, "first"), work(2L, "missing"), work(3L, "third")));

            assertEquals(BatchChunkResult.Status.CONFLICTED, result.status());
            assertEquals(1L, result.conflicts().getFirst().inputOffset());
            assertEquals(2, recorder.prepares.get());
            assertEquals(2, recorder.addBatches.get());
            assertEquals(2, recorder.executeBatches.get());
        }
    }

    @Test
    void mapsLaterSqlShapeBatchFailureToItsOriginalInputOffset() throws Exception {
        BatchUpdateException partial = new BatchUpdateException(
                "later owner-restricted shape failed", "23000", 0,
                new int[]{Statement.EXECUTE_FAILED});
        BatchRecorder recorder = new BatchRecorder(2, partial);
        try (Connection database = database()) {
            JdbcBatchEvidenceSupport.Outcome outcome = assertDoesNotThrow(() -> executeEvidence(
                    database, recorder, List.of(
                            work(1L, "first"),
                            work(2L, "second", UPDATE_SQL + " and 1 = 1"))));

            assertEquals(BatchExecutionState.PARTIAL, outcome.fact().state());
            assertEquals(List.of(0L), outcome.fact().successfulOffsets());
            assertEquals(List.of(1L), outcome.fact().failedOffsets());
            assertEquals(2, recorder.executeBatches.get());
        }
    }

    private static BatchChunkResult execute(Connection database,
                                            BatchRecorder recorder,
                                            List<ProtectedWriteWork> workItems) throws Exception {
        List<ProtectedBatchRows.RowView> rows = workItems.stream()
                .map(JdbcOwnerRestrictedBatchUpdateTest::row)
                .toList();
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                UPDATE_SQL,
                2,
                List.of(String.class, Long.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.empty(),
                BatchWriteOptions.atomic(workItems.size()),
                BatchRowCountPolicy.EXACTLY_ONE);

        return new JdbcBatchChunkExecutor().execute(
                recorder.connection(database), request, 0, 0L, rows,
                JdbcBatchSupport.BatchDeadline.start(Duration.ofSeconds(5)));
    }

    private static JdbcBatchEvidenceSupport.Outcome executeEvidence(
            Connection database,
            BatchRecorder recorder,
            List<ProtectedWriteWork> workItems) {
        List<ProtectedBatchRows.RowView> rows = workItems.stream()
                .map(JdbcOwnerRestrictedBatchUpdateTest::row)
                .toList();
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                UPDATE_SQL,
                2,
                List.of(String.class, Long.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.empty(),
                BatchWriteOptions.atomic(workItems.size()),
                BatchRowCountPolicy.EXACTLY_ONE);
        return new JdbcBatchChunkExecutor().executeBatchEvidence(
                recorder.connection(database), request, 0, 0L, rows,
                JdbcBatchSupport.BatchDeadline.start(Duration.ofSeconds(5)));
    }

    private static ProtectedBatchRows.RowView row(ProtectedWriteWork work) {
        Object[] parameters = work.writeRequest().parameters().toArray();
        return ProtectedBatchRows.decode(ProtectedBatchRows.extend(parameters, work), parameters.length);
    }

    private static ProtectedWriteWork work(long id, String value) {
        return work(id, value, UPDATE_SQL);
    }

    private static ProtectedWriteWork work(long id, String value, String updateSql) {
        return new ProtectedWriteWork(
                ProtectedWriteWork.Kind.UPDATE,
                new SqlRequest(updateSql, List.of(value, id)),
                new SqlRequest("select id from business_row where id = ?", List.of(id)),
                List.of("id"),
                Map.of(),
                "id = ?",
                "delete from token_index where id = ? and field_tag = ?",
                "insert into token_index(id, field_tag, token) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("phone", List.of())));
    }

    private static Connection database() throws SQLException {
        Connection connection = DriverManager.getConnection(
                "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1");
        try (Statement statement = connection.createStatement()) {
            statement.execute("create table business_row(id bigint primary key, value_col varchar(40))");
            statement.execute("create table token_index(id bigint, field_tag varchar(40), token varbinary)");
            statement.execute("insert into business_row(id, value_col) values (1, 'old-1'), (2, 'old-2')");
        }
        connection.setAutoCommit(false);
        return connection;
    }

    private static List<String> values(Connection connection) throws SQLException {
        try (PreparedStatement statement = connection.prepareStatement(
                "select value_col from business_row order by id");
             var resultSet = statement.executeQuery()) {
            java.util.ArrayList<String> values = new java.util.ArrayList<>(2);
            while (resultSet.next()) {
                values.add(resultSet.getString(1));
            }
            return List.copyOf(values);
        }
    }

    private static Object invoke(Object target, Method method, Object[] arguments) throws Throwable {
        try {
            return method.invoke(target, arguments);
        } catch (InvocationTargetException error) {
            throw error.getCause();
        }
    }

    private static final class BatchRecorder {

        private final int[] forcedCounts;
        private final int failureExecution;
        private final BatchUpdateException failure;
        private final AtomicInteger prepares = new AtomicInteger();
        private final AtomicInteger addBatches = new AtomicInteger();
        private final AtomicInteger executeBatches = new AtomicInteger();
        private final AtomicInteger executeUpdates = new AtomicInteger();

        private BatchRecorder(int[] forcedCounts) {
            this(forcedCounts, -1, null);
        }

        private BatchRecorder(int failureExecution, BatchUpdateException failure) {
            this(null, failureExecution, failure);
        }

        private BatchRecorder(int[] forcedCounts,
                              int failureExecution,
                              BatchUpdateException failure) {
            this.forcedCounts = forcedCounts;
            this.failureExecution = failureExecution;
            this.failure = failure;
        }

        private Connection connection(Connection delegate) {
            return (Connection) Proxy.newProxyInstance(
                    JdbcOwnerRestrictedBatchUpdateTest.class.getClassLoader(),
                    new Class<?>[]{Connection.class},
                    (proxy, method, arguments) -> {
                        Object result = invoke(delegate, method, arguments);
                        if ("prepareStatement".equals(method.getName())
                                && arguments != null
                                && arguments.length > 0
                                && arguments[0] instanceof String sql
                                && sql.startsWith(UPDATE_SQL + " and ")) {
                            prepares.incrementAndGet();
                            return statement((PreparedStatement) result);
                        }
                        return result;
                    });
        }

        private PreparedStatement statement(PreparedStatement delegate) {
            return (PreparedStatement) Proxy.newProxyInstance(
                    JdbcOwnerRestrictedBatchUpdateTest.class.getClassLoader(),
                    new Class<?>[]{PreparedStatement.class},
                    (proxy, method, arguments) -> switch (method.getName()) {
                        case "addBatch" -> {
                            addBatches.incrementAndGet();
                            yield invoke(delegate, method, arguments);
                        }
                        case "executeBatch" -> {
                            int execution = executeBatches.incrementAndGet();
                            if (execution == failureExecution) {
                                throw failure;
                            }
                            int[] actual = (int[]) invoke(delegate, method, arguments);
                            yield forcedCounts == null ? actual : forcedCounts.clone();
                        }
                        case "executeLargeUpdate", "executeUpdate" -> {
                            executeUpdates.incrementAndGet();
                            yield invoke(delegate, method, arguments);
                        }
                        default -> invoke(delegate, method, arguments);
                    });
        }
    }
}
