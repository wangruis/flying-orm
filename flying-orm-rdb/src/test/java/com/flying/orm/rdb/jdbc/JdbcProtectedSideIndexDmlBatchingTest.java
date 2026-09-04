package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import org.junit.jupiter.api.Test;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JdbcProtectedSideIndexDmlBatchingTest {

    @Test
    void singleProtectedWriteBatchesAllTokensForOneOwnerField() {
        SideIndexRecorder recorder = new SideIndexRecorder(3);
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource(connection(recorder, true)));

        executor.atomicProtectedWrite(work(), SqlExecutionOptions.safeDefaults());

        recorder.assertOneBoundedTokenBatch();
    }

    @Test
    void rollsBackOwnedProtectedWriteInterruptedDuringTokenBatchExecution() {
        AtomicInteger commits = new AtomicInteger();
        AtomicInteger rollbacks = new AtomicInteger();
        SideIndexRecorder recorder = new SideIndexRecorder(3, 1, true);
        JdbcSqlExecutor executor = JdbcSqlExecutor.create(dataSource(
                connection(recorder, true, commits, rollbacks)));

        try {
            RdbException failure = assertThrows(RdbException.class,
                    () -> executor.atomicProtectedWrite(work(), SqlExecutionOptions.safeDefaults()));

            assertEquals(RdbErrorKind.CANCELLED, failure.kind());
            assertEquals("HY008", failure.sqlState());
            assertEquals(1, recorder.executeBatches.get());
            assertEquals(0, commits.get());
            assertEquals(1, rollbacks.get());
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void protectedBatchCompletionBatchesAllTokensForOneOwnerField() throws Exception {
        SideIndexRecorder recorder = new SideIndexRecorder(6);
        JdbcProtectedBatchSideIndex.Prepared prepared = new JdbcProtectedBatchSideIndex.Prepared(
                List.of(new JdbcProtectedBatchSideIndex.RowState(work(7L), List.of()),
                        new JdbcProtectedBatchSideIndex.RowState(work(8L), List.of())));

        new JdbcProtectedBatchSideIndex().complete(
                connection(recorder, false), prepared, BatchChunkResult.committed(0, 0, 1, 1L),
                JdbcBatchSupport.BatchDeadline.start(Duration.ofSeconds(5)));

        recorder.assertOneBoundedTokenBatch(List.of(7L, 7L, 7L, 8L, 8L, 8L));
    }

    @Test
    void jdbcSuccessNoInfoIsAcceptedOnlyForGeneratedSingleRowTokenInsert() throws Exception {
        SideIndexRecorder recorder = new SideIndexRecorder(3, Statement.SUCCESS_NO_INFO);
        ProtectedWriteWork work = work();
        JdbcProtectedBatchSideIndex.Prepared prepared = new JdbcProtectedBatchSideIndex.Prepared(
                List.of(new JdbcProtectedBatchSideIndex.RowState(work, List.of())));

        new JdbcProtectedBatchSideIndex().complete(
                connection(recorder, false), prepared, BatchChunkResult.committed(0, 0, 1, 1L),
                JdbcBatchSupport.BatchDeadline.start(Duration.ofSeconds(5)));

        recorder.assertOneBoundedTokenBatch();
    }

    @Test
    void jdbcExecuteFailedCannotSatisfyPerTokenExactlyOneContract() {
        SideIndexRecorder recorder = new SideIndexRecorder(3, Statement.EXECUTE_FAILED);
        ProtectedWriteWork work = work();
        JdbcProtectedBatchSideIndex.Prepared prepared = new JdbcProtectedBatchSideIndex.Prepared(
                List.of(new JdbcProtectedBatchSideIndex.RowState(work, List.of())));

        assertThrows(IllegalStateException.class, () -> new JdbcProtectedBatchSideIndex().complete(
                connection(recorder, false), prepared, BatchChunkResult.committed(0, 0, 1, 1L),
                JdbcBatchSupport.BatchDeadline.start(Duration.ofSeconds(5))));
    }

    @Test
    void jdbcZeroOrMultipleRowsCannotSatisfyTokenInsertContract() {
        for (int invalid : List.of(0, 2)) {
            SideIndexRecorder recorder = new SideIndexRecorder(3, invalid);
            ProtectedWriteWork work = work();
            JdbcProtectedBatchSideIndex.Prepared prepared = new JdbcProtectedBatchSideIndex.Prepared(
                    List.of(new JdbcProtectedBatchSideIndex.RowState(work, List.of())));

            assertThrows(IllegalStateException.class, () -> new JdbcProtectedBatchSideIndex().complete(
                    connection(recorder, false), prepared, BatchChunkResult.committed(0, 0, 1, 1L),
                    JdbcBatchSupport.BatchDeadline.start(Duration.ofSeconds(5))));
        }
    }

    @Test
    void jdbcTokenBatchIsSplitAtTheFixedInternalLimit() throws Exception {
        int tokenCount = JdbcProtectedSideIndexDml.MAX_TOKEN_BATCH_SIZE + 1;
        SideIndexRecorder recorder = new SideIndexRecorder(tokenCount);
        ProtectedWriteWork work = work(7L, tokenCount);
        JdbcProtectedBatchSideIndex.Prepared prepared = new JdbcProtectedBatchSideIndex.Prepared(
                List.of(new JdbcProtectedBatchSideIndex.RowState(work, List.of())));

        new JdbcProtectedBatchSideIndex().complete(
                connection(recorder, false), prepared, BatchChunkResult.committed(0, 0, 1, 1L),
                JdbcBatchSupport.BatchDeadline.start(Duration.ofSeconds(5)));

        assertEquals(2, recorder.prepares.get());
        assertEquals(2, recorder.executeBatches.get());
        assertEquals(tokenCount, recorder.addBatches.get());
    }

    @Test
    void interruptionAfterFinalBindingSkipsTokenBatchExecution() {
        AtomicInteger executions = new AtomicInteger();
        PreparedStatement statement = (PreparedStatement) Proxy.newProxyInstance(
                JdbcProtectedSideIndexDmlBatchingTest.class.getClassLoader(),
                new Class<?>[]{PreparedStatement.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "addBatch" -> {
                        Thread.currentThread().interrupt();
                        yield null;
                    }
                    case "executeBatch" -> {
                        executions.incrementAndGet();
                        yield new int[]{1};
                    }
                    case "cancel", "close", "setObject", "setQueryTimeout" -> null;
                    case "isClosed" -> false;
                    default -> defaultValue(method.getReturnType());
                });
        Connection connection = (Connection) Proxy.newProxyInstance(
                JdbcProtectedSideIndexDmlBatchingTest.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> "prepareStatement".equals(method.getName())
                        ? statement : defaultValue(method.getReturnType()));

        try {
            SQLException failure = assertThrows(SQLException.class, () ->
                    JdbcProtectedSideIndexDml.insertParameterSets(
                            connection,
                            "insert into token_index(id, field_tag, token) values (?, ?, ?)",
                            List.of(List.of(7L, "phone", "token")),
                            JdbcBatchSupport.BatchDeadline.start(Duration.ofSeconds(5))));

            assertEquals("HY008", failure.getSQLState());
            assertEquals(0, executions.get());
        } finally {
            Thread.interrupted();
        }
    }

    private static ProtectedWriteWork work() {
        return work(7L);
    }

    private static ProtectedWriteWork work(long ownerId) {
        return work(ownerId, 3);
    }

    private static ProtectedWriteWork work(long ownerId, int tokenCount) {
        List<byte[]> tokens = java.util.stream.IntStream.range(0, tokenCount)
                .mapToObj(index -> new byte[]{(byte) (index + 1)})
                .toList();
        return new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into business_row(id, value_col) values (?, ?)",
                               List.of(ownerId, "value")),
                null,
                List.of("id"),
                Map.of("id", ownerId),
                "id = ?",
                "delete from token_index where id = ? and field_tag = ?",
                "insert into token_index(id, field_tag, token) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("phone", tokens)));
    }

    private static DataSource dataSource(Connection connection) {
        return (DataSource) Proxy.newProxyInstance(
                JdbcProtectedSideIndexDmlBatchingTest.class.getClassLoader(),
                new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> "getConnection".equals(method.getName())
                        ? connection : defaultValue(method.getReturnType()));
    }

    private static Connection connection(SideIndexRecorder recorder, boolean includeBusinessWrite) {
        return connection(recorder, includeBusinessWrite, new AtomicInteger(), new AtomicInteger());
    }

    private static Connection connection(SideIndexRecorder recorder,
                                         boolean includeBusinessWrite,
                                         AtomicInteger commits,
                                         AtomicInteger rollbacks) {
        return (Connection) Proxy.newProxyInstance(
                JdbcProtectedSideIndexDmlBatchingTest.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getAutoCommit" -> true;
                    case "prepareStatement" -> {
                        String sql = (String) arguments[0];
                        yield sql.contains("token_index")
                                ? recorder.newStatement() : businessStatement(includeBusinessWrite);
                    }
                    case "commit" -> commits.incrementAndGet();
                    case "rollback" -> rollbacks.incrementAndGet();
                    case "setAutoCommit", "close" -> null;
                    case "isClosed" -> false;
                    case "toString" -> "protected-side-index-connection";
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static PreparedStatement businessStatement(boolean allowed) {
        return (PreparedStatement) Proxy.newProxyInstance(
                JdbcProtectedSideIndexDmlBatchingTest.class.getClassLoader(),
                new Class<?>[]{PreparedStatement.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "executeLargeUpdate" -> allowed ? 1L : throwUnexpectedBusinessWrite();
                    case "executeUpdate" -> allowed ? 1 : throwUnexpectedBusinessWrite();
                    case "close", "setObject", "setQueryTimeout" -> null;
                    case "isClosed" -> false;
                    default -> defaultValue(method.getReturnType());
                });
    }

    private static int throwUnexpectedBusinessWrite() {
        throw new AssertionError("batch side-index completion must not execute business DML");
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
        return 0;
    }

    private static final class SideIndexRecorder {

        private final int tokenCount;
        private final int batchResult;
        private final boolean interruptAfterExecute;
        private final AtomicInteger prepares = new AtomicInteger();
        private final AtomicInteger executeUpdates = new AtomicInteger();
        private final AtomicInteger executeBatches = new AtomicInteger();
        private final AtomicInteger addBatches = new AtomicInteger();
        private final AtomicInteger binaryStreamBinds = new AtomicInteger();
        private final List<List<Object>> parameterSets = new ArrayList<>();
        private final List<Object> current = new ArrayList<>(Arrays.asList(null, null, null));

        private SideIndexRecorder(int tokenCount) {
            this(tokenCount, 1);
        }

        private SideIndexRecorder(int tokenCount, int batchResult) {
            this(tokenCount, batchResult, false);
        }

        private SideIndexRecorder(int tokenCount, int batchResult, boolean interruptAfterExecute) {
            this.tokenCount = tokenCount;
            this.batchResult = batchResult;
            this.interruptAfterExecute = interruptAfterExecute;
        }

        private PreparedStatement newStatement() {
            prepares.incrementAndGet();
            AtomicInteger statementAdds = new AtomicInteger();
            return (PreparedStatement) Proxy.newProxyInstance(
                    JdbcProtectedSideIndexDmlBatchingTest.class.getClassLoader(),
                    new Class<?>[]{PreparedStatement.class},
                    (proxy, method, arguments) -> switch (method.getName()) {
                        case "setObject" -> {
                            current.set((Integer) arguments[0] - 1, arguments[1]);
                            yield null;
                        }
                        case "setBinaryStream" -> {
                            binaryStreamBinds.incrementAndGet();
                            try {
                                current.set((Integer) arguments[0] - 1,
                                            ((InputStream) arguments[1]).readAllBytes());
                            } catch (IOException failure) {
                                throw new IllegalStateException(failure);
                            }
                            yield null;
                        }
                        case "addBatch" -> {
                            addBatches.incrementAndGet();
                            statementAdds.incrementAndGet();
                            parameterSets.add(List.copyOf(current));
                            yield null;
                        }
                        case "executeBatch" -> {
                            executeBatches.incrementAndGet();
                            if (interruptAfterExecute) {
                                Thread.currentThread().interrupt();
                            }
                            yield java.util.stream.IntStream.range(0, statementAdds.get())
                                    .map(ignored -> batchResult).toArray();
                        }
                        case "executeUpdate" -> {
                            executeUpdates.incrementAndGet();
                            yield 1;
                        }
                        case "close", "setQueryTimeout" -> null;
                        case "isClosed" -> false;
                        default -> defaultValue(method.getReturnType());
                    });
        }

        private void assertOneBoundedTokenBatch() {
            assertOneBoundedTokenBatch(List.of(7L, 7L, 7L));
        }

        private void assertOneBoundedTokenBatch(List<Long> expectedOwners) {
            assertEquals(1, prepares.get(), "one owner/field must prepare one INSERT statement");
            assertEquals(0, executeUpdates.get(), "tokens must not execute one-by-one");
            assertEquals(1, executeBatches.get(), "one owner/field must execute one JDBC batch");
            assertEquals(tokenCount, addBatches.get());
            assertEquals(tokenCount, binaryStreamBinds.get(),
                         "owned tokens must bind through read-only binary views, not copied byte[] values");
            assertEquals(tokenCount, parameterSets.size());
            for (int index = 0; index < tokenCount; index++) {
                List<Object> parameters = parameterSets.get(index);
                assertEquals(expectedOwners.get(index), parameters.get(0));
                assertEquals("phone", parameters.get(1));
                assertArrayEquals(new byte[]{(byte) (index % 3 + 1)}, (byte[]) parameters.get(2));
            }
        }
    }
}
