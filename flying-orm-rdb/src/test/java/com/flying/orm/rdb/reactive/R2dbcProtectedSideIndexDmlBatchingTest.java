package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
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
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class R2dbcProtectedSideIndexDmlBatchingTest {

    @Test
    void singleProtectedWriteBatchesAllTokensForOneOwnerField() {
        SideIndexRecorder recorder = new SideIndexRecorder(3);
        ConnectionFactory factory = connectionFactory(connection(recorder, true));

        R2dbcSqlExecutor.create(factory)
                .atomicProtectedWrite(work(), SqlExecutionOptions.safeDefaults())
                .block(Duration.ofSeconds(5));

        recorder.assertOneBoundedTokenBatch();
    }

    @Test
    void protectedBatchCompletionBatchesAllTokensForOneOwnerField() {
        SideIndexRecorder recorder = new SideIndexRecorder(6);
        ConnectionFactory factory = connectionFactory(connection(recorder, false));
        R2dbcProtectedBatchSideIndex.Prepared prepared = new R2dbcProtectedBatchSideIndex.Prepared(
                List.of(new R2dbcProtectedBatchSideIndex.RowState(work(7L), List.of()),
                        new R2dbcProtectedBatchSideIndex.RowState(work(8L), List.of())));

        new R2dbcProtectedBatchSideIndex(R2dbcBindMarkers.from(factory))
                .complete(connection(recorder, false), prepared, BatchChunkResult.committed(0, 0, 1, 1L))
                .block(Duration.ofSeconds(5));

        recorder.assertOneBoundedTokenBatch(List.of(7L, 7L, 7L, 8L, 8L, 8L));
    }

    @Test
    void exactAggregatedR2dbcBatchCountIsAccepted() {
        SideIndexRecorder recorder = new SideIndexRecorder(3, true);
        ConnectionFactory factory = connectionFactory(connection(recorder, false));
        ProtectedWriteWork work = work();
        R2dbcProtectedBatchSideIndex.Prepared prepared = new R2dbcProtectedBatchSideIndex.Prepared(
                List.of(new R2dbcProtectedBatchSideIndex.RowState(work, List.of())));

        new R2dbcProtectedBatchSideIndex(R2dbcBindMarkers.from(factory))
                .complete(connection(recorder, false), prepared,
                          BatchChunkResult.committed(0, 0, 1, 1L))
                .block(Duration.ofSeconds(5));

        recorder.assertOneBoundedTokenBatch();
    }

    @Test
    void failedPerTokenR2dbcCountCannotSatisfyExactlyOneContract() {
        SideIndexRecorder recorder = new SideIndexRecorder(3, false, 1);
        ConnectionFactory factory = connectionFactory(connection(recorder, false));
        ProtectedWriteWork work = work();
        R2dbcProtectedBatchSideIndex.Prepared prepared = new R2dbcProtectedBatchSideIndex.Prepared(
                List.of(new R2dbcProtectedBatchSideIndex.RowState(work, List.of())));

        assertThrows(IllegalStateException.class, () ->
                new R2dbcProtectedBatchSideIndex(R2dbcBindMarkers.from(factory))
                        .complete(connection(recorder, false), prepared,
                                  BatchChunkResult.committed(0, 0, 1, 1L))
                        .block(Duration.ofSeconds(5)));
    }

    @Test
    void r2dbcRowsUpdatedErrorFailsTheTokenBatch() {
        SideIndexRecorder recorder = new SideIndexRecorder(3, false, -1, 1);
        ConnectionFactory factory = connectionFactory(connection(recorder, false));
        ProtectedWriteWork work = work();
        R2dbcProtectedBatchSideIndex.Prepared prepared = new R2dbcProtectedBatchSideIndex.Prepared(
                List.of(new R2dbcProtectedBatchSideIndex.RowState(work, List.of())));

        assertThrows(IllegalStateException.class, () ->
                new R2dbcProtectedBatchSideIndex(R2dbcBindMarkers.from(factory))
                        .complete(connection(recorder, false), prepared,
                                  BatchChunkResult.committed(0, 0, 1, 1L))
                        .block(Duration.ofSeconds(5)));
    }

    @Test
    void r2dbcTokenBatchIsSplitAtTheFixedInternalLimit() {
        int tokenCount = R2dbcProtectedSideIndexDml.MAX_TOKEN_BATCH_SIZE + 1;
        SideIndexRecorder recorder = new SideIndexRecorder(tokenCount);
        ConnectionFactory factory = connectionFactory(connection(recorder, false));
        ProtectedWriteWork work = work(7L, tokenCount);
        R2dbcProtectedBatchSideIndex.Prepared prepared = new R2dbcProtectedBatchSideIndex.Prepared(
                List.of(new R2dbcProtectedBatchSideIndex.RowState(work, List.of())));

        new R2dbcProtectedBatchSideIndex(R2dbcBindMarkers.from(factory))
                .complete(connection(recorder, false), prepared,
                          BatchChunkResult.committed(0, 0, 1, 1L))
                .block(Duration.ofSeconds(5));

        assertEquals(2, recorder.statements.get());
        assertEquals(2, recorder.executions.get());
        assertEquals(R2dbcProtectedSideIndexDml.MAX_TOKEN_BATCH_SIZE,
                     recorder.maxParameterSetsPerStatement.get());
        assertEquals(tokenCount - 2, recorder.adds.get());
    }

    @Test
    void r2dbcTokenBatchIsAlsoSplitAtTheInternalParameterLimit() {
        int tokenCount = R2dbcProtectedSideIndexDml.MAX_TOKEN_BATCH_SIZE;
        int parametersPerToken = 5;
        SideIndexRecorder recorder = new SideIndexRecorder(tokenCount, parametersPerToken);
        List<List<Object>> parameterSets = java.util.stream.IntStream.range(0, tokenCount)
                .mapToObj(index -> List.<Object>of(
                        7L, "tenant", 8L, "phone", new byte[]{(byte) index}))
                .toList();

        R2dbcProtectedSideIndexDml.insertParameterSets(
                        connection(recorder, false),
                        "insert into token_index(tenant_id, form_id, id, field_tag, token) "
                                + "values (?, ?, ?, ?, ?)",
                        parameterSets)
                .block(Duration.ofSeconds(5));

        assertEquals(2, recorder.statements.get());
        assertEquals(2, recorder.executions.get());
        assertEquals(400, recorder.maxParameterSetsPerStatement.get());
        assertEquals(tokenCount - 2, recorder.adds.get());
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

    private static Connection connection(SideIndexRecorder recorder, boolean includeBusinessWrite) {
        return (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "isAutoCommit" -> true;
                    case "beginTransaction", "commitTransaction", "rollbackTransaction", "setAutoCommit", "close" ->
                            Mono.empty();
                    case "createStatement" -> ((String) arguments[0]).contains("token_index")
                            ? recorder.newStatement() : businessStatement(includeBusinessWrite);
                    case "toString" -> "protected-side-index-connection";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Statement businessStatement(boolean allowed) {
        Statement[] statement = new Statement[1];
        statement[0] = (Statement) Proxy.newProxyInstance(
                Statement.class.getClassLoader(), new Class<?>[]{Statement.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "bind", "bindNull", "add" -> statement[0];
                    case "execute" -> allowed
                            ? Flux.just(rowsUpdated(1L))
                            : Flux.error(new AssertionError(
                                    "batch side-index completion must not execute business DML"));
                    case "toString" -> "business-statement";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
        return statement[0];
    }

    private static Result rowsUpdated(long value) {
        return (Result) Proxy.newProxyInstance(
                Result.class.getClassLoader(), new Class<?>[]{Result.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getRowsUpdated" -> Mono.just(value);
                    case "toString" -> "rows-updated-result";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static Result rowsUpdatedError() {
        return (Result) Proxy.newProxyInstance(
                Result.class.getClassLoader(), new Class<?>[]{Result.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "getRowsUpdated" -> Mono.error(new IllegalStateException("rows-updated failed"));
                    case "toString" -> "failed-rows-updated-result";
                    default -> throw new UnsupportedOperationException(method.getName());
                });
    }

    private static final class SideIndexRecorder {

        private final int tokenCount;
        private final boolean aggregateBatchCount;
        private final int failedResultIndex;
        private final int errorResultIndex;
        private final int parameterCount;
        private final AtomicInteger statements = new AtomicInteger();
        private final AtomicInteger executions = new AtomicInteger();
        private final AtomicInteger adds = new AtomicInteger();
        private final AtomicInteger maxParameterSetsPerStatement = new AtomicInteger();
        private final List<List<Object>> parameterSets = new ArrayList<>();

        private SideIndexRecorder(int tokenCount) {
            this(tokenCount, false, -1, -1, 3);
        }

        private SideIndexRecorder(int tokenCount, int parameterCount) {
            this(tokenCount, false, -1, -1, parameterCount);
        }

        private SideIndexRecorder(int tokenCount, boolean aggregateBatchCount) {
            this(tokenCount, aggregateBatchCount, -1, -1, 3);
        }

        private SideIndexRecorder(int tokenCount, boolean aggregateBatchCount, int failedResultIndex) {
            this(tokenCount, aggregateBatchCount, failedResultIndex, -1, 3);
        }

        private SideIndexRecorder(int tokenCount,
                                  boolean aggregateBatchCount,
                                  int failedResultIndex,
                                  int errorResultIndex) {
            this(tokenCount, aggregateBatchCount, failedResultIndex, errorResultIndex, 3);
        }

        private SideIndexRecorder(int tokenCount,
                                  boolean aggregateBatchCount,
                                  int failedResultIndex,
                                  int errorResultIndex,
                                  int parameterCount) {
            this.tokenCount = tokenCount;
            this.aggregateBatchCount = aggregateBatchCount;
            this.failedResultIndex = failedResultIndex;
            this.errorResultIndex = errorResultIndex;
            this.parameterCount = parameterCount;
        }

        private Statement newStatement() {
            statements.incrementAndGet();
            List<Object> current = new ArrayList<>(
                    java.util.Collections.nCopies(parameterCount, null));
            AtomicInteger statementAdds = new AtomicInteger();
            Statement[] statement = new Statement[1];
            statement[0] = (Statement) Proxy.newProxyInstance(
                    Statement.class.getClassLoader(), new Class<?>[]{Statement.class},
                    (proxy, method, arguments) -> switch (method.getName()) {
                        case "bind" -> {
                            current.set((Integer) arguments[0], arguments[1]);
                            yield statement[0];
                        }
                        case "bindNull" -> {
                            current.set((Integer) arguments[0], null);
                            yield statement[0];
                        }
                        case "add" -> {
                            adds.incrementAndGet();
                            statementAdds.incrementAndGet();
                            parameterSets.add(List.copyOf(current));
                            yield statement[0];
                        }
                        case "execute" -> {
                            executions.incrementAndGet();
                            int statementParameterSets = statementAdds.get() + 1;
                            maxParameterSetsPerStatement.accumulateAndGet(
                                    statementParameterSets, Math::max);
                            if (parameterSets.size() < tokenCount) {
                                parameterSets.add(List.copyOf(current));
                            }
                            yield aggregateBatchCount && statementAdds.get() > 0
                                    ? Flux.just(rowsUpdated(statementParameterSets))
                                    : Flux.range(0, statementParameterSets)
                                            .map(index -> index == errorResultIndex
                                                    ? rowsUpdatedError() : rowsUpdated(
                                                            index == failedResultIndex ? 0L : 1L));
                        }
                        case "toString" -> "side-index-statement";
                        default -> throw new UnsupportedOperationException(method.getName());
                    });
            return statement[0];
        }

        private void assertOneBoundedTokenBatch() {
            assertOneBoundedTokenBatch(List.of(7L, 7L, 7L));
        }

        private void assertOneBoundedTokenBatch(List<Long> expectedOwners) {
            assertEquals(1, statements.get(), "one owner/field must create one INSERT statement");
            assertEquals(1, executions.get(), "one owner/field must execute one R2DBC statement batch");
            assertEquals(tokenCount - 1, adds.get(), "R2DBC add is required between parameter sets");
            assertEquals(tokenCount, parameterSets.size());
            for (int index = 0; index < tokenCount; index++) {
                List<Object> parameters = parameterSets.get(index);
                assertEquals(expectedOwners.get(index), parameters.get(0));
                assertEquals("phone", parameters.get(1));
                ByteBuffer token = assertInstanceOf(ByteBuffer.class, parameters.get(2));
                assertTrue(token.isReadOnly(), "owned token views must not expose mutable payloads");
                byte[] bytes = new byte[token.remaining()];
                token.duplicate().get(bytes);
                assertArrayEquals(new byte[]{(byte) (index % 3 + 1)}, bytes);
            }
        }
    }
}
