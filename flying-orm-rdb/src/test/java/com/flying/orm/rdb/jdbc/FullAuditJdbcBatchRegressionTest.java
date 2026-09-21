package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchOptimisticLockException;
import com.flying.orm.rdb.batch.BatchRowConflict;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteRequests;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FullAuditJdbcBatchRegressionTest {

    @Test
    void atomicConflictRetainsGlobalInputOffsetWithoutClaimingTransactionOutcome() {
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State()
                .outcome(1, 1).outcome(1, 0);
        JdbcBatchWriter writer = externalWriter(state);

        BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                () -> writer.writeBatch(exactlyOneRequest()));

        BatchOptimisticLockException conflict =
                assertInstanceOf(BatchOptimisticLockException.class, failure.getCause());
        assertAll(
                () -> assertEquals(List.of(BatchRowConflict.exactlyOne(3L, 0L)), conflict.conflicts()),
                () -> assertEquals(BatchExecutionState.PARTIAL, failure.evidence().state()),
                () -> assertEquals(List.of(3L), failure.evidence().failedOffsets().boxed().toList()));
    }

    @Test
    void evidenceConflictRetainsGlobalInputOffsetAndPendingExternalFact() {
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State()
                .outcome(1, 1).outcome(1, 0);
        JdbcBatchWriter writer = externalWriter(state);

        BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                () -> writer.writeBatchEvidence(exactlyOneRequest()));

        BatchOptimisticLockException conflict =
                assertInstanceOf(BatchOptimisticLockException.class, failure.getCause());
        assertAll(
                () -> assertEquals(List.of(BatchRowConflict.exactlyOne(3L, 0L)), conflict.conflicts()),
                () -> assertEquals(BatchExecutionState.PARTIAL, failure.evidence().state()),
                () -> assertEquals(List.of(0L, 1L, 2L), failure.evidence().successfulOffsets().boxed().toList()));
    }

    @Test
    void completeBusinessCountsSurviveStatementCloseFailure() {
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State().outcome(1, 1);
        SQLException closeFailure = new SQLException("statement close failed", "HY000");
        JdbcBatchWriter writer = externalWriter(state,
                failingStatementConnection(state, closeFailure, null));

        BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                () -> writer.writeBatchEvidence(rows(2, 2)));

        assertSame(closeFailure, failure.getCause());
        assertPendingKnown(failure, 2L, List.of(0L, 1L));
    }

    @Test
    void completedUpdateCountSurvivesGeneratedKeyReadFailure() {
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State();
        SQLException keyFailure = new SQLException("generated keys unavailable", "HY000");
        JdbcBatchWriter writer = externalWriter(state,
                failingStatementConnection(state, null, keyFailure));
        BatchWriteRequest base = rows(1, 1);
        BatchWriteRequest request = new BatchWriteRequest(
                base.statement(), base.parameterTypes(), base.rows(), base.options(),
                base.rowCountPolicy(), BatchGeneratedKeys.required("id", (offset, key) -> { }));

        BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                () -> writer.writeBatchEvidence(request));

        assertSame(keyFailure, failure.getCause());
        assertPendingKnown(failure, 1L, List.of(0L));
    }

    @Test
    void completeBusinessCountsSurviveProtectedSideIndexFailure() {
        SQLException tokenFailure = new SQLException("side-index insert failed", "HY000");
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State()
                .outcome(1).failure(tokenFailure);
        JdbcBatchWriter writer = externalWriter(state);

        BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                () -> writer.writeProtectedBatchEvidence(protectedInsertRequest()));

        assertSame(tokenFailure, failure.getCause());
        assertPendingKnown(failure, 1L, List.of(0L));
    }

    @Test
    void incompleteReturnedCountsRemainUnknown() {
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State().outcome(1);
        JdbcBatchWriter writer = externalWriter(state);

        BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                () -> writer.writeBatchEvidence(rows(2, 2)));

        assertInstanceOf(SQLException.class, failure.getCause());
        assertPendingUnknown(failure);
    }

    @Test
    void unreportedBatchUpdateExceptionTailRemainsUnknown() {
        BatchUpdateException partial = new BatchUpdateException(
                "unreported tail", "23000", 0, new int[]{1});
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State().failure(partial);
        JdbcBatchWriter writer = externalWriter(state);

        BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                () -> writer.writeBatchEvidence(rows(2, 2)));

        assertSame(partial, failure.getCause());
        assertPendingUnknown(failure);
        assertEquals(List.of(0L), failure.evidence().successfulOffsets().boxed().toList());
    }

    @Test
    void successNoInfoRemainsUnknownAfterStatementCloseFailure() {
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State()
                .outcome(1, Statement.SUCCESS_NO_INFO);
        SQLException closeFailure = new SQLException("statement close failed", "HY000");
        JdbcBatchWriter writer = externalWriter(state,
                failingStatementConnection(state, closeFailure, null));

        BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                () -> writer.writeBatchEvidence(rows(2, 2)));

        assertSame(closeFailure, failure.getCause());
        assertPendingUnknown(failure);
        assertEquals(List.of(0L, 1L), failure.evidence().successfulOffsets().boxed().toList());
    }

    @Test
    void executeFailedDoesNotBecomeAKnownZeroRowCount() {
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State()
                .outcome(1, Statement.EXECUTE_FAILED);
        JdbcBatchWriter writer = externalWriter(state);

        BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                () -> writer.writeBatchEvidence(rows(2, 2)));

        assertPendingUnknown(failure);
        BatchExecutionEvidence chunk = failure.evidence();
        assertEquals(List.of(0L), chunk.successfulOffsets().boxed().toList());
        assertEquals(List.of(1L), chunk.failedOffsets().boxed().toList());
    }

    @Test
    void overflowDoesNotPublishThePreviouslyAccumulatedCountAsKnown() {
        BatchUpdateException counts = new BatchUpdateException(
                "large batch counts", "HY000", 0, new long[]{Long.MAX_VALUE, 1L}, null);
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State().failure(counts);
        JdbcBatchWriter writer = externalWriter(state);

        BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                () -> writer.writeBatchEvidence(rows(2, 2)));

        RdbException overflow = assertInstanceOf(RdbException.class, failure.getCause());
        assertInstanceOf(ArithmeticException.class, overflow.getCause());
        assertPendingUnknown(failure);
        assertEquals(List.of(0L), failure.evidence().successfulOffsets().boxed().toList(),
                "the overflowing count must not mark its input position as fully recorded");
    }

    private static BatchWriteRequest rows(int rowCount, int chunkSize) {
        return JdbcBatchEvidenceTestSupport.request(
                Flux.range(0, rowCount).map(value -> new Object[]{value}), chunkSize);
    }

    private static BatchWriteRequest exactlyOneRequest() {
        BatchWriteRequest base = rows(4, 2);
        return new BatchWriteRequest(
                base.statement(), base.parameterTypes(), base.rows(), base.options(),
                BatchRowCountPolicy.EXACTLY_ONE, base.generatedKeys());
    }

    private static JdbcBatchWriter externalWriter(JdbcBatchEvidenceTestSupport.State state) {
        return externalWriter(state, state.connection());
    }

    private static JdbcBatchWriter externalWriter(JdbcBatchEvidenceTestSupport.State state,
                                                  Connection connection) {


        return JdbcBatchWriter.create(com.flying.orm.rdb.execution.ConnectionAccessTestSupport.borrowed(connection), com.flying.orm.rdb.dialect.RdbDialect.h2())
                ;
    }

    private static void assertPendingKnown(BatchExecutionEvidenceException failure,
                                           long affectedRows, List<Long> offsets) {
        BatchExecutionEvidence evidence = failure.evidence();
        BatchExecutionEvidence chunk = evidence;
        assertAll(
                () -> assertEquals(BatchExecutionState.PARTIAL, evidence.state()),
                () -> assertEquals(BatchExecutionState.PARTIAL, chunk.state()),
                () -> assertTrue(evidence.affectedRows().isKnown()),
                () -> assertEquals(affectedRows, evidence.affectedRows().value()),
                () -> assertTrue(chunk.affectedRows().isKnown()),
                () -> assertEquals(affectedRows, chunk.affectedRows().value()),
                () -> assertEquals(offsets, chunk.successfulOffsets().boxed().toList()),
                () -> assertTrue(chunk.failedOffsets().boxed().toList().isEmpty()));
    }

    private static void assertPendingUnknown(BatchExecutionEvidenceException failure) {
        BatchExecutionEvidence evidence = failure.evidence();
        assertAll(
                () -> assertNotEquals(BatchExecutionState.SUCCESS, evidence.state()),
                () -> assertFalse(evidence.affectedRows().isKnown()),
                () -> assertFalse(evidence.affectedRows().isKnown()));
    }

    private static BatchWriteRequest protectedInsertRequest() {
        SqlRequest insert = new SqlRequest("insert into samples(id, value) values (?, ?)",
                List.of(1L, "ciphertext"));
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT, insert, null,
                List.of("id"), Map.of("id", 1L), "id = ?",
                "delete from token_index where id = ? and field_tag = ?",
                "insert into token_index(id, field_tag, token) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("value",
                        List.of(new byte[]{1}, new byte[]{2}, new byte[]{3}))));
        Object[] row = ProtectedBatchRows.extend(insert.parameters().toArray(), work);
        return BatchWriteRequests.request(insert.sql(), 2, List.of(Long.class, String.class),
                SqlBindMarkerStyle.CANONICAL, Flux.<Object[]>just(row), BatchWriteOptions.of(1));
    }

    // Extend the existing driver fixture only at the Statement failure boundary.
    // SQL execution, evidence collection and exception assembly remain production code.
    private static Connection failingStatementConnection(JdbcBatchEvidenceTestSupport.State state,
                                                          SQLException closeFailure,
                                                          SQLException keyFailure) {
        Connection delegate = state.connection();
        return proxy(Connection.class, (self, method, arguments) -> {
            Object result = invoke(delegate, method, arguments);
            if (!method.getName().equals("prepareStatement")) {
                return result;
            }
            PreparedStatement statement = (PreparedStatement) result;
            return proxy(PreparedStatement.class, (statementProxy, statementMethod, statementArguments) -> {
                String name = statementMethod.getName();
                if (name.equals("close") && closeFailure != null) {
                    throw closeFailure;
                }
                if (keyFailure != null) {
                    if (name.equals("executeLargeUpdate")) {
                        return 1L;
                    }
                    if (name.equals("getGeneratedKeys")) {
                        throw keyFailure;
                    }
                }
                return invoke(statement, statementMethod, statementArguments);
            });
        });
    }

    private static Object invoke(Object target, Method method, Object[] arguments) throws Throwable {
        try {
            return method.invoke(target, arguments);
        } catch (InvocationTargetException failure) {
            throw failure.getCause();
        }
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
