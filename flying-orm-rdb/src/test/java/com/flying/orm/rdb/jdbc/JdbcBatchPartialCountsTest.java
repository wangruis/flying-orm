package com.flying.orm.rdb.jdbc;
import com.flying.orm.rdb.batch.*;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import java.sql.*;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class JdbcBatchPartialCountsTest {
    @Test void completeFailuresAreNotUnknownExecutions() {
        for (int count : new int[]{0, 2, Statement.EXECUTE_FAILED}) {
            for (boolean driverThrows : new boolean[]{false, true}) {
                JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State();
                if (driverThrows) state.failure(new BatchUpdateException("failed", "HY000", 0,
                        new int[]{count, count}));
                else state.outcome(count, count);
                BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                        () -> state.writer().writeBatch(strictRequest(2)));
                assertEquals(BatchExecutionState.FAILED, failure.evidence().state());
                assertEquals(2, failure.evidence().failedCount());
                assertEquals(count >= 0, failure.evidence().affectedRows().isKnown());
            }
        }
    }

    @Test void confirmedFailureDoesNotHideAnUnreportedTail() {
        for (int count : new int[]{0, Statement.EXECUTE_FAILED}) {
            for (boolean driverThrows : new boolean[]{false, true}) {
                JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State();
                if (driverThrows) state.failure(new BatchUpdateException("partial", "08006", 0,
                        new int[]{count}));
                else state.outcome(count);
                BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                        () -> state.writer().writeBatch(strictRequest(2)));
                assertEquals(BatchExecutionState.UNKNOWN, failure.evidence().state());
                assertEquals(1, failure.evidence().failedCount());
                assertFalse(failure.evidence().affectedRows().isKnown());
            }
        }
    }

    @Test void connectionLostDuringExecutionCannotProveFailure() {
        JdbcBatchEvidenceTestSupport.State state = new JdbcBatchEvidenceTestSupport.State()
                .failure(new SQLException("connection lost", "08006"));
        BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                () -> state.writer().writeBatch(strictRequest(2)));
        assertEquals(BatchExecutionState.UNKNOWN, failure.evidence().state());
        assertEquals(0, failure.evidence().failedCount());
        assertEquals(0, failure.evidence().successfulCount());
    }

    private static BatchWriteRequest strictRequest(int size) {
        BatchWriteRequest request = JdbcBatchEvidenceTestSupport.request(
                Flux.range(0, size).map(index -> new Object[]{index}), size);
        return new BatchWriteRequest(request.statement(), request.parameterTypes(), request.rows(),
                request.options(), BatchRowCountPolicy.EXACTLY_ONE, request.generatedKeys());
    }

    @Test void preservesProvableBatchUpdateExceptionCounts() {
        var state = new JdbcBatchEvidenceTestSupport.State().failure(new BatchUpdateException(
                "partial", "23000", 0, new int[]{1, Statement.SUCCESS_NO_INFO, Statement.EXECUTE_FAILED, 1}));
        var failure = assertThrows(BatchExecutionEvidenceException.class, () -> state.writer().writeBatch(
                JdbcBatchEvidenceTestSupport.request(Flux.range(0, 5).map(i -> new Object[]{i}), 5)));
        var fact = failure.evidence();
        assertEquals(BatchExecutionState.PARTIAL, fact.state());
        assertEquals(List.of(0L,1L,3L), fact.successfulOffsets().boxed().toList());
        assertEquals(List.of(2L), fact.failedOffsets().boxed().toList());
        assertEquals(5, fact.inputCount());
        assertFalse(fact.affectedRows().isKnown());
        assertFalse(fact.isSuccessful(4));
        assertFalse(fact.isFailed(4));
        assertEquals(1, state.released.get());
    }

    @Test void keepsUnreportedBatchUpdateExceptionTailUnknown() {
        var state = new JdbcBatchEvidenceTestSupport.State().failure(
                new BatchUpdateException("partial", "23000", 0, new int[]{1}));
        var failure = assertThrows(BatchExecutionEvidenceException.class, () -> state.writer().writeBatch(
                JdbcBatchEvidenceTestSupport.request(Flux.range(0, 4).map(i -> new Object[]{i}), 4)));
        assertEquals(List.of(0L), failure.evidence().successfulOffsets().boxed().toList());
        assertEquals(0, failure.evidence().failedCount());
        assertFalse(failure.evidence().affectedRows().isKnown());
    }

    @Test void batchAcceptsSuccessfulUnknownCounts() {
        var state = new JdbcBatchEvidenceTestSupport.State().outcome(Statement.SUCCESS_NO_INFO);
        var request = JdbcBatchEvidenceTestSupport.request(Flux.<Object[]>just(new Object[]{1}), 1);
        var fact = state.writer().writeBatch(request);
        assertEquals(BatchExecutionState.SUCCESS, fact.state());
        assertEquals(1, fact.successfulCount());
        assertFalse(fact.affectedRows().isKnown());
    }

    @Test void preservesAllReturnedPositionsAfterExecuteFailed() {
        var state = new JdbcBatchEvidenceTestSupport.State().outcome(1, Statement.EXECUTE_FAILED, 1, 1);
        var failure = assertThrows(BatchExecutionEvidenceException.class, () -> state.writer().writeBatch(
                JdbcBatchEvidenceTestSupport.request(Flux.range(0, 4).map(i -> new Object[]{i}), 4)));
        assertEquals(List.of(0L,2L,3L), failure.evidence().successfulOffsets().boxed().toList());
        assertEquals(List.of(1L), failure.evidence().failedOffsets().boxed().toList());
    }

    @Test void exactPolicyKeepsEveryConflictAndKnownCount() {
        var state = new JdbcBatchEvidenceTestSupport.State().outcome(1, 0, 2, 1, 0);
        var request = JdbcBatchEvidenceTestSupport.request(Flux.range(0, 5).map(i -> new Object[]{i}), 5);
        request = new BatchWriteRequest(request.statement(), request.parameterTypes(), request.rows(),
                request.options(), BatchRowCountPolicy.EXACTLY_ONE, request.generatedKeys());
        var strict = request;
        var failure = assertThrows(BatchExecutionEvidenceException.class, () -> state.writer().writeBatch(strict));
        assertEquals(List.of(0L,3L), failure.evidence().successfulOffsets().boxed().toList());
        assertEquals(List.of(1L,2L,4L), failure.evidence().failedOffsets().boxed().toList());
        assertEquals(List.of(1L,2L,4L), failure.evidence().conflicts().stream().map(BatchRowConflict::inputOffset).toList());
        assertEquals(4, failure.evidence().affectedRows().value());
    }

    @Test void strictUnknownCountRetainsAllObservedSqlPositions() {
        var state = new JdbcBatchEvidenceTestSupport.State().outcome(1, Statement.SUCCESS_NO_INFO, 0, 1);
        var request = JdbcBatchEvidenceTestSupport.request(Flux.range(0, 4).map(i -> new Object[]{i}), 4);
        var strict = new BatchWriteRequest(request.statement(), request.parameterTypes(), request.rows(),
                request.options(), BatchRowCountPolicy.EXACTLY_ONE, request.generatedKeys());
        var failure = assertThrows(BatchExecutionEvidenceException.class, () -> state.writer().writeBatch(strict));
        assertEquals(List.of(0L,1L,3L), failure.evidence().successfulOffsets().boxed().toList());
        assertEquals(List.of(2L), failure.evidence().failedOffsets().boxed().toList());
        assertFalse(failure.evidence().affectedRows().isKnown());
    }
}
