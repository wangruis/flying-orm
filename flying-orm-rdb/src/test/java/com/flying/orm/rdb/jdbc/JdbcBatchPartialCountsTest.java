package com.flying.orm.rdb.jdbc;
import com.flying.orm.rdb.batch.*;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import java.sql.*;
import java.util.List;
import static org.junit.jupiter.api.Assertions.*;

class JdbcBatchPartialCountsTest {
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
        var failure = assertThrows(BatchExecutionEvidenceException.class, () -> state.writer().writeBatchEvidence(
                JdbcBatchEvidenceTestSupport.request(Flux.range(0, 4).map(i -> new Object[]{i}), 4)));
        assertEquals(List.of(0L), failure.evidence().successfulOffsets().boxed().toList());
        assertEquals(0, failure.evidence().failedCount());
        assertFalse(failure.evidence().affectedRows().isKnown());
    }

    @Test void ordinaryAndEvidenceBothAcceptSuccessfulUnknownCounts() {
        for (boolean alias : new boolean[]{false, true}) {
            var state = new JdbcBatchEvidenceTestSupport.State().outcome(Statement.SUCCESS_NO_INFO);
            var request = JdbcBatchEvidenceTestSupport.request(Flux.<Object[]>just(new Object[]{1}), 1);
            var fact = alias ? state.writer().writeBatchEvidence(request) : state.writer().writeBatch(request);
            assertEquals(BatchExecutionState.SUCCESS, fact.state());
            assertEquals(1, fact.successfulCount());
            assertFalse(fact.affectedRows().isKnown());
        }
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
