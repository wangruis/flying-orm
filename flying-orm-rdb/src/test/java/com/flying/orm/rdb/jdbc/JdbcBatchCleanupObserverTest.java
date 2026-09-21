package com.flying.orm.rdb.jdbc;
import com.flying.orm.rdb.batch.*;
import com.flying.orm.rdb.observation.*;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import java.sql.SQLException;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class JdbcBatchCleanupObserverTest {
    @Test void releaseFailurePreservesCompletedSqlAndIsReported() {
        var state = new JdbcBatchEvidenceTestSupport.State();
        state.releaseFailure = new SQLException("release failed");
        var observer = new Observer();
        var failure = assertThrows(BatchExecutionEvidenceException.class, () -> state.writer()
                .withBatchObserver(observer).writeBatch(request()));
        assertEquals(1, failure.evidence().successfulCount());
        assertEquals(1, failure.evidence().affectedRows().value());
        assertEquals(1, state.released.get());
        assertEquals(1, observer.cleanups.size());
        assertEquals(ResourceCleanupObservation.Phase.CONNECTION_RELEASE, observer.cleanups.getFirst().phase());
        assertTrue(observer.cleanups.getFirst().executionCompleted());
    }

    @Test void disabledObserverDoesNotReceiveCleanup() {
        var state = new JdbcBatchEvidenceTestSupport.State();
        state.releaseFailure = new SQLException("release failed");
        var observer = new Observer(); observer.enabled = false;
        assertThrows(BatchExecutionEvidenceException.class, () -> state.writer().withBatchObserver(observer).writeBatch(request()));
        assertEquals(List.of(), observer.cleanups);
    }

    @Test void observerRuntimeFailureCannotReplaceReleaseFailure() {
        var state = new JdbcBatchEvidenceTestSupport.State();
        state.releaseFailure = new SQLException("release failed");
        var observer = new Observer(); observer.failure = new IllegalArgumentException("observer");
        var failure = assertThrows(BatchExecutionEvidenceException.class, () -> state.writer().withBatchObserver(observer).writeBatch(request()));
        assertSame(state.releaseFailure, failure.getCause().getCause());
        assertEquals(1, failure.evidence().successfulCount());
    }

    @Test void directCleanupFatalWinsAndKeepsOperationFailure() {
        var state = new JdbcBatchEvidenceTestSupport.State().failure(new SQLException("execute failed"));
        VirtualMachineError fatal = new VirtualMachineError("release fatal") {};
        state.releaseFailure = fatal;
        assertSame(fatal, assertThrows(VirtualMachineError.class, () -> state.writer().writeBatch(request())));
        assertEquals(1, state.released.get());
        assertEquals(1, fatal.getSuppressed().length);
    }

    @Test void directCleanupObserverErrorRemainsPrimaryAfterReleaseFailure() {
        var state = new JdbcBatchEvidenceTestSupport.State();
        state.releaseFailure = new SQLException("release failed");
        var observer = new Observer();
        observer.error = new AssertionError("observer error");
        assertSame(observer.error, assertThrows(AssertionError.class, () -> state.writer()
                .withBatchObserver(observer).writeBatch(request())));
        assertEquals(1, state.released.get());
        assertEquals(1, observer.error.getSuppressed().length);
    }

    @Test void statementCleanupFailurePreservesSqlFactsAndPreventsPost() {
        var state = new JdbcBatchEvidenceTestSupport.State();
        state.statementCloseFailure = new SQLException("close");
        var failure = assertThrows(BatchExecutionEvidenceException.class, () -> state.writer().writeBatch(
                request(), offset -> fail("statement cleanup incomplete")));
        assertEquals(1, failure.evidence().successfulCount());
        assertEquals(1, failure.evidence().affectedRows().value());
        assertEquals(1, state.released.get());
    }

    private static BatchWriteRequest request() {
        return JdbcBatchEvidenceTestSupport.request(Flux.<Object[]>just(new Object[]{1}), 1);
    }
    static final class Observer implements BatchExecutionObserver, SqlExecutionObserver {
        boolean enabled = true;
        RuntimeException failure;
        Error error;
        List<ResourceCleanupObservation> cleanups = new ArrayList<>();
        public boolean enabled() { return enabled; }
        public void onExecution(BatchExecutionObservation event) {}
        public void onExecution(SqlExecutionObservation event) {}
        public void onResourceCleanup(ResourceCleanupObservation event) {
            cleanups.add(event);
            if (error != null) throw error;
            if (failure != null) throw failure;
        }
    }
}
