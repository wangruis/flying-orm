package com.flying.orm.rdb.observation;

import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SqlFailureCategoryTraversalTest {

    @Test
    void classifiesOneCauseChainWithOneTraversal() {
        RdbException databaseFailure = new RdbException(
                RdbErrorKind.DEADLOCK, "deadlock", "40001", 1213, new IllegalStateException("driver"));
        CountingFailure middle = new CountingFailure(databaseFailure);
        CountingFailure root = new CountingFailure(middle);

        assertEquals(SqlFailureCategory.DEADLOCK, SqlFailureCategory.classify(root));
        assertEquals(1, root.causeReads());
        assertEquals(1, middle.causeReads());
    }

    @Test
    void doesNotDiscardKnownFailureBeyondAnArbitraryCauseDepth() {
        Throwable failure = new RdbException(
                RdbErrorKind.DEADLOCK, "deadlock", "40001", 1213, new IllegalStateException("driver"));
        for (int index = 0; index < 100; index++) {
            failure = new IllegalStateException("application context", failure);
        }
        assertEquals(SqlFailureCategory.DEADLOCK, SqlFailureCategory.classify(failure));
    }

    @Test
    void aCauseCycleWithoutDatabaseFactsRemainsUnknown() {
        java.util.concurrent.CompletionException first = new java.util.concurrent.CompletionException("first", null) {
            @Override public synchronized Throwable getCause() { return second; }
            private final Throwable second = new java.util.concurrent.CompletionException(this);
        };
        assertEquals(SqlFailureCategory.UNKNOWN, SqlFailureCategory.classify(first));
    }

    private static final class CountingFailure extends RuntimeException {
        private int causeReads;

        private CountingFailure(Throwable cause) {
            super(cause);
        }

        @Override
        public synchronized Throwable getCause() {
            causeReads++;
            return super.getCause();
        }

        private int causeReads() {
            return causeReads;
        }
    }
}
