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
