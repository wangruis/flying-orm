package com.flying.orm.rdb.batch;

import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.exception.RdbExceptionTranslator;
import io.r2dbc.spi.R2dbcException;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

class BatchExecutionEvidenceFailureBoundaryTest {

    @Test
    void eachErrorKindOwnsItsSafeMessage() {
        for (RdbErrorKind kind : RdbErrorKind.values()) {
            RdbException error = new RdbException(kind, "private-value from private_table", "23505", 7,
                    new SQLException("private-driver-value"));
            BatchExecutionEvidence.Failure failure = BatchExecutionEvidence.Failure.from(error);
            assertEquals(kind.message(), failure.message());
            assertFalse(failure.toString().contains("private"));
            assertSame(error, RdbExceptionTranslator.translate(error));
            assertEquals("private-value from private_table", error.getMessage());
            assertEquals("database operation failed", error.toErrorReport().message());
        }
    }

    @Test
    void jdbcAndR2dbcRetainEveryStableMessageAndOriginalCause() {
        List<String> states = List.of("23505", "23000", "42000", "08006", "HYT00",
                "40P01", "55P03", "57014", "ZZZZZ");
        List<String> messages = List.of("database duplicate key conflict", "database integrity constraint failed",
                "database rejected sql", "database connection failed", "database operation timed out",
                "database transaction deadlocked", "database lock wait timed out", "database operation was cancelled",
                "database operation failed");
        RdbErrorKind[] kinds = RdbErrorKind.values();
        assertEquals(kinds.length, states.size());
        for (int index = 0; index < kinds.length; index++) {
            String state = states.get(index);
            List<Throwable> failures = List.of(new SQLException("private-value", state),
                    new R2dbcException("private-value", state) { });
            for (Throwable failure : failures) {
                RdbException translated = assertInstanceOf(RdbException.class, RdbExceptionTranslator.translate(failure));
                assertEquals(kinds[index], translated.kind());
                assertEquals(messages.get(index), translated.getMessage());
                assertSame(failure, translated.getCause());
                BatchExecutionEvidence.Failure summary = BatchExecutionEvidence.Failure.from(failure);
                assertEquals(messages.get(index), summary.message());
                assertEquals(state, summary.sqlState());
            }
        }
    }

    @Test
    void businessFailureSummaryStaysGenericWithoutWrappingTheOriginal() {
        IllegalStateException original = new IllegalStateException("private-value");
        assertSame(original, RdbExceptionTranslator.translate(original));
        BatchExecutionEvidence.Failure summary = BatchExecutionEvidence.Failure.from(original);
        assertEquals(RdbErrorKind.UNKNOWN, summary.kind());
        assertEquals("database operation failed", summary.message());
        assertNull(summary.sqlState());
        assertEquals(0, summary.errorCode());
    }

    @Test
    void executionEvidenceOwnsSafeFailureWithoutRetainingDriverMessage() throws Exception {
        Class<?> failureType = Class.forName(
                "com.flying.orm.rdb.batch.BatchExecutionEvidence$Failure");
        assertEquals(List.of("type", "message", "sqlState", "errorCode", "kind"),
                     Arrays.stream(failureType.getRecordComponents())
                           .map(component -> component.getName())
                           .toList());

        Method from = failureType.getMethod("from", Throwable.class);
        String sensitiveMessage = "duplicate secret-value from insert into private_table";
        Object valid = from.invoke(null, new SQLException(sensitiveMessage, "23505", 1062));

        assertEquals(SQLException.class.getName(), failureType.getMethod("type").invoke(valid));
        assertEquals("database duplicate key conflict", failureType.getMethod("message").invoke(valid));
        assertFalse(((String) failureType.getMethod("message").invoke(valid)).contains(sensitiveMessage));
        assertFalse(valid.toString().contains(sensitiveMessage));
        assertEquals("23505", failureType.getMethod("sqlState").invoke(valid));
        assertEquals(1062, failureType.getMethod("errorCode").invoke(valid));
        assertEquals(RdbErrorKind.DUPLICATE_KEY, failureType.getMethod("kind").invoke(valid));

        Object invalid = from.invoke(null, new SQLException(sensitiveMessage, "2*505", 1062));
        assertNull(failureType.getMethod("sqlState").invoke(invalid));
        assertEquals(1062, failureType.getMethod("errorCode").invoke(invalid));
    }
}
