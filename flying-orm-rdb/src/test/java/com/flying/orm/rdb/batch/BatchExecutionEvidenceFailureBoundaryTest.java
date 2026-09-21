package com.flying.orm.rdb.batch;

import com.flying.orm.rdb.exception.RdbErrorKind;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

class BatchExecutionEvidenceFailureBoundaryTest {

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
