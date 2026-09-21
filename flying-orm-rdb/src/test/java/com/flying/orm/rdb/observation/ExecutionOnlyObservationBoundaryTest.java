package com.flying.orm.rdb.observation;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExecutionOnlyObservationBoundaryTest {
    @Test
    void observersNeverRequestOrReceiveTransactionSource() {
        assertFalse(Arrays.stream(SqlExecutionObserver.class.getMethods())
                          .anyMatch(method -> method.getName().equals("requiresTransactionSource")));
        for (Class<?> type : List.of(SqlExecutionObserver.class, BatchExecutionObserver.class)) {
            assertFalse(Arrays.stream(type.getMethods()).flatMap(method -> Arrays.stream(method.getParameterTypes()))
                              .anyMatch(parameter -> parameter.getSimpleName().equals("SqlTransactionSource")));
        }
    }

    @Test
    void batchProgressDoesNotPublishInternalChunksOrCommitResults() {
        assertTrue(BatchExecutionObservation.class.isRecord());
        assertEquals(List.of("PROGRESS", "SUMMARY"),
                     Arrays.stream(BatchExecutionEventType.values()).map(Enum::name).toList());
        assertFalse(Arrays.stream(BatchExecutionObservation.class.getMethods()).map(Method::getName)
                          .anyMatch(name -> name.equals("mode") || name.equals("chunkIndex")));
    }

    @Test
    void resourceCleanupReportsReleaseAndExecutionInsteadOfTransactionOutcome() {
        assertEquals(List.of("operation", "phase", "executionCompleted", "error"),
                     Arrays.stream(ResourceCleanupObservation.class.getRecordComponents())
                           .map(component -> component.getName()).toList());
        assertEquals(List.of("CONNECTION_RELEASE", "SESSION_CLEANUP", "LOB_CLEANUP"),
                     Arrays.stream(ResourceCleanupObservation.Phase.values()).map(Enum::name).toList());
        assertFalse(Arrays.stream(SqlExecutionResultKind.values())
                          .anyMatch(value -> value.name().equals("ENLISTED") || value.name().equals("ROLLED_BACK")));
    }
}
