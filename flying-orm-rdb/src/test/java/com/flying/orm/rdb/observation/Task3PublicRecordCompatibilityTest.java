package com.flying.orm.rdb.observation;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.batch.BatchAffectedRows;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class Task3PublicRecordCompatibilityTest {

    @Test
    void preservesGeneratedRecordHashSemantics() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("id", "BIGINT"))
                                      .build();
        Map<String, Object> values = Map.of("id", 7L);
        ProtectedFieldRuntime.PreparedWrite write =
                new ProtectedFieldRuntime.PreparedWrite(form, values);
        PreparedWriteReference writeReference = new PreparedWriteReference(form, values);
        BatchExecutionObservation.BatchWriteRequestView request =
                new BatchExecutionObservation.BatchWriteRequestView(
                        "insert into events(id) values (?)",
                        1,
                        SqlExecutionBackend.JDBC);
        BatchExecutionObservation event = new BatchExecutionObservation(
                BatchExecutionEventType.PROGRESS, request, BatchExecutionState.SUCCESS,
                1L, BatchAffectedRows.known(1), 1L, 0L, 0L, 3L, SqlFailureCategory.NONE, null);
        ObservationReference reference = new ObservationReference(
                BatchExecutionEventType.PROGRESS, request, BatchExecutionState.SUCCESS,
                1L, BatchAffectedRows.known(1), 1L, 0L, 0L, 3L, SqlFailureCategory.NONE, null);

        assertEquals(writeReference.hashCode(), write.hashCode());
        assertEquals(reference.hashCode(), event.hashCode());
        assertEquals(write, new ProtectedFieldRuntime.PreparedWrite(form, values));
        assertEquals(event, new BatchExecutionObservation(
                BatchExecutionEventType.PROGRESS, request, BatchExecutionState.SUCCESS,
                1L, BatchAffectedRows.known(1), 1L, 0L, 0L, 3L, SqlFailureCategory.NONE, null));
    }

    private record PreparedWriteReference(DynamicForm physicalForm, Map<String, Object> values) {
    }

    private record ObservationReference(BatchExecutionEventType eventType,
                                        BatchExecutionObservation.BatchWriteRequestView request,
                                        BatchExecutionState state,
                                        long inputCount,
                                        BatchAffectedRows affectedRows,
                                        long successfulCount,
                                        long failedCount,
                                        long conflictCount,
                                        long durationNanos,
                                        SqlFailureCategory failureCategory,
                                        BatchExecutionEvidence.Failure failure) {
    }
}
