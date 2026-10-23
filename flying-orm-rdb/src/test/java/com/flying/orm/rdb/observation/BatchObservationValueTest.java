package com.flying.orm.rdb.observation;

import com.flying.orm.rdb.batch.BatchAffectedRows;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class BatchObservationValueTest {

    @Test
    void preservesGeneratedRecordHashSemantics() {
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

        assertEquals(reference.hashCode(), event.hashCode());
        assertEquals(event, new BatchExecutionObservation(
                BatchExecutionEventType.PROGRESS, request, BatchExecutionState.SUCCESS,
                1L, BatchAffectedRows.known(1), 1L, 0L, 0L, 3L, SqlFailureCategory.NONE, null));
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
