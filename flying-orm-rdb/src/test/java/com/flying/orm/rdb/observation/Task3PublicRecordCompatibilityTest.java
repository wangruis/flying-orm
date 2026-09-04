package com.flying.orm.rdb.observation;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
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
                        BatchWriteOptions.Mode.ATOMIC,
                        1,
                        SqlExecutionBackend.JDBC);
        BatchExecutionObservation.Chunk chunk = new BatchExecutionObservation.Chunk(
                request,
                BatchChunkResult.Status.COMMITTED,
                0,
                0L,
                1L,
                1L,
                3L,
                SqlFailureCategory.NONE,
                null,
                null);
        ChunkReference chunkReference = new ChunkReference(
                request, BatchChunkResult.Status.COMMITTED, 0, 0L, 1L, 1L, 3L,
                SqlFailureCategory.NONE, null, null);

        assertEquals(writeReference.hashCode(), write.hashCode());
        assertEquals(chunkReference.hashCode(), chunk.hashCode());
        assertEquals(write, new ProtectedFieldRuntime.PreparedWrite(form, values));
        assertEquals(chunk, new BatchExecutionObservation.Chunk(
                request, BatchChunkResult.Status.COMMITTED, 0, 0L, 1L, 1L, 3L,
                SqlFailureCategory.NONE, null, null));
    }

    private record PreparedWriteReference(DynamicForm physicalForm, Map<String, Object> values) {
    }

    private record ChunkReference(BatchExecutionObservation.BatchWriteRequestView request,
                                  BatchChunkResult.Status status,
                                  int chunkIndex,
                                  long startOffset,
                                  long inputCount,
                                  long affectedRows,
                                  long durationNanos,
                                  SqlFailureCategory failureCategory,
                                  BatchChunkResult.Failure failure,
                                  BatchChunkResult.RecoveryToken recoveryToken) {
    }
}
