package com.flying.orm.rdb.observation;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

/** 锁住观测事件的状态边界，避免上层指标同时读到两套互相冲突的结论。 */
class ExecutionObservationContractTest {

    @Test
    void rejectsContradictorySqlStatusAndFailureDetails() {
        assertThrows(IllegalArgumentException.class,
                     () -> observation(SqlExecutionStatus.SUCCESS,
                                       SqlFailureCategory.TIMEOUT,
                                       null));
        assertThrows(IllegalArgumentException.class,
                     () -> observation(SqlExecutionStatus.ERROR,
                                       SqlFailureCategory.NONE,
                                       new IllegalStateException("failed")));
        assertThrows(IllegalArgumentException.class,
                     () -> observation(SqlExecutionStatus.ERROR,
                                       SqlFailureCategory.TIMEOUT,
                                       null));
    }

    @Test
    void rejectsBatchEventWithMoreThanOnePrimaryStatus() {
        assertThrows(IllegalArgumentException.class,
                     () -> new BatchExecutionObservation(BatchExecutionEventType.CHUNK,
                                                         SqlStatementType.UPDATE,
                                                         "update users set name = ?",
                                                         BatchWriteOptions.Mode.ATOMIC,
                                                         BatchWriteResult.Status.COMMITTED,
                                                         BatchChunkResult.Status.COMMITTED,
                                                         null,
                                                         0,
                                                         0,
                                                         1,
                                                         1,
                                                         1,
                                                         1,
                                                         SqlFailureCategory.NONE,
                                                         null,
                                                         null));
    }

    private static SqlExecutionObservation observation(SqlExecutionStatus status,
                                                       SqlFailureCategory category,
                                                       Throwable error) {
        return new SqlExecutionObservation(SqlExecutionOperation.UPDATE,
                                           SqlStatementType.UPDATE,
                                           status,
                                           category,
                                           "update users set name = ?",
                                           1,
                                           0,
                                           1,
                                           1,
                                           error);
    }
}
