package com.flying.orm.rdb.observation;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchResolution;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** 固定 SQL、批量分片、汇总和 UNKNOWN 恢复状态到业务结果分类的映射。 */
class SqlExecutionResultKindTest {

    @Test
    void mapsSqlStatusAndFailureCategoryToBusinessResultKind() {
        assertEquals(SqlExecutionResultKind.SUCCESS,
                     SqlExecutionResultKind.fromSql(SqlExecutionStatus.SUCCESS, SqlFailureCategory.NONE));
        assertEquals(SqlExecutionResultKind.CANCELLED,
                     SqlExecutionResultKind.fromSql(SqlExecutionStatus.CANCELLED, SqlFailureCategory.NONE));
        assertEquals(SqlExecutionResultKind.TIMEOUT,
                     SqlExecutionResultKind.fromSql(SqlExecutionStatus.ERROR, SqlFailureCategory.TIMEOUT));
        assertEquals(SqlExecutionResultKind.ROW_LIMIT,
                     SqlExecutionResultKind.fromSql(SqlExecutionStatus.ERROR, SqlFailureCategory.ROW_LIMIT));
        assertEquals(SqlExecutionResultKind.CONNECTION,
                     SqlExecutionResultKind.fromSql(SqlExecutionStatus.ERROR, SqlFailureCategory.CONNECTION));
    }

    @Test
    void mapsBatchStatusToBusinessResultKind() {
        assertEquals(SqlExecutionResultKind.SUCCESS,
                     SqlExecutionResultKind.fromBatchChunk(BatchChunkResult.Status.COMMITTED,
                                                           SqlFailureCategory.NONE));
        assertEquals(SqlExecutionResultKind.ENLISTED,
                     SqlExecutionResultKind.fromBatchChunk(BatchChunkResult.Status.ENLISTED,
                                                           SqlFailureCategory.NONE));
        assertEquals(SqlExecutionResultKind.ROLLED_BACK,
                     SqlExecutionResultKind.fromBatchChunk(BatchChunkResult.Status.ROLLED_BACK,
                                                           SqlFailureCategory.NONE));
        assertEquals(SqlExecutionResultKind.PARTIAL,
                     SqlExecutionResultKind.fromBatchSummary(BatchWriteResult.Status.PARTIAL,
                                                             SqlFailureCategory.UNKNOWN));
        assertEquals(SqlExecutionResultKind.ENLISTED,
                     SqlExecutionResultKind.fromBatchSummary(BatchWriteResult.Status.ENLISTED,
                                                             SqlFailureCategory.NONE));
        assertEquals(SqlExecutionResultKind.UNKNOWN,
                     SqlExecutionResultKind.fromBatchSummary(BatchWriteResult.Status.UNKNOWN,
                                                             SqlFailureCategory.UNKNOWN));
        assertEquals(SqlExecutionResultKind.TIMEOUT,
                     SqlExecutionResultKind.fromBatchSummary(BatchWriteResult.Status.UNKNOWN,
                                                             SqlFailureCategory.TIMEOUT));
        assertEquals(SqlExecutionResultKind.SUCCESS,
                     SqlExecutionResultKind.fromBatchRecovery(BatchResolution.Status.COMMITTED,
                                                              SqlFailureCategory.NONE));
    }

    @Test
    void keepsKnownDatabaseFailureCategoryInBatchObservations() {
        RdbException deadlock = new RdbException(RdbErrorKind.DEADLOCK,
                                                 "deadlock detected",
                                                 "40P01",
                                                 null,
                                                 new IllegalStateException("driver deadlock"));
        BatchChunkResult failed = BatchChunkResult.failed(0, 0, 1, deadlock);
        BatchWriteResult result = BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT,
                                                        List.of(failed));
        BatchExecutionObservation.BatchWriteRequestView request =
                new BatchExecutionObservation.BatchWriteRequestView("update users set name = ?",
                                                                    BatchWriteOptions.Mode.INDEPENDENT,
                                                                    1);

        assertEquals(SqlFailureCategory.DEADLOCK,
                     BatchExecutionObservation.chunk(request, failed, 1).failureCategory());
        assertEquals(SqlFailureCategory.DEADLOCK,
                     BatchExecutionObservation.summary(request, result, 1).failureCategory());
        assertEquals(SqlFailureCategory.DEADLOCK,
                     BatchExecutionObservation.summary(request,
                                                       result.status(),
                                                       result.inputCount(),
                                                       result.affectedRows(),
                                                       result.conflictCount(),
                                                       failed.failure(),
                                                       null,
                                                       1)
                                              .failureCategory());
    }
}
