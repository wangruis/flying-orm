package com.flying.orm.rdb.batch;

import com.flying.orm.rdb.exception.RdbErrorKind;
import io.r2dbc.spi.R2dbcException;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证批量分片结果可以准确汇总，不把已回滚数据算进影响行数。
 *
 * @author wangr
 * @date 2026-07-23
 * @version v1.0
 */
class BatchWriteResultTest {

    @Test
    void exposesStableFailureKindForIndependentBusinessHandling() {
        BatchChunkResult failed = BatchChunkResult.failed(0,
                                                          0,
                                                          1,
                                                          new DriverException("deadlock", "40001", 1205));

        assertNotNull(failed.failure());
        assertEquals(RdbErrorKind.DEADLOCK, failed.failure().kind());
    }

    @Test
    void publicBatchFailureDoesNotExposeDriverValues() {
        String driverMessage = "Duplicate entry 'alice@example.com' for key 'users.email'";
        BatchChunkResult failed = BatchChunkResult.failed(
                0, 0, 1, new DriverException(driverMessage, "23000", 1062));

        assertEquals(RdbErrorKind.DUPLICATE_KEY, failed.failure().kind());
        assertEquals("database duplicate key conflict", failed.failure().message());
        assertFalse(failed.failure().message().contains("alice@example.com"));

        BatchChunkResult unknown = BatchChunkResult.failed(
                1, 1, 1, new IllegalStateException("password=secret"));
        assertEquals("database operation failed", unknown.failure().message());
        assertFalse(unknown.failure().message().contains("secret"));
    }

    /** 非标准 SQLState 可能含驱动诊断或任意长文本，公开批量摘要只能保留五位大写字母数字状态码。 */
    @Test
    void publicBatchFailureRejectsUnsafeSqlState() {
        String unsafeState = "SECRET\n" + "x".repeat(5_000);
        BatchChunkResult failed = BatchChunkResult.failed(
                0, 0, 1, new DriverException("driver failed", unsafeState, 7));
        BatchChunkResult.Failure direct = new BatchChunkResult.Failure(
                "driver.Error", "database operation failed", unsafeState, 7, RdbErrorKind.UNKNOWN);

        assertNull(failed.failure().sqlState());
        assertNull(direct.sqlState());
    }

    /**
     * 独立分片既有成功又有失败时，使用方必须看到部分成功。
     */
    @Test
    void summarizesIndependentFailuresAsPartial() {
        BatchChunkResult committed = BatchChunkResult.committed(0, 0, 2, 2);
        BatchChunkResult failed = BatchChunkResult.failed(1,
                                                          2,
                                                          2,
                                                          new IllegalStateException("duplicate key"));

        BatchWriteResult result = BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT,
                                                        List.of(committed, failed));

        assertEquals(BatchWriteResult.Status.PARTIAL, result.status());
        assertEquals(4, result.inputCount());
        assertEquals(2, result.affectedRows());
        assertEquals(List.of(committed, failed), result.chunks());
    }

    /**
     * 空输入没有分片，也没有影响行数，但整体是成功完成。
     */
    @Test
    void createsCommittedEmptyResult() {
        BatchWriteResult result = BatchWriteResult.empty(BatchWriteOptions.Mode.ATOMIC);

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(0, result.inputCount());
        assertEquals(0, result.affectedRows());
        assertEquals(List.of(), result.chunks());
    }

    /**
     * 外部事务尚未提交时只能报告已经加入事务，不能把执行行数当成已提交行数。
     */
    @Test
    void summarizesExternalTransactionWorkAsEnlisted() {
        BatchChunkResult first = enlisted(0, 0, 2);
        BatchChunkResult second = enlisted(1, 2, 1);

        BatchWriteResult result = BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC,
                                                        List.of(first, second));

        assertEquals(BatchWriteResult.Status.ENLISTED, result.status());
        assertEquals(3, result.inputCount());
        assertEquals(0, result.affectedRows());
    }

    private static BatchChunkResult enlisted(int chunkIndex, long startOffset, int inputCount) {
        return new BatchChunkResult(chunkIndex,
                                    startOffset,
                                    inputCount,
                                    0,
                                    BatchChunkResult.Status.ENLISTED,
                                    null,
                                    null,
                                    List.of());
    }

    /**
     * 原子事务失败后，成功执行过的分片也不能计入已提交行数。
     */
    @Test
    void summarizesAtomicRollbackAndKeepsResultOnException() {
        BatchChunkResult rolledBack = BatchChunkResult.rolledBack(0, 0, 2);
        BatchChunkResult failed = BatchChunkResult.failed(1,
                                                          2,
                                                          1,
                                                          new IllegalStateException("duplicate key"));
        BatchWriteResult result = BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC,
                                                        List.of(rolledBack, failed));
        BatchWriteException error = new BatchWriteException("atomic batch rolled back",
                                                            new IllegalStateException("duplicate key"),
                                                            result);

        assertEquals(BatchWriteResult.Status.ROLLED_BACK, result.status());
        assertEquals(0, result.affectedRows());
        assertSame(result, error.result());
    }

    @Test
    void summarizesConflictsWithoutCountingRolledBackRows() {
        BatchRowConflict conflict = BatchRowConflict.exactlyOne(3, 0);
        BatchChunkResult chunk = BatchChunkResult.conflicted(1, 2, 2, List.of(conflict));

        BatchWriteResult result = BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, List.of(chunk));

        assertEquals(BatchWriteResult.Status.ROLLED_BACK, result.status());
        assertEquals(0, result.affectedRows());
        assertEquals(1, result.conflictCount());
        assertEquals(List.of(conflict), result.conflicts());
        assertEquals(BatchRowConflict.Reason.NO_MATCH, conflict.reason());
    }

    /**
     * 提交结果不确定时保留恢复令牌，确认入口只能给出已提交或仍未知。
     */
    @Test
    void carriesRecoveryTokenForUnknownResult() {
        BatchChunkResult.RecoveryToken token = new BatchChunkResult.RecoveryToken("import-1",
                                                                                  0,
                                                                                  "flying_orm_batch_receipt",
                                                                                  "plan-hash",
                                                                                  "payload-hash",
                                                                                  2L,
                                                                                  2L);
        BatchChunkResult unknown = BatchChunkResult.unknown(0,
                                                            0,
                                                            2,
                                                            new IllegalStateException("commit response lost"),
                                                            token);
        BatchWriteResult result = BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, List.of(unknown));
        BatchResolution resolution = BatchResolution.committed(token);

        assertEquals(BatchWriteResult.Status.UNKNOWN, result.status());
        assertEquals(token, result.chunks().getFirst().recoveryToken());
        assertEquals(BatchResolution.Status.COMMITTED, resolution.status());
        assertThrows(IllegalArgumentException.class,
                     () -> new BatchChunkResult.RecoveryToken(" ",
                                                               0,
                                                               "receipt",
                                                               "plan",
                                                               "payload",
                                                               2L,
                                                               2L));

        BatchChunkResult unknownWithoutReceipt = BatchChunkResult.unknown(1,
                                                                          2,
                                                                          1,
                                                                          new IllegalStateException(
                                                                                  "commit response lost"));
        assertNull(unknownWithoutReceipt.recoveryToken());
    }

    @Test
    void rejectsContradictoryChunkResultState() {
        BatchChunkResult.Failure failure = new BatchChunkResult.Failure("driver.Error",
                                                                        "failed",
                                                                        "HY000",
                                                                        0,
                                                                        RdbErrorKind.UNKNOWN);

        assertThrows(IllegalArgumentException.class,
                     () -> new BatchChunkResult(0,
                                                0,
                                                1,
                                                1,
                                                 BatchChunkResult.Status.ROLLED_BACK,
                                                 null,
                                                 null,
                                                 List.of()));
        assertThrows(IllegalArgumentException.class,
                     () -> new BatchChunkResult(0,
                                                0,
                                                1,
                                                1,
                                                 BatchChunkResult.Status.COMMITTED,
                                                 failure,
                                                 null,
                                                 List.of()));
        assertThrows(IllegalArgumentException.class,
                     () -> new BatchChunkResult(0,
                                                0,
                                                1,
                                                0,
                                                 BatchChunkResult.Status.UNKNOWN,
                                                 null,
                                                 null,
                                                 List.of()));
    }

    /**
     * 分片偏移会用于恢复生成键和行级冲突位置；最后一行位置溢出时不能构造看似合法的结果。
     */
    @Test
    void rejectsChunkWhoseInputRangeWouldOverflow() {
        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                                                      () -> BatchChunkResult.committed(0,
                                                                                       Long.MAX_VALUE,
                                                                                       2,
                                                                                       0));

        assertEquals("batch chunk input range must not overflow", error.getMessage());
    }

    /** 用标准 R2DBC 异常字段验证分类，不依赖具体数据库驱动。 */
    private static final class DriverException extends R2dbcException {
        private DriverException(String message, String sqlState, int errorCode) {
            super(message, sqlState, errorCode);
        }
    }
}
