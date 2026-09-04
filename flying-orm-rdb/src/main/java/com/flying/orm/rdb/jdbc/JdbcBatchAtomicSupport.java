package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.execution.ProtectedBatchRows;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.TimeoutException;

import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.failure;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.readChunk;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.restoreInterrupt;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.rethrowVirtualMachineError;

/**
 * JDBC ATOMIC legacy 路径在借连接前使用的首片和截止时间准备。
 */
final class JdbcBatchAtomicSupport {

    private JdbcBatchAtomicSupport() {
    }

    static List<ProtectedBatchRows.RowView> readFirstOwnedChunk(
            JdbcBatchRows rows,
            BatchWriteRequest request) {
        JdbcBatchSupport.ChunkReadProgress progress = new JdbcBatchSupport.ChunkReadProgress();
        try {
            return readChunk(rows, request, 0L, 0,
                    JdbcBatchSupport.BatchDeadline.start(Duration.ZERO), progress);
        } catch (RuntimeException | Error | InterruptedException | TimeoutException error) {
            restoreInterrupt(error);
            rethrowVirtualMachineError(error);
            throw failure("jdbc atomic batch input failed", error,
                    failedBeforeTransaction(error, progress.acceptedRows()));
        }
    }

    static void requireAtomicDeadline(JdbcBatchSupport.BatchDeadline deadline, int firstInputCount) {
        try {
            deadline.remaining();
        } catch (RuntimeException | TimeoutException error) {
            rethrowVirtualMachineError(error);
            throw failure("jdbc atomic batch timed out before execution", error,
                    failedBeforeTransaction(error, firstInputCount));
        }
    }

    static BatchWriteResult failedBeforeTransaction(Throwable error, int inputCount) {
        return BatchWriteResult.from(
                BatchWriteOptions.Mode.ATOMIC,
                List.of(BatchChunkResult.failed(0, 0L, inputCount, error)));
    }
}
