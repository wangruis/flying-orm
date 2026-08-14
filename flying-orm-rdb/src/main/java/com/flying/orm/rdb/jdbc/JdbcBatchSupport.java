package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.execution.ProtectedBatchRows;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

/** JDBC 批量两个事务模式共用的分片边界、结果状态和连接收尾规则。 */
final class JdbcBatchSupport {

    private JdbcBatchSupport() {
    }

    /** JDBC 尚未实现事务回执时要在订阅输入和获取连接前明确拒绝，不能执行到一半才降级。 */
    static BatchWriteRequest requireSupportedRequest(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        if (safeRequest.options().recovery().mode() != BatchWriteOptions.RecoveryMode.NONE) {
            throw new UnsupportedOperationException("jdbc batch receipt recovery is not available yet");
        }
        return safeRequest;
    }

    static List<Object[]> readChunk(JdbcBatchRows rows,
                                    BatchWriteRequest request,
                                    long offset,
                                    int chunkIndex,
                                    BatchDeadline deadline) throws InterruptedException, TimeoutException {
        List<Object[]> result = new ArrayList<>(request.options().chunkSize());
        long bytes = 0L;
        while (result.size() < request.options().chunkSize()) {
            Object[] row = rows.next(deadline.remaining());
            if (row == null) {
                break;
            }
            // 先确认上游真的还有下一行，再判断是否需要创建新的结果分片。
            // 这样正好消费完 maxResultChunks 后的 EOF 会正常结束，不会被误判超限。
            if (chunkIndex >= request.options().maxResultChunks()) {
                throw new BatchMemoryLimitExceededException("result chunks", request.options().maxResultChunks(),
                                                            (long) chunkIndex + 1);
            }
            if (offset + result.size() >= request.options().maxRows()) {
                throw new BatchMemoryLimitExceededException("rows", request.options().maxRows(),
                                                            offset + result.size() + 1);
            }
            ProtectedBatchRows.work(row, request.parameterCount());
            bytes = saturatedAdd(bytes, ProtectedBatchRows.estimateRowBytes(row, request.parameterCount()));
            if (bytes == Long.MAX_VALUE || bytes > request.options().maxBufferedBytes()) {
                throw new BatchMemoryLimitExceededException("buffered bytes", request.options().maxBufferedBytes(), bytes);
            }
            // JdbcBatchRows 已在 onNext 当下取得整张参数图的独立所有权，分片直接接管该快照。
            result.add(row);
        }
        return result;
    }

    static List<BatchChunkResult> enlisted(List<BatchChunkResult> chunks) {
        return chunks.stream().map(chunk -> new BatchChunkResult(chunk.chunkIndex(), chunk.startOffset(),
                chunk.inputCount(), 0L, BatchChunkResult.Status.ENLISTED, null, null, List.of())).toList();
    }

    /**
     * 外部事务里已经成功执行的分片只能说 ENLISTED；失败分片是否已经被数据库部分执行，
     * 本层不能替事务管理器猜测，所以保守保留 UNKNOWN。
     */
    static BatchWriteException externalFailure(BatchWriteException error) {
        List<BatchChunkResult> external = error.result().chunks().stream().map(chunk ->
                chunk.status() == BatchChunkResult.Status.COMMITTED
                        ? new BatchChunkResult(chunk.chunkIndex(), chunk.startOffset(), chunk.inputCount(), 0L,
                                               BatchChunkResult.Status.ENLISTED, null, null, List.of())
                        : BatchChunkResult.unknown(chunk.chunkIndex(), chunk.startOffset(), chunk.inputCount(),
                                                   error.getCause())).toList();
        return failure(error.getMessage(), error.getCause(), BatchWriteResult.from(error.result().mode(), external));
    }

    static List<BatchChunkResult> rolledBack(List<BatchChunkResult> chunks) {
        if (chunks.isEmpty()) {
            return List.of(BatchChunkResult.rolledBack(0, 0L, 0));
        }
        return chunks.stream().map(chunk -> BatchChunkResult.rolledBack(chunk.chunkIndex(), chunk.startOffset(),
                chunk.inputCount())).toList();
    }

    static List<BatchChunkResult> rolledBackForAtomic(List<BatchChunkResult> chunks) {
        return chunks.stream().map(chunk -> chunk.status() == BatchChunkResult.Status.COMMITTED
                ? BatchChunkResult.rolledBack(chunk.chunkIndex(), chunk.startOffset(), chunk.inputCount()) : chunk).toList();
    }

    /** 回滚自身失败后不能再猜事务结果，整批已经接收的分片都按 UNKNOWN 返回。 */
    static List<BatchChunkResult> unknown(List<BatchChunkResult> chunks, Throwable error) {
        if (chunks.isEmpty()) {
            return List.of(BatchChunkResult.unknown(0, 0L, 0, error));
        }
        return chunks.stream().map(chunk -> BatchChunkResult.unknown(
                chunk.chunkIndex(), chunk.startOffset(), chunk.inputCount(), error)).toList();
    }

    static List<BatchChunkResult> append(List<BatchChunkResult> chunks, BatchChunkResult last) {
        List<BatchChunkResult> result = new ArrayList<>(chunks);
        result.add(last);
        return result;
    }

    static BatchWriteException unknownAfterCommitFailure(Throwable error,
                                                          BatchWriteRequest request,
                                                          List<BatchChunkResult> chunks) {
        // 空批次也已发出 commit；回执丢失时必须保留一个零行 UNKNOWN，不能让空列表按全称汇总成 COMMITTED。
        List<BatchChunkResult> unknown = chunks.isEmpty()
                ? List.of(BatchChunkResult.unknown(0, 0L, 0, error))
                : chunks.stream().map(chunk -> BatchChunkResult.unknown(
                        chunk.chunkIndex(), chunk.startOffset(), chunk.inputCount(), error)).toList();
        return failure("jdbc batch commit outcome is unknown", error,
                       BatchWriteResult.from(request.options().mode(), unknown));
    }

    static RollbackOutcome rollbackAfterFailure(Connection connection, Throwable error) {
        try {
            connection.rollback();
            return RollbackOutcome.succeeded();
        } catch (SQLException | RuntimeException | Error rollbackFailure) {
            return RollbackOutcome.failed(error, rollbackFailure);
        }
    }

    static RollbackOutcome rollbackQuietly(Connection connection) {
        return rollbackQuietly(connection, null);
    }

    static RollbackOutcome rollbackQuietly(Connection connection, Throwable error) {
        try {
            connection.rollback();
            return RollbackOutcome.succeeded();
        } catch (SQLException | RuntimeException | Error rollbackFailure) {
            return RollbackOutcome.failed(error, rollbackFailure);
            // 原异常决定本次失败类型；回滚异常不能覆盖已经要返回给上层的执行失败。
        }
    }

    static Throwable restoreAutoCommit(Connection connection) {
        try {
            connection.setAutoCommit(true);
            return null;
        } catch (SQLException | RuntimeException | Error failure) {
            return failure;
        }
    }

    static BatchWriteException failure(String message, Throwable error, BatchWriteResult result) {
        return new BatchWriteException(message, error, result);
    }

    static void restoreInterrupt(Throwable error) {
        if (error instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
    }

    /** 回滚和资源隔离完成后，VM 错误必须保持 JVM 原有的终止语义，不能包装为业务批处理失败。 */
    record RollbackOutcome(boolean confirmed, VirtualMachineError cleanupFatal) {

        static RollbackOutcome succeeded() {
            return new RollbackOutcome(true, null);
        }

        static RollbackOutcome failed(Throwable primary, Throwable rollbackFailure) {
            VirtualMachineError primaryFatal = JdbcThrowableGraph.findVirtualMachineError(primary);
            VirtualMachineError rollbackFatal = JdbcThrowableGraph.findVirtualMachineError(rollbackFailure);
            VirtualMachineError fatal = primaryFatal != null ? primaryFatal : rollbackFatal;
            if (fatal != null) {
                if (primary != null && primary != fatal) {
                    JdbcThrowableGraph.addSuppressedIfAcyclic(fatal, primary);
                }
                if (rollbackFailure != fatal) {
                    JdbcThrowableGraph.addSuppressedIfAcyclic(fatal, rollbackFailure);
                }
            } else if (primary != null && rollbackFailure != primary) {
                JdbcThrowableGraph.addSuppressedIfAcyclic(primary, rollbackFailure);
            }
            return new RollbackOutcome(false, fatal);
        }
    }

    static void rethrowVirtualMachineError(Error error) {
        if (error instanceof VirtualMachineError) {
            throw error;
        }
    }

    /** 保护已在主链路传播的 VM 错误；普通主失败则转为恢复 VM 错误的辅助上下文。 */
    static void rethrowRestoreVirtualMachineError(Throwable restoreFailure, Throwable operationFailure) {
        VirtualMachineError restoreFatal = JdbcThrowableGraph.findVirtualMachineError(restoreFailure);
        VirtualMachineError operationFatal = JdbcThrowableGraph.findVirtualMachineError(operationFailure);
        if (operationFatal != null) {
            if (restoreFailure != null && operationFatal != restoreFailure) {
                JdbcThrowableGraph.addSuppressedIfAcyclic(operationFatal, restoreFailure);
            }
            throw operationFatal;
        }
        if (restoreFatal == null) {
            return;
        }
        if (operationFailure != null && operationFailure != restoreFatal) {
            JdbcThrowableGraph.addSuppressedIfAcyclic(restoreFatal, operationFailure);
        }
        throw restoreFatal;
    }

    /** try-with-resources 会把清理阶段异常压到主异常上，VM 错误必须在业务包装前恢复为主失败。 */
    static void rethrowSuppressedVirtualMachineError(Throwable error) {
        VirtualMachineError fatal = JdbcThrowableGraph.findVirtualMachineError(error);
        if (fatal != null) {
            throw fatal;
        }
    }

    private static long saturatedAdd(long left, long right) {
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }

    /** 统一计算整批剩余时间，读取上游、绑定参数和提交事务共享同一个截止点。 */
    record BatchDeadline(long expiresAt) {
        static BatchDeadline start(Duration timeout) {
            if (timeout.isZero()) {
                return new BatchDeadline(Long.MAX_VALUE);
            }
            long timeoutNanos = saturatingNanos(timeout);
            if (timeoutNanos == Long.MAX_VALUE) {
                return new BatchDeadline(Long.MAX_VALUE);
            }
            long startedAt = System.nanoTime();
            return new BatchDeadline(startedAt > Long.MAX_VALUE - timeoutNanos
                    ? Long.MAX_VALUE : startedAt + timeoutNanos);
        }

        Duration remaining() throws TimeoutException {
            if (expiresAt == Long.MAX_VALUE) {
                return Duration.ZERO;
            }
            long nanos = expiresAt - System.nanoTime();
            if (nanos <= 0L) {
                throw new TimeoutException("jdbc batch timed out");
            }
            return Duration.ofNanos(nanos);
        }

        private static long saturatingNanos(Duration value) {
            try {
                return value.toNanos();
            } catch (ArithmeticException ignored) {
                return Long.MAX_VALUE;
            }
        }
    }
}
