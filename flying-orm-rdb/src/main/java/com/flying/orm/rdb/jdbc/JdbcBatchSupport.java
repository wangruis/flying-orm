package com.flying.orm.rdb.jdbc;

import static com.flying.orm.rdb.jdbc.JdbcFailureSupport.directVirtualMachineError;
import static com.flying.orm.rdb.jdbc.JdbcFailureSupport.suppress;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.internal.DurationLimits;
import com.flying.orm.rdb.internal.plan.SqlExecutionStatements;

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

    /** 回执恢复是当前 R2DBC 执行能力；JDBC 必须在订阅输入和获取连接前拒绝，不能执行到一半才降级。 */
    static BatchWriteRequest requireSupportedRequest(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        if (safeRequest.options().recovery().mode() != BatchWriteOptions.RecoveryMode.NONE) {
            throw new UnsupportedOperationException("batch receipt recovery is supported only by the R2DBC executor");
        }
        SqlExecutionStatements.canonical(safeRequest.statement(), "");
        return safeRequest;
    }

    static List<ProtectedBatchRows.RowView> readChunk(JdbcBatchRows rows,
                                                      BatchWriteRequest request,
                                                      long offset,
                                                      int chunkIndex,
                                                      BatchDeadline deadline,
                                                      ChunkReadProgress progress) throws InterruptedException, TimeoutException {
        ChunkReadProgress safeProgress = Objects.requireNonNull(progress, "chunk read progress must not be null");
        safeProgress.reset();
        BatchWriteOptions options = request.options();
        List<ProtectedBatchRows.RowView> result = new ArrayList<>(Math.min(options.chunkSize(), 16));
        long maxBytesBeforeNextRow = options.maxBufferedBytes() / options.concurrency() - options.maxRowBytes();
        long bytes = 0L;
        // 未知下一行在 onNext 转移所有权之前，必须先留出其声明的单行上限。
        while (result.size() < options.chunkSize() && bytes <= maxBytesBeforeNextRow) {
            ProtectedBatchRows.RowView rowView = rows.nextRowView(deadline.remaining());
            if (rowView == null) {
                break;
            }
            // 先确认上游真的还有下一行，再判断是否需要创建新的结果分片。
            // 这样正好消费完 maxResultChunks 后的 EOF 会正常结束，不会被误判超限。
            if (chunkIndex >= request.options().maxResultChunks()) {
                throw new BatchMemoryLimitExceededException("result chunks", request.options().maxResultChunks(),
                                                            (long) chunkIndex + 1);
            }
            if (request.options().maxRows() > 0
                    && offset + result.size() >= request.options().maxRows()) {
                throw new BatchMemoryLimitExceededException("rows", request.options().maxRows(),
                                                            offset + result.size() + 1);
            }
            // 行权重已在输入边界校验不超过 maxRowBytes；上述预留保证加法不溢出且不超片预算。
            bytes += rowView.estimatedBytes();
            safeProgress.accepted();
            // 上游已经转移整行所有权，分片直接接管，不再复制参数图。
            result.add(rowView);
        }
        return result;
    }

    /** 记录当前分片已完成输入快照的行数；仅由同一个 JDBC 消费线程读写。 */
    static final class ChunkReadProgress {
        private int acceptedRows;

        void reset() {
            acceptedRows = 0;
        }

        void accepted() {
            acceptedRows++;
        }

        int acceptedRows() {
            return acceptedRows;
        }
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
            VirtualMachineError primaryFatal = directVirtualMachineError(primary);
            VirtualMachineError rollbackFatal = directVirtualMachineError(rollbackFailure);
            VirtualMachineError fatal = primaryFatal != null ? primaryFatal : rollbackFatal;
            if (fatal != null) {
                if (primary != null && primary != fatal) {
                    suppress(fatal, primary);
                }
                if (rollbackFailure != fatal) {
                    suppress(fatal, rollbackFailure);
                }
            } else if (primary != null && rollbackFailure != primary) {
                suppress(primary, rollbackFailure);
            }
            return new RollbackOutcome(false, fatal);
        }
    }

    /** 同步 JDBC 边界只提升当前直接抛出的 VM 错误，不扫描 cause 或 suppressed 图。 */
    static void rethrowVirtualMachineError(Throwable error) {
        VirtualMachineError fatal = directVirtualMachineError(error);
        if (fatal != null) {
            throw fatal;
        }
    }

    /**
     * Java try-with-resources 会把 close 失败直接放入主体异常的 suppressed 列表。
     * 这里只识别当前合并层，不解释任意 cause 或嵌套 suppressed 图。
     */
    static void rethrowTryWithResourcesVirtualMachineError(Throwable error) {
        rethrowVirtualMachineError(error);
        for (Throwable cleanupFailure : error.getSuppressed()) {
            if (cleanupFailure instanceof VirtualMachineError fatal) {
                throw fatal;
            }
        }
    }

    /** 统一计算整批剩余时间，读取上游、绑定参数和提交事务共享同一个截止点。 */
    record BatchDeadline(long expiresAt) {
        static BatchDeadline start(Duration timeout) {
            if (timeout.isZero()) {
                return new BatchDeadline(Long.MAX_VALUE);
            }
            long timeoutNanos = DurationLimits.nanos(timeout);
            if (timeoutNanos == Long.MAX_VALUE) {
                return new BatchDeadline(Long.MAX_VALUE);
            }
            long startedAt = System.nanoTime();
            return new BatchDeadline(DurationLimits.addSaturated(startedAt, timeoutNanos));
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

    }
}
