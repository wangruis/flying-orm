package com.flying.orm.rdb.jdbc;

import static com.flying.orm.core.internal.error.ThrowableGraph.findVirtualMachineError;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchOptimisticLockException;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteCompletion;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionObservers;
import com.flying.orm.rdb.observation.SqlTransactionSource;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;
import com.flying.orm.rdb.transaction.JdbcTransactionParticipant;
import com.flying.orm.rdb.transaction.TransactionOutcome;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.append;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.enlisted;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.externalFailure;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.failure;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.readChunk;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.rethrowTryWithResourcesVirtualMachineError;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.rethrowVirtualMachineError;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.restoreInterrupt;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.rolledBack;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.unknown;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.failedBeforeTransaction;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.readFirstChunk;

/**
 * 原生 JDBC 批量写入器。
 *
 * <p>ATOMIC 只参与上层外部事务，非空批次缺少事务时在业务 SQL 前拒绝；空批次保留无工作结果。
 * 执行后返回 ENLISTED，不替上层提交、回滚或关闭连接。INDEPENDENT 每个分片独立提交，当前稳定支持
 * concurrency=1；更高并发会在消费任何输入前拒绝，避免先承诺了配置、实际却偷偷串行。</p>
 *
 * @author wangr
 * @version v2.0.0
 */
public final class JdbcBatchWriter implements SyncBatchExecutor {

    private final DataSource dataSource;
    private final JdbcConnectionProvider connections;
    private final JdbcTransactionParticipant transactionParticipant;
    private final BatchExecutionObserver batchObserver;
    private final JdbcBatchExecutionObservationSupport observations;
    private final JdbcBatchChunkExecutor chunks = new JdbcBatchChunkExecutor();
    private final JdbcBatchEvidenceExecutor evidence;
    private final JdbcIndependentBatchExecutor independent;

    private JdbcBatchWriter(DataSource dataSource,
                            JdbcTransactionParticipant transactionParticipant,
                            BatchExecutionObserver batchObserver) {
        this.dataSource = Objects.requireNonNull(dataSource, "jdbc data source must not be null");
        this.transactionParticipant = Objects.requireNonNull(
                transactionParticipant, "jdbc transaction participant must not be null");
        this.batchObserver = batchObserver;
        SqlExecutionObserver cleanupObserver = batchObserver instanceof SqlExecutionObserver sqlObserver
                ? SqlExecutionObservers.safe(sqlObserver) : null;
        if (cleanupObserver != null && !cleanupObserver.enabled()) {
            cleanupObserver = null;
        }
        this.connections = new JdbcConnectionProvider(this.dataSource, transactionParticipant, cleanupObserver);
        this.observations = JdbcBatchExecutionObservationSupport.create(batchObserver, cleanupObserver);
        this.evidence = new JdbcBatchEvidenceExecutor(connections, chunks);
        this.independent = new JdbcIndependentBatchExecutor(connections, transactionParticipant, chunks);
    }

    /** 创建不参与外部事务的 JDBC 批量写入器。 */
    public static JdbcBatchWriter create(DataSource dataSource) {
        return new JdbcBatchWriter(dataSource, JdbcTransactionParticipant.none(), null);
    }

    /** 接入上层已经绑定的 JDBC 事务；参与者按调用读取当前事务，所以实例仍可安全复用。 */
    public JdbcBatchWriter withTransactionParticipant(JdbcTransactionParticipant participant) {
        return new JdbcBatchWriter(dataSource, participant, batchObserver);
    }

    /** 返回共享同一 DataSource 和事务参与者、但使用新批量 observer 的不可变执行器。 */
    public JdbcBatchWriter withBatchObserver(BatchExecutionObserver observer) {
        return new JdbcBatchWriter(dataSource, transactionParticipant,
                                   Objects.requireNonNull(observer, "batch observer must not be null"));
    }

    @Override
    public BatchWriteResult writeBatch(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = JdbcBatchSupport.requireSupportedRequest(request);
        JdbcBatchExecutionObservationSupport.BatchContext context = observations.begin(safeRequest);
        return execute(safeRequest, context);
    }

    /**
     * 执行批量并返回 SQL 执行证据。外部事务路径在最后一片执行完成后立即返回，
     * 不注册或等待事务 completion。
     */
    @Override
    public BatchExecutionEvidence writeBatchEvidence(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = JdbcBatchSupport.requireSupportedRequest(request);
        if (safeRequest.options().mode() != BatchWriteOptions.Mode.ATOMIC) {
            throw new UnsupportedOperationException("jdbc batch evidence currently requires ATOMIC mode");
        }
        JdbcBatchExecutionObservationSupport.BatchContext context = observations.begin(safeRequest);
        try {
            BatchExecutionEvidence executionEvidence = evidence.write(safeRequest);
            context.evidence(executionEvidence);
            return executionEvidence;
        } catch (BatchExecutionEvidenceException failure) {
            context.evidence(failure.evidence());
            throw failure;
        }
    }

    @Override
    public BatchExecutionEvidence writeProtectedBatchEvidence(BatchWriteRequest request) {
        return writeBatchEvidence(request);
    }

    @Override
    public BatchWriteResult writeProtectedBatch(BatchWriteRequest request) {
        return writeBatch(request);
    }

    @Override
    public List<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
        BatchWriteRequest safeRequest = JdbcBatchSupport.requireSupportedRequest(request);
        if (safeRequest.options().mode() != BatchWriteOptions.Mode.INDEPENDENT) {
            throw new IllegalArgumentException("jdbc batch chunk results require INDEPENDENT mode");
        }
        JdbcBatchExecutionObservationSupport.BatchContext context = observations.begin(safeRequest);
        return execute(safeRequest, context).chunks();
    }

    @Override
    public List<BatchChunkResult> writeProtectedBatchChunks(BatchWriteRequest request) {
        return writeBatchChunks(request);
    }

    private BatchWriteResult execute(BatchWriteRequest request,
                                     JdbcBatchExecutionObservationSupport.BatchContext context) {
        try {
            BatchWriteResult result = request.options().mode() == BatchWriteOptions.Mode.ATOMIC
                    ? writeAtomic(request, context) : independent.write(request, context);
            context.completed(result);
            return result;
        } catch (RuntimeException error) {
            context.failed(error);
            throw error;
        } catch (Error error) {
            context.failed(error);
            throw error;
        }
    }

    private BatchWriteResult writeAtomic(BatchWriteRequest request,
                                         JdbcBatchExecutionObservationSupport.BatchContext context) {
        List<BatchChunkResult> results = new ArrayList<>();
        JdbcTransactionContext transaction;
        try {
            // 外部事务按调用入口解析一次；不能在输入回调后重新选择连接归属。
            transaction = connections.currentTransaction().orElse(null);
        } catch (RuntimeException error) {
            rethrowVirtualMachineError(error);
            throw failure("jdbc atomic batch transaction resolution failed", error,
                          failedBeforeTransaction(error, 0));
        }
        JdbcConnectionProvider.JdbcConnectionLease acquired = null;
        int firstInputCount = 0;
        try {
            JdbcBatchRows rows = new JdbcBatchRows(
                    request.rows(), request.parameterCount(), request.options().maxRowBytes());
            try {
                if (transaction == null) {
                    firstInputCount = readFirstChunk(rows, request).size();
                    if (firstInputCount > 0) {
                        throw new IllegalStateException("non-empty ATOMIC batch requires an external jdbc transaction");
                    }
                } else {
                    acquired = JdbcConnectionProvider.JdbcConnectionLease.external(transaction);
                }
            } catch (RuntimeException | Error error) {
                // 尚未借出连接时也必须取消输入，并保留取消失败的 suppressed 诊断。
                try (rows) {
                    throw error;
                }
            }
            if (acquired == null) {
                try (rows) {
                    return BatchWriteResult.empty(BatchWriteOptions.Mode.ATOMIC);
                }
            }
            // 先取消输入，再释放借用租约；外部连接仍归事务拥有者管理。
            try (JdbcConnectionProvider.JdbcConnectionLease lease = acquired; rows) {
                context.transactionSource(SqlTransactionSource.EXTERNAL);
                try {
                    consumeAtomic(lease.connection(), request, rows, results);
                    return results.isEmpty()
                            ? BatchWriteResult.empty(BatchWriteOptions.Mode.ATOMIC)
                            : enlistExternal(lease.externalTransaction(), request, results, context);
                } catch (BatchWriteException error) {
                    throw externalFailure(error);
                } catch (SQLException | RuntimeException | Error error) {
                    restoreInterrupt(error);
                    if (error instanceof Error fatal) {
                        rethrowVirtualMachineError(fatal);
                    }
                    throw failure("external jdbc atomic batch failed", error,
                                  BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC,
                                                        unknown(results, error)));
                }
            }
        } catch (BatchWriteException error) {
            rethrowTryWithResourcesVirtualMachineError(error);
            throw error;
        } catch (IllegalArgumentException | IllegalStateException error) {
            rethrowTryWithResourcesVirtualMachineError(error);
            throw error;
        } catch (Exception error) {
            rethrowTryWithResourcesVirtualMachineError(error);
            throw failure("jdbc atomic batch failed", error,
                          acquired == null ? failedBeforeTransaction(error, firstInputCount)
                                  : BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, unknown(results, error)));
        } catch (Error error) {
            rethrowTryWithResourcesVirtualMachineError(error);
            throw error;
        }
    }

    private void consumeAtomic(Connection connection,
                               BatchWriteRequest request,
                               JdbcBatchRows rows,
                               List<BatchChunkResult> results) throws SQLException {
        long offset = 0L;
        int chunkIndex = 0;
        JdbcBatchSupport.ChunkReadProgress readProgress = new JdbcBatchSupport.ChunkReadProgress();
        while (true) {
            List<ProtectedBatchRows.RowView> chunk;
            try {
                chunk = readChunk(rows, request, offset, chunkIndex, readProgress);
            } catch (RuntimeException | InterruptedException | TimeoutException error) {
                restoreInterrupt(error);
                throw failure("jdbc atomic batch input failed", error, BatchWriteResult.from(
                        BatchWriteOptions.Mode.ATOMIC, append(results,
                                BatchChunkResult.failed(
                                        chunkIndex, offset, readProgress.acceptedRows(), error))));
            }
            if (chunk.isEmpty()) {
                break;
            }
            BatchChunkResult result;
            try {
                result = chunks.execute(connection, request, chunkIndex, offset, chunk);
            } catch (SQLException | RuntimeException | Error error) {
                if (error instanceof Error fatal) {
                    rethrowVirtualMachineError(fatal);
                }
                throw failure("jdbc atomic batch chunk failed", error, BatchWriteResult.from(
                        BatchWriteOptions.Mode.ATOMIC, append(results,
                                BatchChunkResult.failed(chunkIndex, offset, chunk.size(), error))));
            }
            if (result.status() == BatchChunkResult.Status.CONFLICTED) {
                throw failure("jdbc atomic batch optimistic lock conflict", new BatchOptimisticLockException(result.conflicts()),
                              BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, append(results, result)));
            }
            results.add(result);
            offset += chunk.size();
            chunkIndex++;
        }
    }

    /**
     * 把外部事务的 ENLISTED 结果接到最终通知，只保留受批量上限保护的结果快照。
     * 注册失败按 UNKNOWN 释放 Repository 暂存引用，不能改变 SQL 已加入事务的事实。
     */
    private static BatchWriteResult enlistExternal(JdbcTransactionContext transaction,
                                                   BatchWriteRequest request,
                                                   List<BatchChunkResult> executed,
                                                   JdbcBatchExecutionObservationSupport.BatchContext observation) {
        JdbcTransactionContext safeTransaction = Objects.requireNonNull(
                transaction, "jdbc transaction context must not be null");
        BatchWriteRequest safeRequest = Objects.requireNonNull(request, "batch write request must not be null");
        List<BatchChunkResult> snapshot = List.copyOf(Objects.requireNonNull(
                executed, "executed batch chunks must not be null"));
        BatchWriteResult result = BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, enlisted(snapshot));
        if (!registerExternalCompletion(safeTransaction, safeRequest.completion(), snapshot,
                      Objects.requireNonNull(observation, "batch observation context must not be null"))) {
            // 没有完成通知时，不能无限保留 Repository 的实体快照；UNKNOWN 明确告诉生命周期不要当成提交。
            completeExternalSynchronously(safeRequest.completion(), externalFinalResult(snapshot, TransactionOutcome.UNKNOWN));
        }
        return result;
    }

    private static boolean registerExternalCompletion(JdbcTransactionContext transaction,
                                                       BatchWriteCompletion completion,
                                                       List<BatchChunkResult> executed,
                                                       JdbcBatchExecutionObservationSupport.BatchContext observation) {
        AtomicBoolean notified = new AtomicBoolean();
        try {
            return transaction.completion().register(outcome -> {
                if (!notified.compareAndSet(false, true)) {
                    return BatchWriteCompletion.noop().afterCompletion(
                            externalFinalResult(executed, TransactionOutcome.UNKNOWN));
                }
                BatchWriteResult finalResult = externalFinalResult(executed, outcome);
                observation.finalized(finalResult);
                return Objects.requireNonNull(
                        completion.afterCompletion(finalResult),
                        "batch completion publisher must not be null");
            });
        } catch (RuntimeException failure) {
            VirtualMachineError fatal = findVirtualMachineError(failure);
            if (fatal != null) {
                throw fatal;
            }
            // 注册器是上层协作设施；它异常时不能把已经成功加入事务的 SQL 改成执行失败。
            return false;
        }
    }

    private static BatchWriteResult externalFinalResult(List<BatchChunkResult> executed, TransactionOutcome outcome) {
        TransactionOutcome safeOutcome = outcome == null ? TransactionOutcome.UNKNOWN : outcome;
        return switch (safeOutcome) {
            case COMMITTED -> BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, executed);
            case ROLLED_BACK -> BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, rolledBack(executed));
            case UNKNOWN -> BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, unknown(
                    executed, new IllegalStateException("external jdbc transaction outcome is unknown")));
        };
    }

    /** JDBC 无法登记外部事务完成通知时，仅调用明确的同步收尾钩子。 */
    private static void completeExternalSynchronously(BatchWriteCompletion completion, BatchWriteResult result) {
        try {
            completion.afterCompletionUnavailable(result);
        } catch (RuntimeException failure) {
            VirtualMachineError fatal = findVirtualMachineError(failure);
            if (fatal != null) {
                throw fatal;
            }
            // 清理协作失败不能覆盖 SQL 已经加入外部事务这一事实。
        }
    }

}
