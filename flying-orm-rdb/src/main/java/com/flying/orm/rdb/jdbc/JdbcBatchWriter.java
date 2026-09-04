package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchWriteException;
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

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.append;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.externalFailure;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.failure;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.readChunk;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.rethrowTryWithResourcesVirtualMachineError;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.rethrowVirtualMachineError;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.restoreInterrupt;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.rollbackAfterFailure;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.rolledBack;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.rolledBackForAtomic;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.unknown;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.unknownAfterCommitFailure;
import static com.flying.orm.rdb.jdbc.JdbcBatchAtomicSupport.failedBeforeTransaction;
import static com.flying.orm.rdb.jdbc.JdbcBatchAtomicSupport.readFirstOwnedChunk;
import static com.flying.orm.rdb.jdbc.JdbcBatchAtomicSupport.requireAtomicDeadline;

/**
 * 原生 JDBC 批量写入器。
 *
 * <p>ATOMIC 使用一条自有连接完成整批事务；连接来自外部事务时只执行 SQL 并返回 ENLISTED，
 * 从不替上层提交、回滚或关闭连接。INDEPENDENT 每个分片独立提交，当前第一批稳定支持
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
    private final JdbcExternalBatchCompletion externalCompletion = new JdbcExternalBatchCompletion();

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
            throw failure("jdbc atomic batch connection acquisition failed", error,
                          failedBeforeTransaction(error, 0));
        }
        JdbcConnectionProvider.JdbcConnectionLease acquired = null;
        int firstInputCount = 0;
        try {
            JdbcBatchRows rows = new JdbcBatchRows(
                    request.rows(), request.parameterCount(), request.options().maxRowBytes());
            List<ProtectedBatchRows.RowView> firstChunk;
            try {
                firstChunk = transaction == null ? readFirstOwnedChunk(rows, request) : null;
                if (firstChunk != null) {
                    firstInputCount = firstChunk.size();
                }
                if (transaction != null || firstInputCount > 0) {
                    try {
                        acquired = transaction == null ? connections.acquireOwned()
                                : JdbcConnectionProvider.JdbcConnectionLease.external(transaction);
                    } catch (SQLException | RuntimeException error) {
                        rethrowVirtualMachineError(error);
                        throw failure("jdbc atomic batch connection acquisition failed", error,
                                      failedBeforeTransaction(error, firstInputCount));
                    }
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
            // 保持原收尾顺序：先取消输入，再归还自有连接；每条路径只关闭输入一次。
            try (JdbcConnectionProvider.JdbcConnectionLease lease = acquired; rows) {
                context.transactionSource(lease.transactionSource() == SqlTransactionSource.EXTERNAL
                        ? SqlTransactionSource.EXTERNAL : SqlTransactionSource.INTERNAL);
                JdbcBatchSupport.BatchDeadline deadline = JdbcBatchSupport.BatchDeadline.start(
                        request.options().timeout());
                requireAtomicDeadline(deadline, firstInputCount);
                if (lease.transactionSource() == SqlTransactionSource.EXTERNAL) {
                    try {
                        consumeAtomic(lease.connection(), request, rows, null, results, deadline);
                        return externalCompletion.enlist(lease.externalTransaction(), request, results, context);
                    } catch (BatchWriteException error) {
                        throw externalFailure(error);
                    } catch (SQLException | RuntimeException | Error | InterruptedException | TimeoutException error) {
                        restoreInterrupt(error);
                        if (error instanceof Error fatal) {
                            rethrowVirtualMachineError(fatal);
                        }
                        throw failure("external jdbc atomic batch failed", error,
                                      BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC,
                                                            unknown(results, error)));
                    }
                }
                return executeOwnedAtomic(lease, request, rows, firstChunk, results, deadline);
            }
        } catch (BatchWriteException error) {
            rethrowTryWithResourcesVirtualMachineError(error);
            throw error;
        } catch (IllegalArgumentException error) {
            rethrowTryWithResourcesVirtualMachineError(error);
            throw error;
        } catch (SQLException | RuntimeException error) {
            rethrowTryWithResourcesVirtualMachineError(error);
            throw failure("jdbc atomic batch failed", error,
                          acquired == null ? failedBeforeTransaction(error, firstInputCount)
                                  : BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, unknown(results, error)));
        } catch (Error error) {
            rethrowTryWithResourcesVirtualMachineError(error);
            throw error;
        }
    }

    private BatchWriteResult executeOwnedAtomic(JdbcConnectionProvider.JdbcConnectionLease lease,
                                                 BatchWriteRequest request,
                                                 JdbcBatchRows rows,
                                                 List<ProtectedBatchRows.RowView> firstChunk,
                                                 List<BatchChunkResult> results,
                                                 JdbcBatchSupport.BatchDeadline deadline) {
        Connection connection = lease.connection();
        int firstInputCount = firstChunk.size();
        boolean commitAttempted = false;
        try {
            if (connection.getAutoCommit()) {
                connection.setAutoCommit(false);
            }
            consumeAtomic(connection, request, rows, firstChunk, results, deadline);
            deadline.remaining();
            JdbcStatementControl.requireNotInterrupted();
            try {
                commitAttempted = true;
                connection.commit();
                lease.markTransactionOutcomeConfirmed();
            } catch (SQLException | RuntimeException | Error commitFailure) {
                if (commitFailure instanceof VirtualMachineError fatal) {
                    rethrowVirtualMachineError(fatal);
                }
                throw unknownAfterCommitFailure(commitFailure, request, results);
            }
            return BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, results);
        } catch (BatchWriteException error) {
            if (error.result().status() == BatchWriteResult.Status.UNKNOWN) {
                throw error;
            }
            JdbcBatchSupport.RollbackOutcome rollback = rollbackAfterFailure(connection, error);
            boolean rollbackConfirmed = rollback.confirmed();
            if (rollbackConfirmed) {
                lease.markTransactionOutcomeConfirmedWithPrimaryFailure();
            }
            if (!rollbackConfirmed) {
                if (rollback.cleanupFatal() != null) {
                    throw rollback.cleanupFatal();
                }
            }
            List<BatchChunkResult> finalChunks = rollbackConfirmed
                    ? rolledBackForAtomic(error.result().chunks())
                    : unknown(error.result().chunks(), error);
            BatchWriteException translated = failure(error.getMessage(), error.getCause(), BatchWriteResult.from(
                    BatchWriteOptions.Mode.ATOMIC, finalChunks));
            throw translated;
        } catch (SQLException | RuntimeException | Error | InterruptedException | TimeoutException error) {
            if (commitAttempted) {
                if (error instanceof Error fatal) {
                    rethrowVirtualMachineError(fatal);
                }
                BatchWriteException translated = unknownAfterCommitFailure(error, request, results);
                throw translated;
            }
            JdbcBatchSupport.RollbackOutcome rollback = rollbackAfterFailure(connection, error);
            boolean rollbackConfirmed = rollback.confirmed();
            if (rollbackConfirmed) {
                lease.markTransactionOutcomeConfirmedWithPrimaryFailure();
            }
            if (!rollbackConfirmed) {
                if (rollback.cleanupFatal() != null) {
                    throw rollback.cleanupFatal();
                }
            }
            restoreInterrupt(error);
            if (error instanceof Error fatal) {
                rethrowVirtualMachineError(fatal);
            }
            List<BatchChunkResult> accepted = results.isEmpty()
                    ? List.of(BatchChunkResult.failed(0, 0L, firstInputCount, error)) : results;
            List<BatchChunkResult> finalChunks = rollbackConfirmed
                    ? rolledBack(accepted) : unknown(accepted, error);
            BatchWriteException translated = failure("jdbc atomic batch failed", error,
                    BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, finalChunks));
            throw translated;
        }
    }

    private void consumeAtomic(Connection connection,
                               BatchWriteRequest request,
                               JdbcBatchRows rows,
                               List<ProtectedBatchRows.RowView> chunk,
                               List<BatchChunkResult> results,
                               JdbcBatchSupport.BatchDeadline deadline)
            throws SQLException, InterruptedException, TimeoutException {
        long offset = 0L;
        int chunkIndex = 0;
        boolean prefetched = chunk != null;
        JdbcBatchSupport.ChunkReadProgress readProgress = new JdbcBatchSupport.ChunkReadProgress();
        while (true) {
            if (chunk == null) {
                try {
                    chunk = readChunk(rows, request, offset, chunkIndex, deadline, readProgress);
                } catch (RuntimeException | InterruptedException | TimeoutException error) {
                    throw failure("jdbc atomic batch input failed", error, BatchWriteResult.from(
                            BatchWriteOptions.Mode.ATOMIC, append(results,
                                    BatchChunkResult.failed(
                                            chunkIndex, offset, readProgress.acceptedRows(), error))));
                }
            }
            if (chunk.isEmpty()) {
                break;
            }
            BatchChunkResult result;
            try {
                result = chunks.execute(connection, request, chunkIndex, offset, chunk, deadline);
            } catch (SQLException | RuntimeException | Error | TimeoutException error) {
                if (error instanceof Error fatal) {
                    rethrowVirtualMachineError(fatal);
                }
                throw failure("jdbc atomic batch chunk failed", error, BatchWriteResult.from(
                        BatchWriteOptions.Mode.ATOMIC, append(results,
                                BatchChunkResult.failed(chunkIndex, offset, chunk.size(), error))));
            }
            if (result.status() == BatchChunkResult.Status.CONFLICTED) {
                throw failure("jdbc atomic batch optimistic lock conflict", new IllegalStateException("batch row count conflict"),
                              BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, append(results, result)));
            }
            results.add(result);
            offset += chunk.size();
            chunkIndex++;
            if (prefetched) {
                // 首片列表仍被调用栈引用；执行完即释放其参数，下一片不能与首片同时占用输入预算。
                chunk.clear();
                prefetched = false;
            }
            chunk = null;
        }
    }

}
