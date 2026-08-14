package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.observation.SqlTransactionSource;
import com.flying.orm.rdb.transaction.JdbcTransactionParticipant;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.failure;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.readChunk;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.rethrowRestoreVirtualMachineError;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.rethrowSuppressedVirtualMachineError;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.restoreAutoCommit;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.restoreInterrupt;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.rollbackQuietly;

/**
 * JDBC INDEPENDENT 批量的分片事务编排器。
 *
 * <p>每个分片独占一条自有连接并完成一次提交或回滚；外部事务在消费输入前拒绝，
 * 在预检后的连接获取竞态中再次拒绝。该类型无可变共享状态，可安全供批量写入器复用。</p>
 *
 * @author wangr
 * @since 2026-08-08
 * @version v1.0
 */
final class JdbcIndependentBatchExecutor {

    private final JdbcConnectionProvider connections;
    private final JdbcTransactionParticipant transactionParticipant;
    private final JdbcBatchChunkExecutor chunks;

    JdbcIndependentBatchExecutor(JdbcConnectionProvider connections,
                                 JdbcTransactionParticipant transactionParticipant,
                                 JdbcBatchChunkExecutor chunks) {
        this.connections = Objects.requireNonNull(connections, "jdbc connections must not be null");
        this.transactionParticipant = Objects.requireNonNull(
                transactionParticipant, "jdbc transaction participant must not be null");
        this.chunks = Objects.requireNonNull(chunks, "jdbc batch chunks must not be null");
    }

    BatchWriteResult write(BatchWriteRequest request,
                           JdbcBatchExecutionObservationSupport.BatchContext context) {
        rejectExternalTransaction(context);
        if (request.options().concurrency() != 1) {
            throw new IllegalArgumentException("jdbc independent batch currently supports concurrency=1");
        }
        List<BatchChunkResult> results = new ArrayList<>();
        JdbcBatchSupport.BatchDeadline deadline = JdbcBatchSupport.BatchDeadline.start(request.options().timeout());
        try (JdbcBatchRows rows = new JdbcBatchRows(
                request.rows(), request.parameterCount(), request.options().maxBufferedBytes())) {
            long offset = 0L;
            int chunkIndex = 0;
            List<Object[]> chunk;
            while (!(chunk = readChunk(rows, request, offset, chunkIndex, deadline)).isEmpty()) {
                results.add(executeChunk(request, chunkIndex, offset, chunk, deadline, context));
                offset += chunk.size();
                chunkIndex++;
            }
            return BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT, results);
        } catch (JdbcExternalTransactionModeException error) {
            // 事务参与状态在预检后发生变化时仍要原样拒绝，不能包装成“批量已经执行失败”。
            rethrowSuppressedVirtualMachineError(error);
            throw error;
        } catch (RuntimeException | Error | InterruptedException | TimeoutException error) {
            rethrowSuppressedVirtualMachineError(error);
            restoreInterrupt(error);
            throw failure("jdbc independent batch failed", error,
                          inputFailureResult(results, error));
        }
    }

    /**
     * 输入流在下一片形成前终止时，补一个零输入失败片以避免空列表按全称量词被汇总为 COMMITTED。
     * 已完成分片保留原有位置和状态，失败片仅表达尚未形成的下一个输入边界。
     */
    private static BatchWriteResult inputFailureResult(List<BatchChunkResult> completed, Throwable error) {
        BatchWriteResult completedResult = BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT, completed);
        List<BatchChunkResult> results = new ArrayList<>(completed);
        int nextChunkIndex = results.stream().mapToInt(BatchChunkResult::chunkIndex).max().orElse(-1) + 1;
        results.add(BatchChunkResult.failed(nextChunkIndex, completedResult.inputCount(), 0, error));
        return BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT, results);
    }

    private BatchChunkResult executeChunk(BatchWriteRequest request,
                                          int chunkIndex,
                                          long offset,
                                          List<Object[]> rows,
                                          JdbcBatchSupport.BatchDeadline deadline,
                                          JdbcBatchExecutionObservationSupport.BatchContext context)
            throws TimeoutException {
        try (JdbcConnectionProvider.JdbcConnectionLease lease = connections.acquire()) {
            context.transactionSource(lease.transactionSource() == SqlTransactionSource.EXTERNAL
                    ? SqlTransactionSource.EXTERNAL : SqlTransactionSource.INTERNAL);
            if (lease.transactionSource() == SqlTransactionSource.EXTERNAL) {
                throw new JdbcExternalTransactionModeException(
                        "INDEPENDENT batch cannot use an external jdbc transaction connection");
            }
            Connection connection = lease.connection();
            boolean restoreAutoCommit = false;
            boolean autoCommitStateKnown = false;
            boolean transactionFinished = false;
            boolean commitAttempted = false;
            Throwable operationFailure = null;
            try {
                restoreAutoCommit = connection.getAutoCommit();
                autoCommitStateKnown = true;
                if (restoreAutoCommit) {
                    connection.setAutoCommit(false);
                }
                BatchChunkResult result = chunks.execute(connection, request, chunkIndex, offset, rows, deadline);
                if (result.status() == BatchChunkResult.Status.CONFLICTED) {
                    JdbcBatchSupport.RollbackOutcome rollback = rollbackQuietly(connection);
                    transactionFinished = rollback.confirmed();
                    if (transactionFinished) {
                        lease.markTransactionOutcomeConfirmed();
                        return result;
                    }
                    SQLException uncertainty = new SQLException("jdbc batch conflict rollback failed");
                    lease.discardAfterUncertainTransaction(uncertainty);
                    if (rollback.cleanupFatal() != null) {
                        operationFailure = rollback.cleanupFatal();
                        throw rollback.cleanupFatal();
                    }
                    return BatchChunkResult.unknown(chunkIndex, offset, rows.size(), uncertainty);
                }
                commitAttempted = true;
                connection.commit();
                transactionFinished = true;
                lease.markTransactionOutcomeConfirmed();
                return result;
            } catch (SQLException | RuntimeException | Error | TimeoutException error) {
                operationFailure = error;
                if (commitAttempted) {
                    lease.discardAfterUncertainTransaction(error);
                    rethrowSuppressedVirtualMachineError(error);
                    return BatchChunkResult.unknown(chunkIndex, offset, rows.size(), error);
                }
                JdbcBatchSupport.RollbackOutcome rollback = rollbackQuietly(connection, error);
                transactionFinished = rollback.confirmed();
                if (!autoCommitStateKnown || !transactionFinished) {
                    lease.discardAfterUncertainTransaction(error);
                }
                if (!transactionFinished) {
                    if (rollback.cleanupFatal() != null) {
                        operationFailure = rollback.cleanupFatal();
                        throw rollback.cleanupFatal();
                    }
                }
                rethrowSuppressedVirtualMachineError(error);
                if (transactionFinished) {
                    lease.markTransactionOutcomeConfirmed();
                }
                return transactionFinished
                        ? BatchChunkResult.failed(chunkIndex, offset, rows.size(), error)
                        : BatchChunkResult.unknown(chunkIndex, offset, rows.size(), error);
            } finally {
                if (restoreAutoCommit && transactionFinished) {
                    Throwable resetFailure = restoreAutoCommit(connection);
                    if (resetFailure != null) {
                        lease.discardAfterUncertainTransaction(resetFailure);
                        rethrowRestoreVirtualMachineError(resetFailure, operationFailure);
                        if (operationFailure != null && operationFailure != resetFailure) {
                            JdbcThrowableGraph.addSuppressedIfAcyclic(operationFailure, resetFailure);
                        }
                    }
                }
            }
        } catch (SQLException error) {
            rethrowSuppressedVirtualMachineError(error);
            throw failure("jdbc independent batch chunk failed", error,
                          BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT, List.of(
                                  BatchChunkResult.failed(chunkIndex, offset, rows.size(), error))));
        }
    }

    private void rejectExternalTransaction(JdbcBatchExecutionObservationSupport.BatchContext context) {
        if (transactionParticipant.currentTransactionForExecution().isPresent()) {
            context.transactionSource(SqlTransactionSource.EXTERNAL);
            throw new IllegalStateException("INDEPENDENT batch cannot participate in an external jdbc transaction");
        }
    }
}
