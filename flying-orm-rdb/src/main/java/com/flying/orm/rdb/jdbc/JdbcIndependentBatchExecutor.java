package com.flying.orm.rdb.jdbc;

import static com.flying.orm.rdb.jdbc.JdbcFailureSupport.suppress;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.internal.batch.BatchChunkCompletion;
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
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.rethrowTryWithResourcesVirtualMachineError;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.rethrowVirtualMachineError;
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
        JdbcBatchSupport.ChunkReadProgress readProgress = new JdbcBatchSupport.ChunkReadProgress();
        boolean notifyingCompletion = false;
        try (JdbcBatchRows rows = new JdbcBatchRows(
                request.rows(), request.parameterCount(), request.options().maxRowBytes())) {
            long offset = 0L;
            int chunkIndex = 0;
            List<ProtectedBatchRows.RowView> chunk;
            while (true) {
                try {
                    chunk = readChunk(rows, request, offset, chunkIndex, readProgress);
                } catch (RuntimeException | Error | InterruptedException | TimeoutException error) {
                    rethrowVirtualMachineError(error);
                    restoreInterrupt(error);
                    throw failure("jdbc independent batch input failed", error,
                                  inputFailureResult(results, readProgress.acceptedRows(), error));
                }
                if (chunk.isEmpty()) {
                    break;
                }
                BatchChunkResult result;
                try {
                    result = executeChunk(request, chunkIndex, offset, chunk, context);
                } catch (BatchWriteException error) {
                    throw appendCompletedResults(results, error);
                }
                results.add(result);
                if (request.completion() instanceof BatchChunkCompletion completion) {
                    // executeChunk 已释放租约；POST 失败不能落入 SQL/输入失败的处理边界。
                    notifyingCompletion = true;
                    completion.afterChunk(result);
                    notifyingCompletion = false;
                }
                offset += chunk.size();
                chunkIndex++;
            }
            return BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT, results);
        } catch (JdbcExternalTransactionModeException error) {
            // 事务参与状态在预检后发生变化时仍要原样拒绝，不能包装成“批量已经执行失败”。
            rethrowTryWithResourcesVirtualMachineError(error);
            throw error;
        } catch (BatchWriteException error) {
            rethrowTryWithResourcesVirtualMachineError(error);
            throw error;
        } catch (IllegalArgumentException error) {
            rethrowTryWithResourcesVirtualMachineError(error);
            throw error;
        } catch (RuntimeException | Error error) {
            rethrowTryWithResourcesVirtualMachineError(error);
            if (notifyingCompletion) {
                throw error;
            }
            restoreInterrupt(error);
            throw failure("jdbc independent batch failed", error,
                          inputFailureResult(results, readProgress.acceptedRows(), error));
        }
    }

    /**
     * 输入流在下一片形成前终止时，补记当前分片已经完成快照的输入行。
     * 已完成分片保留原有位置和状态，不保留失败分片的参数值。
     */
    private static BatchWriteResult inputFailureResult(List<BatchChunkResult> completed,
                                                       int acceptedRows,
                                                       Throwable error) {
        BatchWriteResult completedResult = BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT, completed);
        List<BatchChunkResult> results = new ArrayList<>(completed);
        int nextChunkIndex = results.stream().mapToInt(BatchChunkResult::chunkIndex).max().orElse(-1) + 1;
        results.add(BatchChunkResult.failed(
                nextChunkIndex, completedResult.inputCount(), acceptedRows, error));
        return BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT, results);
    }

    /** 当前分片失败时，异常仍必须携带本次调用此前已经确认的分片事实。 */
    private static BatchWriteException appendCompletedResults(List<BatchChunkResult> completed,
                                                               BatchWriteException error) {
        if (completed.isEmpty()) {
            return error;
        }
        List<BatchChunkResult> merged = new ArrayList<>(completed.size() + error.result().chunks().size());
        merged.addAll(completed);
        merged.addAll(error.result().chunks());
        BatchWriteException combined = failure(error.getMessage(), error.getCause(),
                BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT, merged));
        for (Throwable suppressed : error.getSuppressed()) {
            suppress(combined, suppressed);
        }
        return combined;
    }

    private BatchChunkResult executeChunk(BatchWriteRequest request,
                                           int chunkIndex,
                                           long offset,
                                           List<ProtectedBatchRows.RowView> rows,
                                           JdbcBatchExecutionObservationSupport.BatchContext context) {
        BatchChunkResult outcome = null;
        try (JdbcConnectionProvider.JdbcConnectionLease lease = connections.acquire()) {
            context.transactionSource(lease.transactionSource() == SqlTransactionSource.EXTERNAL
                    ? SqlTransactionSource.EXTERNAL : SqlTransactionSource.INTERNAL);
            if (lease.transactionSource() == SqlTransactionSource.EXTERNAL) {
                throw new JdbcExternalTransactionModeException(
                        "INDEPENDENT batch cannot use an external jdbc transaction connection");
            }
            Connection connection = lease.connection();
            boolean transactionFinished = false;
            boolean commitAttempted = false;
            try {
                if (connection.getAutoCommit()) {
                    connection.setAutoCommit(false);
                }
                BatchChunkResult result = chunks.execute(connection, request, chunkIndex, offset, rows);
                if (result.status() == BatchChunkResult.Status.CONFLICTED) {
                    JdbcBatchSupport.RollbackOutcome rollback = rollbackQuietly(connection);
                    transactionFinished = rollback.confirmed();
                    if (transactionFinished) {
                        lease.markTransactionOutcomeConfirmed();
                        outcome = result;
                        return outcome;
                    }
                    SQLException uncertainty = new SQLException("jdbc batch conflict rollback failed");
                    if (rollback.cleanupFatal() != null) {
                        throw rollback.cleanupFatal();
                    }
                    lease.markTransactionOutcomeUnknown(uncertainty);
                    outcome = BatchChunkResult.unknown(chunkIndex, offset, rows.size(), uncertainty);
                    return outcome;
                }
                JdbcStatementControl.requireNotInterrupted();
                commitAttempted = true;
                connection.commit();
                outcome = result;
                transactionFinished = true;
                lease.markTransactionOutcomeConfirmed();
                return outcome;
            } catch (SQLException | RuntimeException | Error error) {
                if (commitAttempted) {
                    rethrowVirtualMachineError(error);
                    lease.markTransactionOutcomeUnknown(error);
                    outcome = BatchChunkResult.unknown(chunkIndex, offset, rows.size(), error);
                    return outcome;
                }
                JdbcBatchSupport.RollbackOutcome rollback = rollbackQuietly(connection, error);
                transactionFinished = rollback.confirmed();
                if (!transactionFinished) {
                    if (rollback.cleanupFatal() != null) {
                        throw rollback.cleanupFatal();
                    }
                }
                rethrowVirtualMachineError(error);
                if (transactionFinished) {
                    lease.markTransactionOutcomeConfirmed();
                } else {
                    lease.markTransactionOutcomeUnknown(error);
                }
                outcome = transactionFinished
                        ? BatchChunkResult.failed(chunkIndex, offset, rows.size(), error)
                        : BatchChunkResult.unknown(chunkIndex, offset, rows.size(), error);
                return outcome;
            }
        } catch (SQLException error) {
            rethrowTryWithResourcesVirtualMachineError(error);
            throw failure("jdbc independent batch chunk failed", error,
                          BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT, List.of(
                                  BatchChunkResult.failed(chunkIndex, offset, rows.size(), error))));
        } catch (RuntimeException | Error error) {
            rethrowTryWithResourcesVirtualMachineError(error);
            if (outcome != null) {
                throw failure("jdbc independent batch cleanup failed", error,
                              BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT, List.of(outcome)));
            }
            throw error;
        }
    }

    private void rejectExternalTransaction(JdbcBatchExecutionObservationSupport.BatchContext context) {
        if (transactionParticipant.currentTransaction().isPresent()) {
            context.transactionSource(SqlTransactionSource.EXTERNAL);
            throw new IllegalStateException("INDEPENDENT batch cannot participate in an external jdbc transaction");
        }
    }
}
