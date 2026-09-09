package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.batch.BatchAffectedRows;
import com.flying.orm.rdb.batch.BatchChunkExecutionFact;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchCommitFact;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;

import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.readChunk;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.restoreInterrupt;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.rethrowTryWithResourcesVirtualMachineError;
import static com.flying.orm.rdb.jdbc.JdbcBatchSupport.rethrowVirtualMachineError;

/**
 * JDBC ATOMIC evidence 的外部事务参与和逐片执行证据编排，不决定事务终态。
 */
final class JdbcBatchEvidenceExecutor {

    private final JdbcConnectionProvider connections;
    private final JdbcBatchChunkExecutor chunks;

    JdbcBatchEvidenceExecutor(JdbcConnectionProvider connections, JdbcBatchChunkExecutor chunks) {
        this.connections = Objects.requireNonNull(connections, "jdbc connection provider must not be null");
        this.chunks = Objects.requireNonNull(chunks, "jdbc batch chunk executor must not be null");
    }

    BatchExecutionEvidence write(BatchWriteRequest request) {
        JdbcTransactionContext transaction;
        try {
            transaction = connections.currentTransaction().orElse(null);
        } catch (RuntimeException failure) {
            rethrowVirtualMachineError(failure);
            throw evidenceFailure("jdbc batch transaction resolution failed", failure,
                    BatchCommitFact.NOT_APPLICABLE,
                    List.of(failedEvidence(0, 0L, 0, failure)));
        }

        List<BatchChunkExecutionFact> facts = new ArrayList<>();
        JdbcConnectionProvider.JdbcConnectionLease acquired = null;
        int firstInputCount = 0;
        try {
            JdbcBatchRows rows = new JdbcBatchRows(
                    request.rows(), request.parameterCount(), request.options().maxRowBytes());
            try {
                if (transaction == null) {
                    JdbcBatchSupport.ChunkReadProgress progress = new JdbcBatchSupport.ChunkReadProgress();
                    try {
                        firstInputCount = readChunk(rows, request, 0L, 0, progress).size();
                    } catch (RuntimeException | InterruptedException | TimeoutException failure) {
                        restoreInterrupt(failure);
                        throw evidenceFailure("jdbc batch input failed", failure,
                                BatchCommitFact.NOT_APPLICABLE,
                                List.of(failedEvidence(0, 0L, progress.acceptedRows(), failure)));
                    }
                    if (firstInputCount > 0) {
                        throw new IllegalStateException("non-empty ATOMIC batch evidence requires an external jdbc transaction");
                    }
                } else {
                    acquired = JdbcConnectionProvider.JdbcConnectionLease.external(transaction);
                }
            } catch (RuntimeException | Error failure) {
                // 前置拒绝也必须取消已订阅输入，不借用或创建自有连接。
                try (rows) {
                    throw failure;
                }
            }
            if (acquired == null) {
                try (rows) {
                    return BatchExecutionEvidence.of(
                            BatchWriteOptions.Mode.ATOMIC,
                            BatchExecutionState.SUCCESS,
                            BatchCommitFact.NOT_APPLICABLE,
                            List.of());
                }
            }
            try (JdbcConnectionProvider.JdbcConnectionLease lease = acquired; rows) {
                return executeExternal(lease.connection(), request, rows, facts);
            }
        } catch (BatchExecutionEvidenceException failure) {
            rethrowTryWithResourcesVirtualMachineError(failure);
            throw failure;
        } catch (IllegalStateException failure) {
            rethrowTryWithResourcesVirtualMachineError(failure);
            throw failure;
        } catch (Exception failure) {
            rethrowTryWithResourcesVirtualMachineError(failure);
            if (facts.isEmpty()) {
                facts.add(failedEvidence(0, 0L, firstInputCount, failure));
            }
            throw evidenceFailure("jdbc batch execution failed", failure,
                    acquired == null ? BatchCommitFact.NOT_APPLICABLE : BatchCommitFact.UNKNOWN,
                    facts);
        } catch (Error failure) {
            rethrowTryWithResourcesVirtualMachineError(failure);
            throw failure;
        }
    }

    private BatchExecutionEvidence executeExternal(
            Connection connection,
            BatchWriteRequest request,
            JdbcBatchRows rows,
            List<BatchChunkExecutionFact> facts) {
        try {
            consume(connection, request, rows, facts);
            return BatchExecutionEvidence.of(
                    BatchWriteOptions.Mode.ATOMIC,
                    BatchExecutionState.SUCCESS,
                    facts.isEmpty() ? BatchCommitFact.NOT_APPLICABLE : BatchCommitFact.PENDING_EXTERNAL,
                    facts);
        } catch (EvidenceExecutionFailure failure) {
            throw evidenceFailure("external jdbc batch execution failed", failure.getCause(),
                    failure.databaseWorkAttempted
                            ? BatchCommitFact.PENDING_EXTERNAL : BatchCommitFact.NOT_APPLICABLE,
                    facts);
        }
    }

    private void consume(
            Connection connection,
            BatchWriteRequest request,
            JdbcBatchRows rows,
            List<BatchChunkExecutionFact> facts) {
        long offset = 0L;
        int chunkIndex = 0;
        boolean databaseWorkAttempted = false;
        JdbcBatchSupport.ChunkReadProgress readProgress = new JdbcBatchSupport.ChunkReadProgress();
        while (true) {
            List<ProtectedBatchRows.RowView> chunk;
            try {
                chunk = readChunk(rows, request, offset, chunkIndex, readProgress);
            } catch (RuntimeException | InterruptedException | TimeoutException failure) {
                restoreInterrupt(failure);
                facts.add(failedEvidence(
                        chunkIndex, offset, readProgress.acceptedRows(), failure));
                throw new EvidenceExecutionFailure(failure, databaseWorkAttempted);
            }
            if (chunk.isEmpty()) {
                return;
            }
            JdbcBatchEvidenceSupport.Outcome outcome = chunks.executeBatchEvidence(
                    connection, request, chunkIndex, offset, chunk);
            databaseWorkAttempted |= outcome.databaseWorkAttempted();
            facts.add(outcome.fact());
            if (!outcome.successful()) {
                throw new EvidenceExecutionFailure(outcome.failure(), databaseWorkAttempted);
            }
            offset += chunk.size();
            chunkIndex++;
        }
    }

    private static BatchChunkExecutionFact failedEvidence(
            int chunkIndex,
            long startOffset,
            int inputCount,
            Throwable failure) {
        BatchExecutionState state = JdbcBatchEvidenceSupport.failureState(failure, false);
        return BatchChunkExecutionFact.of(
                chunkIndex,
                startOffset,
                inputCount,
                List.of(),
                List.of(),
                state,
                BatchAffectedRows.unknown(),
                BatchChunkResult.Failure.from(failure));
    }

    private static BatchExecutionEvidenceException evidenceFailure(
            String message,
            Throwable failure,
            BatchCommitFact commitFact,
            List<BatchChunkExecutionFact> facts) {
        return new BatchExecutionEvidenceException(
                message,
                failure,
                BatchExecutionEvidence.of(
                        BatchWriteOptions.Mode.ATOMIC,
                        evidenceState(facts),
                        commitFact,
                        facts));
    }

    private static BatchExecutionState evidenceState(List<BatchChunkExecutionFact> facts) {
        if (facts.stream().allMatch(fact -> fact.state() == BatchExecutionState.SUCCESS)) {
            return BatchExecutionState.SUCCESS;
        }
        boolean hasSuccess = facts.stream().anyMatch(fact -> fact.state() == BatchExecutionState.SUCCESS
                || fact.successfulCount() > 0);
        if (hasSuccess) {
            return BatchExecutionState.PARTIAL;
        }
        return facts.isEmpty() ? BatchExecutionState.UNKNOWN : facts.getLast().state();
    }

    private static final class EvidenceExecutionFailure extends RuntimeException {

        private final boolean databaseWorkAttempted;

        private EvidenceExecutionFailure(Throwable cause, boolean databaseWorkAttempted) {
            super(cause);
            this.databaseWorkAttempted = databaseWorkAttempted;
        }
    }
}
