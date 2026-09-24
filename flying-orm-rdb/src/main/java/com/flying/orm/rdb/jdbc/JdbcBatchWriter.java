package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.internal.plan.SqlExecutionStatements;
import com.flying.orm.rdb.lifecycle.EntityPostWriteException;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.LongConsumer;

/**
 * 原生 JDBC 普通批量：一个有界顺序核心、一条上层连接及累计 SQL 事实。
 * @author wangr
 * @version v4.1.0
 */
public final class JdbcBatchWriter implements SyncBatchExecutor {
    private final JdbcConnectionAccess connections;
    private final RdbDialect dialect;
    private final JdbcBatchExecutionObservationSupport observations;
    private final JdbcExecutionObservationSupport cleanupObservations;
    private final JdbcBatchChunkExecutor executor = new JdbcBatchChunkExecutor();

    private JdbcBatchWriter(JdbcConnectionAccess connections, RdbDialect dialect, BatchExecutionObserver observer) {
        this.connections = Objects.requireNonNull(connections, "jdbc connection access must not be null");
        this.dialect = Objects.requireNonNull(dialect, "RDB dialect must not be null");
        this.observations = JdbcBatchExecutionObservationSupport.create(observer);
        this.cleanupObservations = JdbcExecutionObservationSupport.create(
                observer instanceof SqlExecutionObserver sql ? sql : SqlExecutionObserver.noop());
    }

    public static JdbcBatchWriter create(JdbcConnectionAccess connections, RdbDialect dialect) {
        return new JdbcBatchWriter(connections, dialect, null);
    }

    public JdbcBatchWriter withBatchObserver(BatchExecutionObserver observer) {
        return new JdbcBatchWriter(connections, dialect, Objects.requireNonNull(observer, "batch observer must not be null"));
    }

    @Override
    public BatchExecutionEvidence writeProtectedBatch(BatchWriteRequest request, LongConsumer rowCompleted) {
        return writeBatch(request, rowCompleted);
    }

    @Override
    public BatchExecutionEvidence writeBatch(BatchWriteRequest request, LongConsumer rowCompleted) {
        Objects.requireNonNull(request, "batch write request must not be null");
        SqlExecutionStatements.canonical(request.statement(), dialect.name());
        var observation = observations.begin(request);
        var evidence = new BatchExecutionEvidence.Accumulator();
        JdbcBatchRows input = new JdbcBatchRows(
                request.rows(), request.parameterCount(), request.options().maxRowBytes(),
                request.options().maxRows(), evidence);
        Connection connection = null;
        SqlRequest representative = null;
        Throwable failure = null;
        boolean attempted = false;
        boolean completed = false;
        try {
            long offset = 0;
            while (true) {
                List<ProtectedBatchRows.RowView> buffer = JdbcBatchSupport.readBuffer(input, request);
                if (buffer.isEmpty()) break;
                if (connection == null) {
                    var first = buffer.getFirst();
                    representative = first.work() != null
                            && first.work().kind() == com.flying.orm.rdb.execution.ProtectedWriteWork.Kind.UPDATE
                            ? first.work().ownerQuery()
                            : new SqlRequest(request.sql(), Arrays.asList(first.row()).subList(0, first.parameterCount()),
                                             request.bindMarkerStyle());
                    connection = Objects.requireNonNull(connections.getConnection(representative),
                            "jdbc connection must not be null");
                }
                // An UPDATE owner predicate may depend on a preceding row's business or token write.
                // Keep those rows as complete execution units; input buffering remains bounded.
                boolean sequential = buffer.size() > 1 && buffer.stream().anyMatch(row -> row.work() != null
                        && row.work().kind() == com.flying.orm.rdb.execution.ProtectedWriteWork.Kind.UPDATE);
                for (int from = 0; from < buffer.size();) {
                    List<ProtectedBatchRows.RowView> window = sequential ? buffer.subList(from, from + 1) : buffer;
                    var counts = new JdbcBatchEvidenceSupport.Counts(offset, window.size(), request.rowCountPolicy());
                    boolean successful = false;
                    Throwable executionFailure = null;
                    try {
                        executor.execute(connection, request, offset, window, counts);
                        successful = true;
                    } catch (Throwable error) {
                        executionFailure = error;
                    } finally {
                        attempted |= counts.databaseWorkAttempted();
                        counts.finish(evidence, successful);
                    }
                    if (executionFailure != null) {
                        JdbcBatchSupport.rethrowTryWithResourcesVirtualMachineError(executionFailure);
                        try { completeRows(rowCompleted, offset, window.size(), counts); }
                        catch (RuntimeException callback) {
                            com.flying.orm.core.internal.error.ThrowableGraph.addSuppressedIfAcyclic(executionFailure, callback);
                        }
                        throw executionFailure;
                    }
                    if (observation.enabled()) observation.progress(evidence.snapshot(BatchExecutionState.SUCCESS, null));
                    completeRows(rowCompleted, offset, window.size(), null);
                    offset = Math.addExact(offset, window.size());
                    from += window.size();
                }
            }
            completed = true;
        } catch (Throwable error) {
            JdbcBatchSupport.restoreInterrupt(error);
            failure = error;
        } finally {
            try {
                JdbcResources.close(SqlExecutionOperation.BATCH_WRITE, completed, failure, cleanupObservations,
                        connections, connection, representative, input);
            } catch (Throwable cleanup) {
                failure = cleanup;
            }
        }
        if (failure == null) {
            BatchExecutionEvidence result = evidence.snapshot(BatchExecutionState.SUCCESS, null);
            observation.completed(result);
            return result;
        }
        JdbcBatchSupport.rethrowTryWithResourcesVirtualMachineError(failure);
        BatchExecutionEvidence before = evidence.snapshot(BatchExecutionState.UNKNOWN, null);
        BatchExecutionEvidence result = evidence.snapshot(
                JdbcBatchEvidenceSupport.failureState(failure, before.successfulCount() > 0, attempted),
                BatchExecutionEvidence.Failure.from(failure));
        observation.completed(result);
        if (failure instanceof EntityPostWriteException post) {
            com.flying.orm.core.internal.error.ThrowableGraph.addSuppressedIfAcyclic(post,
                    new BatchExecutionEvidenceException("jdbc batch SQL execution evidence", null, result));
            throw post;
        }
        if (failure instanceof Error error) throw error;
        throw new BatchExecutionEvidenceException("jdbc batch execution failed", failure, result);
    }

    private static void completeRows(LongConsumer callback, long offset, int count,
                                     JdbcBatchEvidenceSupport.Counts partial) {
        if (callback == null) return;
        RuntimeException failure = null;
        for (int i = 0; i < count; i++) {
            if (partial != null && !partial.isCompletedRow(i)) continue;
            try {
                callback.accept(offset + i);
            } catch (RuntimeException error) {
                if (failure == null) failure = error;
                else com.flying.orm.core.internal.error.ThrowableGraph.addSuppressedIfAcyclic(failure, error);
            }
        }
        if (failure != null) throw failure;
    }
}
