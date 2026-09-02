package com.flying.orm.rdb.observation;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchResolution;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;

import java.time.Duration;
import java.util.Objects;

/**
 * 批量写入观测事件。
 *
 * <p>分片、汇总和恢复是三种不同事实，因此分别建模。调用者不再需要用 {@code null} 和哨兵值判断
 * 哪些字段对当前事件有效，观测对象也不会保存批量参数或 rows Publisher。</p>
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public sealed interface BatchExecutionObservation
        permits BatchExecutionObservation.Chunk,
                BatchExecutionObservation.Summary,
                BatchExecutionObservation.Recovery {

    int NO_CHUNK = -1;

    BatchExecutionEventType eventType();

    SqlExecutionBackend backend();

    SqlStatementType statementType();

    String sql();

    long durationNanos();

    SqlFailureCategory failureCategory();

    BatchChunkResult.Failure failure();

    BatchChunkResult.RecoveryToken recoveryToken();

    default Duration duration() {
        return Duration.ofNanos(durationNanos());
    }

    SqlExecutionResultKind resultKind();

    /** 根据一个已完成分片创建事件，保留输入偏移和可能的冲突/恢复信息。 */
    static BatchExecutionObservation chunk(BatchWriteRequestView request,
                                           BatchChunkResult chunk,
                                           long durationNanos) {
        return BatchExecutionObservationFactory.chunk(request, chunk, durationNanos);
    }

    /** 根据完整批量结果创建汇总事件，失败分类取冲突或首个结构化失败。 */
    static BatchExecutionObservation summary(BatchWriteRequestView request,
                                             BatchWriteResult result,
                                             long durationNanos) {
        return BatchExecutionObservationFactory.summary(request, result, durationNanos);
    }

    /** 流式入口直接接收增量汇总值，不为汇总日志保存全部分片。 */
    static BatchExecutionObservation summary(BatchWriteRequestView request,
                                             BatchWriteResult.Status status,
                                             long inputCount,
                                             long affectedRows,
                                             long conflictCount,
                                             BatchChunkResult.Failure firstFailure,
                                             BatchChunkResult.RecoveryToken firstRecoveryToken,
                                             long durationNanos) {
        return BatchExecutionObservationFactory.summary(
                request,
                new BatchSummaryMetrics(status, inputCount, affectedRows, conflictCount,
                                        0, 0, 0, firstFailure, firstRecoveryToken),
                durationNanos);
    }

    /** 流式批量的完整汇总入口，分片计数由订阅内固定大小累加器提供。 */
    static BatchExecutionObservation summary(BatchWriteRequestView request,
                                             BatchWriteResult.Status status,
                                             long inputCount,
                                             long affectedRows,
                                             long conflictCount,
                                             long chunkCount,
                                             long successfulChunkCount,
                                             long failedChunkCount,
                                             BatchChunkResult.Failure firstFailure,
                                             BatchChunkResult.RecoveryToken firstRecoveryToken,
                                             long durationNanos) {
        return BatchExecutionObservationFactory.summary(
                request,
                new BatchSummaryMetrics(status, inputCount, affectedRows, conflictCount,
                                        chunkCount, successfulChunkCount, failedChunkCount,
                                        firstFailure, firstRecoveryToken),
                durationNanos);
    }

    /** 执行链在产生结构化结果前失败时创建保守的 UNKNOWN 汇总事件。 */
    static BatchExecutionObservation failedSummary(BatchWriteRequestView request,
                                                   long durationNanos,
                                                   Throwable error) {
        return BatchExecutionObservationFactory.failedSummary(request, durationNanos, error);
    }

    /** 创建恢复查询事件；恢复本身失败时同时保留异常分类。 */
    static BatchExecutionObservation recovery(BatchResolution resolution,
                                              long durationNanos,
                                              Throwable error) {
        return BatchExecutionObservationFactory.recovery(resolution, durationNanos, error);
    }

    /** 单个批量分片的执行事实。 */
    record Chunk(BatchWriteRequestView request,
                 BatchChunkResult.Status status,
                 int chunkIndex,
                 long startOffset,
                 long inputCount,
                 long affectedRows,
                 long durationNanos,
                 SqlFailureCategory failureCategory,
                 BatchChunkResult.Failure failure,
                 BatchChunkResult.RecoveryToken recoveryToken) implements BatchExecutionObservation {

        public Chunk {
            request = Objects.requireNonNull(request, "batch request view must not be null");
            status = Objects.requireNonNull(status, "batch chunk status must not be null");
            failureCategory = requireFailureCategory(failureCategory);
            requireChunkIndex(chunkIndex);
            requireNonNegative("batch chunk start offset", startOffset);
            requireNonNegative("batch chunk input count", inputCount);
            requireNonNegative("batch chunk affected rows", affectedRows);
            requireDuration(durationNanos);
        }

        static Chunk fromValidated(BatchWriteRequestView request,
                                   BatchChunkResult chunk,
                                   long durationNanos) {
            BatchChunkResult safeChunk = Objects.requireNonNull(
                    chunk, "batch chunk result must not be null");
            return new Chunk(request,
                             safeChunk.status(),
                             safeChunk.chunkIndex(),
                             safeChunk.startOffset(),
                             safeChunk.inputCount(),
                             safeChunk.affectedRows(),
                             durationNanos,
                             BatchObservationClassification.category(safeChunk),
                             safeChunk.failure(),
                             safeChunk.recoveryToken());
        }

        @Override
        public BatchExecutionEventType eventType() {
            return BatchExecutionEventType.CHUNK;
        }

        @Override
        public SqlExecutionBackend backend() {
            return request.backend();
        }

        @Override
        public SqlStatementType statementType() {
            return SqlStatementType.fromSql(request.sql());
        }

        @Override
        public String sql() {
            return request.sql();
        }

        public BatchWriteOptions.Mode mode() {
            return request.mode();
        }

        public int parameterCount() {
            return request.parameterCount();
        }

        @Override
        public SqlExecutionResultKind resultKind() {
            return SqlExecutionResultKind.fromBatchChunk(status, failureCategory);
        }

    }

    /** 完整批量写入的汇总事实。 */
    record Summary(BatchWriteRequestView request,
                   BatchWriteResult.Status status,
                   long inputCount,
                   long affectedRows,
                   long chunkCount,
                   long successfulChunkCount,
                   long failedChunkCount,
                   long durationNanos,
                   SqlFailureCategory failureCategory,
                   BatchChunkResult.Failure failure,
                   BatchChunkResult.RecoveryToken recoveryToken) implements BatchExecutionObservation {

        public Summary {
            request = Objects.requireNonNull(request, "batch request view must not be null");
            status = Objects.requireNonNull(status, "batch summary status must not be null");
            failureCategory = requireFailureCategory(failureCategory);
            requireNonNegative("batch summary input count", inputCount);
            requireNonNegative("batch summary affected rows", affectedRows);
            requireNonNegative("batch summary chunk count", chunkCount);
            requireNonNegative("batch summary successful chunk count", successfulChunkCount);
            requireNonNegative("batch summary failed chunk count", failedChunkCount);
            if (successfulChunkCount > chunkCount || failedChunkCount != chunkCount - successfulChunkCount) {
                throw new IllegalArgumentException(
                        "successful and failed chunks must exactly partition total chunks");
            }
            requireDuration(durationNanos);
        }

        @Override
        public BatchExecutionEventType eventType() {
            return BatchExecutionEventType.SUMMARY;
        }

        @Override
        public SqlExecutionBackend backend() {
            return request.backend();
        }

        @Override
        public SqlStatementType statementType() {
            return SqlStatementType.fromSql(request.sql());
        }

        @Override
        public String sql() {
            return request.sql();
        }

        public BatchWriteOptions.Mode mode() {
            return request.mode();
        }

        public int parameterCount() {
            return request.parameterCount();
        }

        @Override
        public SqlExecutionResultKind resultKind() {
            return SqlExecutionResultKind.fromBatchSummary(status, failureCategory);
        }
    }

    /** 一个恢复令牌查询的结果。 */
    record Recovery(BatchResolution.Status status,
                    long durationNanos,
                    SqlFailureCategory failureCategory,
                    BatchChunkResult.Failure failure,
                    BatchChunkResult.RecoveryToken recoveryToken) implements BatchExecutionObservation {

        public Recovery {
            status = Objects.requireNonNull(status, "batch recovery status must not be null");
            failureCategory = requireFailureCategory(failureCategory);
            recoveryToken = Objects.requireNonNull(recoveryToken, "batch recovery token must not be null");
            requireChunkIndex(recoveryToken.chunkIndex());
            requireDuration(durationNanos);
        }

        @Override
        public BatchExecutionEventType eventType() {
            return BatchExecutionEventType.RECOVERY;
        }

        @Override
        public SqlExecutionBackend backend() {
            return SqlExecutionBackend.R2DBC;
        }

        @Override
        public SqlStatementType statementType() {
            return SqlStatementType.UNKNOWN;
        }

        @Override
        public String sql() {
            return "";
        }

        public int chunkIndex() {
            return recoveryToken.chunkIndex();
        }

        @Override
        public SqlExecutionResultKind resultKind() {
            return SqlExecutionResultKind.fromBatchRecovery(status, failureCategory);
        }
    }

    /** 只保留观测需要的请求元数据，不延长批量参数流的生命周期。 */
    record BatchWriteRequestView(String sql,
                                 BatchWriteOptions.Mode mode,
                                 int parameterCount,
                                 SqlExecutionBackend backend) {

        public BatchWriteRequestView(String sql, BatchWriteOptions.Mode mode, int parameterCount) {
            this(sql, mode, parameterCount, SqlExecutionBackend.UNKNOWN);
        }

        public BatchWriteRequestView {
            sql = Objects.requireNonNull(sql, "batch sql text must not be null");
            mode = Objects.requireNonNull(mode, "batch write mode must not be null");
            backend = Objects.requireNonNull(backend, "batch execution backend must not be null");
            if (parameterCount < 0) {
                throw new IllegalArgumentException("batch parameter count must not be negative");
            }
        }
    }

    private static SqlFailureCategory requireFailureCategory(SqlFailureCategory failureCategory) {
        return Objects.requireNonNull(failureCategory, "batch failure category must not be null");
    }

    private static void requireChunkIndex(int chunkIndex) {
        if (chunkIndex < 0) {
            throw new IllegalArgumentException("batch chunk index must not be negative");
        }
    }

    private static void requireDuration(long durationNanos) {
        requireNonNegative("batch observation duration nanos", durationNanos);
    }

    private static void requireNonNegative(String name, long value) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
    }
}
