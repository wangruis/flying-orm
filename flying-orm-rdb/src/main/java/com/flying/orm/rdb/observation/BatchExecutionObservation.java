package com.flying.orm.rdb.observation;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchResolution;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;

import java.time.Duration;
import java.util.Objects;

/**
 * 批量写入的细粒度观测结果，统一承载 CHUNK、SUMMARY 和 RECOVERY 三类事件。
 *
 * <p>事件只保留 SQL 形态、数量、位置、状态和恢复令牌，不保存参数值或 rows Publisher，避免日志泄露业务数据，
 * 也避免观测对象延长整批数据的生命周期。不同事件不适用的状态字段为 null，构造器会校验对应必填字段。</p>
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public record BatchExecutionObservation(BatchExecutionEventType eventType,
                                        SqlExecutionBackend backend,
                                        SqlStatementType statementType,
                                        String sql,
                                        BatchWriteOptions.Mode mode,
                                        BatchWriteResult.Status summaryStatus,
                                        BatchChunkResult.Status chunkStatus,
                                        BatchResolution.Status recoveryStatus,
                                        int chunkIndex,
                                        long startOffset,
                                        long inputCount,
                                        long affectedRows,
                                        long chunkCount,
                                        long successfulChunkCount,
                                        long failedChunkCount,
                                        int parameterCount,
                                        long durationNanos,
                                        SqlFailureCategory failureCategory,
                                        BatchChunkResult.Failure failure,
                                        BatchChunkResult.RecoveryToken recoveryToken) {

    public static final int NO_CHUNK = -1;

    /**
     * 兼容原来没有分片统计字段的构造方式。旧调用仍可创建事件，只是 SUMMARY 无法凭空还原分片数量，
     * 因而三个新增计数都是 0；使用本类工厂方法创建的新事件会带上准确计数。
     */
    public BatchExecutionObservation(BatchExecutionEventType eventType,
                                     SqlStatementType statementType,
                                     String sql,
                                     BatchWriteOptions.Mode mode,
                                     BatchWriteResult.Status summaryStatus,
                                     BatchChunkResult.Status chunkStatus,
                                     BatchResolution.Status recoveryStatus,
                                     int chunkIndex,
                                     long startOffset,
                                     long inputCount,
                                     long affectedRows,
                                     int parameterCount,
                                     long durationNanos,
                                     SqlFailureCategory failureCategory,
                                     BatchChunkResult.Failure failure,
                                     BatchChunkResult.RecoveryToken recoveryToken) {
        this(eventType,
             SqlExecutionBackend.UNKNOWN,
             statementType,
             sql,
             mode,
             summaryStatus,
             chunkStatus,
             recoveryStatus,
             chunkIndex,
             startOffset,
             inputCount,
             affectedRows,
             eventType == BatchExecutionEventType.CHUNK ? 1 : 0,
             BatchObservationClassification.successfulChunkCount(eventType, chunkStatus),
             BatchObservationClassification.failedChunkCount(eventType, chunkStatus),
             parameterCount,
             durationNanos,
             failureCategory,
             failure,
             recoveryToken);
    }

    public BatchExecutionObservation {
        // 三种事件共用一个模型，但每种事件只允许自己的状态字段成为主状态，避免上层指标解释冲突。
        eventType = Objects.requireNonNull(eventType, "batch observation event type must not be null");
        backend = Objects.requireNonNull(backend, "batch execution backend must not be null");
        statementType = Objects.requireNonNull(statementType, "batch sql statement type must not be null");
        sql = Objects.requireNonNull(sql, "batch sql text must not be null");
        // 一个事件只能有一个主状态。否则 CHUNK 同时带 summaryStatus 时，上层按不同字段会得到两种结论。
        switch (eventType) {
            case CHUNK -> {
                mode = Objects.requireNonNull(mode, "batch write mode must not be null");
                chunkStatus = Objects.requireNonNull(chunkStatus, "batch chunk status must not be null");
                requireAbsent(summaryStatus, "batch chunk event cannot include summary status");
                requireAbsent(recoveryStatus, "batch chunk event cannot include recovery status");
                if (chunkIndex == NO_CHUNK) {
                    throw new IllegalArgumentException("batch chunk event must include chunk index");
                }
            }
            case SUMMARY -> {
                mode = Objects.requireNonNull(mode, "batch write mode must not be null");
                summaryStatus = Objects.requireNonNull(summaryStatus, "batch summary status must not be null");
                requireAbsent(chunkStatus, "batch summary event cannot include chunk status");
                requireAbsent(recoveryStatus, "batch summary event cannot include recovery status");
                if (chunkIndex != NO_CHUNK) {
                    throw new IllegalArgumentException("batch summary event cannot include chunk index");
                }
            }
            case RECOVERY -> {
                requireAbsent(mode, "batch recovery event cannot include write mode");
                recoveryStatus = Objects.requireNonNull(recoveryStatus, "batch recovery status must not be null");
                requireAbsent(summaryStatus, "batch recovery event cannot include summary status");
                requireAbsent(chunkStatus, "batch recovery event cannot include chunk status");
                if (chunkIndex == NO_CHUNK) {
                    throw new IllegalArgumentException("batch recovery event must include chunk index");
                }
            }
        }
        failureCategory = Objects.requireNonNull(failureCategory, "batch failure category must not be null");
        if (chunkIndex < NO_CHUNK) {
            throw new IllegalArgumentException("batch observation chunk index must not be less than -1");
        }
        if (startOffset < 0) {
            throw new IllegalArgumentException("batch observation start offset must not be negative");
        }
        if (inputCount < 0 || affectedRows < 0) {
            throw new IllegalArgumentException("batch observation counts must not be negative");
        }
        if (chunkCount < 0 || successfulChunkCount < 0 || failedChunkCount < 0) {
            throw new IllegalArgumentException("batch observation chunk counts must not be negative");
        }
        if (successfulChunkCount > chunkCount || failedChunkCount != chunkCount - successfulChunkCount) {
            throw new IllegalArgumentException("successful and failed chunks must exactly partition total chunks");
        }
        if (eventType == BatchExecutionEventType.CHUNK && chunkCount != 1) {
            throw new IllegalArgumentException("batch chunk event must describe exactly one chunk");
        }
        if (eventType == BatchExecutionEventType.RECOVERY && chunkCount != 0) {
            throw new IllegalArgumentException("batch recovery event cannot include chunk statistics");
        }
        if (parameterCount < 0) {
            throw new IllegalArgumentException("batch observation parameter count must not be negative");
        }
        if (durationNanos < 0) {
            throw new IllegalArgumentException("batch observation duration nanos must not be negative");
        }
    }

    /** 根据一个已完成分片创建事件，保留输入偏移和可能的冲突/恢复信息。 */
    public static BatchExecutionObservation chunk(BatchWriteRequestView request,
                                                  BatchChunkResult chunk,
                                                  long durationNanos) {
        return BatchExecutionObservationFactory.chunk(request, chunk, durationNanos);
    }

    /** 根据完整批量结果创建汇总事件，失败分类取冲突或首个结构化失败。 */
    public static BatchExecutionObservation summary(BatchWriteRequestView request,
                                                    BatchWriteResult result,
                                                    long durationNanos) {
        return BatchExecutionObservationFactory.summary(request, result, durationNanos);
    }

    /**
     * 流式入口没有必要为了最后一条汇总事件保存全部分片，这个工厂直接接收增量汇总值。
     */
    public static BatchExecutionObservation summary(BatchWriteRequestView request,
                                                    BatchWriteResult.Status status,
                                                    long inputCount,
                                                    long affectedRows,
                                                    long conflictCount,
                                                    BatchChunkResult.Failure firstFailure,
                                                    BatchChunkResult.RecoveryToken firstRecoveryToken,
                                                    long durationNanos) {
        return BatchExecutionObservationFactory.summary(request,
                                                        status,
                                                        inputCount,
                                                        affectedRows,
                                                        conflictCount,
                                                        0,
                                                        0,
                                                        0,
                                                        firstFailure,
                                                        firstRecoveryToken,
                                                        durationNanos);
    }

    /**
     * 流式批量的完整汇总入口。三个分片计数由订阅内的固定大小累加器提供，不保存分片结果列表。
     */
    public static BatchExecutionObservation summary(BatchWriteRequestView request,
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
        return BatchExecutionObservationFactory.summary(request,
                                                        status,
                                                        inputCount,
                                                        affectedRows,
                                                        conflictCount,
                                                        chunkCount,
                                                        successfulChunkCount,
                                                        failedChunkCount,
                                                        firstFailure,
                                                        firstRecoveryToken,
                                                        durationNanos);
    }

    /**
     * 执行链在产生结构化 BatchWriteResult 前失败时创建保守的 UNKNOWN 汇总事件。
     */
    public static BatchExecutionObservation failedSummary(BatchWriteRequestView request,
                                                          long durationNanos,
                                                          Throwable error) {
        return BatchExecutionObservationFactory.failedSummary(request, durationNanos, error);
    }

    /** 创建 UNKNOWN 恢复查询事件；恢复本身失败时同时保留异常分类。 */
    public static BatchExecutionObservation recovery(BatchResolution resolution,
                                                     long durationNanos,
                                                     Throwable error) {
        return BatchExecutionObservationFactory.recovery(resolution, durationNanos, error);
    }

    /** 把纳秒存储转换成便于上层指标系统使用的 Duration。 */
    public Duration duration() {
        return Duration.ofNanos(durationNanos);
    }

    /**
     * 将三套底层状态收口为统一结果类型，方便告警规则不必分别理解分片、汇总和恢复枚举。
     */
    public SqlExecutionResultKind resultKind() {
        return switch (eventType) {
            case CHUNK -> SqlExecutionResultKind.fromBatchChunk(chunkStatus, failureCategory);
            case SUMMARY -> SqlExecutionResultKind.fromBatchSummary(summaryStatus, failureCategory);
            case RECOVERY -> SqlExecutionResultKind.fromBatchRecovery(recoveryStatus, failureCategory);
        };
    }

    private static void requireAbsent(Object value, String message) {
        if (value != null) {
            throw new IllegalArgumentException(message);
        }
    }

    /**
     * 只拿观测需要的请求信息，避免把整条 rows Publisher 带进观测对象。
     */
    public record BatchWriteRequestView(String sql,
                                        BatchWriteOptions.Mode mode,
                                        int parameterCount,
                                        SqlExecutionBackend backend) {

        /** 兼容手工构造的旧观测请求；框架执行路径始终传入明确后端。 */
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
}
