package com.flying.orm.rdb.observation;

/**
 * 决定一条 SQL 执行日志里保留哪些结果字段，以及哪些成功事件值得输出。
 *
 * <p>这个策略只影响日志，不改变 observer 收到的观测事件，更不会影响 SQL、事务或批量结果。
 * 慢 SQL 阈值只过滤成功事件；错误、取消、UNKNOWN 等需要排障的结果始终保留。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public record SqlExecutionLogSelection(boolean includeAffectedRows,
                                       boolean includeReturnedRows,
                                       boolean includeDuration,
                                       long slowThresholdNanos,
                                       boolean includeChunkEvents,
                                       boolean includeSummaryEvents,
                                        boolean includeRecoveryEvents) {

    /** 默认慢 SQL 阈值：执行时间达到 1 秒就提升为 WARN。 */
    public static final long DEFAULT_SLOW_THRESHOLD_NANOS = 1_000_000_000L;

    /** 默认保留结果字段，快 SQL 交给 DEBUG，至少 1 秒的 SQL 交给 WARN。 */
    public static SqlExecutionLogSelection defaults() {
        return new SqlExecutionLogSelection(true,
                                            true,
                                            true,
                                            DEFAULT_SLOW_THRESHOLD_NANOS,
                                            true,
                                            true,
                                            true);
    }

    public SqlExecutionLogSelection {
        if (slowThresholdNanos < 0) {
            throw new IllegalArgumentException("sql log slow threshold nanos must not be negative");
        }
    }

    boolean isSlow(long durationNanos) {
        return durationNanos >= slowThresholdNanos;
    }

    boolean shouldLog(BatchExecutionObservation observation) {
        if (!includes(observation.eventType())) {
            return false;
        }
        // 成功与失败的级别由 observer 决定；这里仅负责选择是否记录 CHUNK、SUMMARY、RECOVERY。
        return true;
    }

    private boolean includes(BatchExecutionEventType eventType) {
        return switch (eventType) {
            case CHUNK -> includeChunkEvents;
            case SUMMARY -> includeSummaryEvents;
            case RECOVERY -> includeRecoveryEvents;
        };
    }
}
