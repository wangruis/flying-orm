package com.flying.orm.rdb.id;

import java.util.concurrent.atomic.AtomicLong;

/**
 * 使用 41 位时间、10 位节点和 12 位序列生成 64 位雪花 ID。
 *
 * <p>节点号必须由应用显式配置，取值为 0 到 1023。实现用一个原子状态合并毫秒和序列，正常并发路径不加锁；
 * 同一毫秒超过 4096 个 ID 时短暂等待下一毫秒。检测到时钟回拨会立即失败，避免生成重复主键。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public final class SnowflakeIdGenerator implements IdGenerator {

    private static final long DEFAULT_EPOCH_MILLIS = 1_704_067_200_000L;
    private static final long MAX_NODE_ID = 1_023L;
    private static final long SEQUENCE_MASK = 4_095L;
    private static final long MAX_TIMESTAMP_DELTA = (1L << 41) - 1;

    private final long nodeId;
    private final long epochMillis;
    private final AtomicLong state = new AtomicLong();

    private SnowflakeIdGenerator(long nodeId, long epochMillis) {
        if (nodeId < 0 || nodeId > MAX_NODE_ID) {
            throw new IllegalArgumentException("snowflake node id must be between 0 and 1023");
        }
        if (epochMillis < 0 || epochMillis >= System.currentTimeMillis()) {
            throw new IllegalArgumentException("snowflake epoch must be non-negative and earlier than current time");
        }
        this.nodeId = nodeId;
        this.epochMillis = epochMillis;
    }

    /** 使用内置的 2024-01-01 UTC 起点创建生成器。 */
    public static SnowflakeIdGenerator create(long nodeId) {
        return new SnowflakeIdGenerator(nodeId, DEFAULT_EPOCH_MILLIS);
    }

    @Override
    public Object generate(Class<?> entityType, String propertyName, Class<?> targetType) {
        while (true) {
            long current = state.get();
            long lastMillis = current >>> 12;
            long now = System.currentTimeMillis();
            if (now < lastMillis) {
                throw new IllegalStateException("system clock moved backwards while generating an id");
            }
            long sequence = now == lastMillis ? (current & SEQUENCE_MASK) + 1 : 0L;
            if (sequence > SEQUENCE_MASK) {
                Thread.onSpinWait();
                continue;
            }
            long next = (now << 12) | sequence;
            if (!state.compareAndSet(current, next)) {
                continue;
            }
            long delta = now - epochMillis;
            if (delta > MAX_TIMESTAMP_DELTA) {
                throw new IllegalStateException("snowflake timestamp exceeds the 41-bit range");
            }
            return (delta << 22) | (nodeId << 12) | sequence;
        }
    }
}
