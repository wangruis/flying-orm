package com.flying.orm.rdb.batch;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Objects;

/**
 * 一个批量分片在驱动返回点已经形成的不可变执行事实。
 *
 * <p>偏移都是整批输入中的绝对位置。对象只保留驱动能证明的成功/失败位置，不保留 SQL 参数、
 * 请求行或实体。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class BatchChunkExecutionFact {

    private final int chunkIndex;
    private final long startOffset;
    private final int inputCount;
    private final BitSet successfulOffsets;
    private final BitSet failedOffsets;
    private final BatchExecutionState state;
    private final BatchAffectedRows affectedRows;
    private final BatchChunkResult.Failure failure;

    private BatchChunkExecutionFact(int chunkIndex,
                                    long startOffset,
                                    int inputCount,
                                    BitSet successfulOffsets,
                                    BitSet failedOffsets,
                                    BatchExecutionState state,
                                    BatchAffectedRows affectedRows,
                                    BatchChunkResult.Failure failure) {
        validatePosition(chunkIndex, startOffset, inputCount);
        this.chunkIndex = chunkIndex;
        this.startOffset = startOffset;
        this.inputCount = inputCount;
        this.successfulOffsets = Objects.requireNonNull(
                successfulOffsets, "successful batch evidence offsets must not be null");
        this.failedOffsets = Objects.requireNonNull(
                failedOffsets, "failed batch evidence offsets must not be null");
        if (this.successfulOffsets.intersects(this.failedOffsets)) {
            throw new IllegalArgumentException("batch evidence position cannot be both successful and failed");
        }
        this.state = Objects.requireNonNull(state, "batch execution state must not be null");
        this.affectedRows = Objects.requireNonNull(
                affectedRows, "batch affected rows must not be null");
        this.failure = failure;
        if (state == BatchExecutionState.SUCCESS && failure != null) {
            throw new IllegalArgumentException("successful batch chunk evidence must not include failure");
        }
        if (state != BatchExecutionState.SUCCESS && failure == null) {
            throw new IllegalArgumentException("non-success batch chunk evidence must include failure");
        }
    }

    public static BatchChunkExecutionFact of(int chunkIndex,
                                             long startOffset,
                                             int inputCount,
                                             List<Long> successfulOffsets,
                                             List<Long> failedOffsets,
                                             BatchExecutionState state,
                                             BatchAffectedRows affectedRows,
                                             BatchChunkResult.Failure failure) {
        validatePosition(chunkIndex, startOffset, inputCount);
        return new BatchChunkExecutionFact(
                chunkIndex, startOffset, inputCount,
                offsets(successfulOffsets, startOffset, inputCount, "successful"),
                offsets(failedOffsets, startOffset, inputCount, "failed"),
                state, affectedRows, failure);
    }

    /** 整个分片已被驱动证明成功；直接创建压缩区间，不物化逐行 offset。 */
    public static BatchChunkExecutionFact allSuccessful(int chunkIndex,
                                                        long startOffset,
                                                        int inputCount,
                                                        BatchAffectedRows affectedRows) {
        validatePosition(chunkIndex, startOffset, inputCount);
        BitSet successful = new BitSet(inputCount);
        successful.set(0, inputCount);
        return new BatchChunkExecutionFact(
                chunkIndex, startOffset, inputCount, successful, new BitSet(),
                BatchExecutionState.SUCCESS, affectedRows, null);
    }

    public int chunkIndex() {
        return chunkIndex;
    }

    public long startOffset() {
        return startOffset;
    }

    public int inputCount() {
        return inputCount;
    }

    public List<Long> successfulOffsets() {
        return offsets(successfulOffsets, startOffset);
    }

    /** @return 已证明成功的位置数量；不会为查询数量物化 offset 列表 */
    public int successfulCount() {
        return successfulOffsets.cardinality();
    }

    public List<Long> failedOffsets() {
        return offsets(failedOffsets, startOffset);
    }

    /** @return 已证明失败的位置数量；不会为查询数量物化 offset 列表 */
    public int failedCount() {
        return failedOffsets.cardinality();
    }

    public BatchExecutionState state() {
        return state;
    }

    public BatchAffectedRows affectedRows() {
        return affectedRows;
    }

    /** @return 安全失败摘要；SUCCESS 时为 null */
    public BatchChunkResult.Failure failure() {
        return failure;
    }

    private static BitSet offsets(List<Long> offsets, long start, int count, String role) {
        List<Long> snapshot = List.copyOf(Objects.requireNonNull(
                offsets, "batch " + role + " offsets must not be null"));
        long end = start + count;
        long previous = -1L;
        BitSet compressed = new BitSet(count);
        for (Long offset : snapshot) {
            long value = Objects.requireNonNull(offset, "batch evidence offset must not be null");
            if (value < start || value >= end) {
                throw new IllegalArgumentException("batch evidence offset must belong to its chunk");
            }
            if (value <= previous) {
                throw new IllegalArgumentException("batch evidence offsets must be strictly increasing");
            }
            previous = value;
            compressed.set(Math.toIntExact(value - start));
        }
        return compressed;
    }

    private static void validatePosition(int chunkIndex, long startOffset, int inputCount) {
        if (chunkIndex < 0 || startOffset < 0L || inputCount < 0) {
            throw new IllegalArgumentException("batch chunk evidence position must not be negative");
        }
        if (inputCount > 0 && startOffset > Long.MAX_VALUE - (inputCount - 1L)) {
            throw new IllegalArgumentException("batch chunk evidence range must not overflow");
        }
    }

    private static List<Long> offsets(BitSet compressed, long start) {
        if (compressed.isEmpty()) {
            return List.of();
        }
        List<Long> result = new ArrayList<>(compressed.cardinality());
        for (int relative = compressed.nextSetBit(0);
             relative >= 0;
             relative = compressed.nextSetBit(relative + 1)) {
            result.add(start + relative);
            if (relative == Integer.MAX_VALUE) {
                break;
            }
        }
        return List.copyOf(result);
    }
}
