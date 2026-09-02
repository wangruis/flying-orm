package com.flying.orm.rdb.batch;

import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;

import java.util.List;
import java.util.Objects;

/**
 * BatchWriteResult 汇总一次批量写入已经确认的结果。
 *
 * @param mode         本次使用的提交方式
 * @param status       整体状态
 * @param inputCount   已接收的总行数
 * @param affectedRows 已确认提交的总影响行数
 * @param chunks       分片结果
 * @author wangr
 * @date 2026-07-23
 * @version v1.0
 */
public record BatchWriteResult(BatchWriteOptions.Mode mode,
                               Status status,
                               long inputCount,
                               long affectedRows,
                               List<BatchChunkResult> chunks) {

    /**
     * 复制分片列表，避免结果返回后被外部修改。
     */
    public BatchWriteResult {
        mode = Objects.requireNonNull(mode, "batch write result mode must not be null");
        status = Objects.requireNonNull(status, "batch write result status must not be null");
        if (inputCount < 0 || affectedRows < 0) {
            throw new IllegalArgumentException("batch write result counts must not be negative");
        }
        chunks = List.copyOf(Objects.requireNonNull(chunks, "batch write result chunks must not be null"));
    }

    /**
     * 从分片结果计算整体状态和已提交行数。
     *
     * @param mode   提交方式
     * @param chunks 分片结果
     * @return 汇总结果
     */
    public static BatchWriteResult from(BatchWriteOptions.Mode mode, List<BatchChunkResult> chunks) {
        List<BatchChunkResult> safeChunks = List.copyOf(Objects.requireNonNull(chunks,
                                                                               "batch chunks must not be null"));
        long inputCount = sumExact(safeChunks.stream().mapToLong(BatchChunkResult::inputCount));
        long affectedRows = sumExact(safeChunks.stream()
                                               .filter(chunk -> chunk.status() == BatchChunkResult.Status.COMMITTED)
                                               .mapToLong(BatchChunkResult::affectedRows));
        return new BatchWriteResult(mode,
                                    summarize(Objects.requireNonNull(mode, "batch mode must not be null"), safeChunks),
                                    inputCount,
                                    affectedRows,
                                    safeChunks);
    }

    /**
     * 创建空输入的批量结果。没有任何分片执行，也就没有影响行数。
     *
     * @param mode 提交方式
     * @return 空批量结果
     */
    public static BatchWriteResult empty(BatchWriteOptions.Mode mode) {
        return new BatchWriteResult(Objects.requireNonNull(mode, "batch mode must not be null"),
                                    Status.COMMITTED,
                                    0,
                                    0,
                                    List.of());
    }

    /**
     * 返回整批里已经确认的冲突数量。UNKNOWN 不会被猜成冲突。
     */
    public long conflictCount() {
        return sumExact(chunks.stream().mapToLong(chunk -> chunk.conflicts().size()));
    }

    /**
     * 按输入顺序返回所有已确认冲突，方便上层直接定位原始数据。
     */
    public List<BatchRowConflict> conflicts() {
        return chunks.stream().flatMap(chunk -> chunk.conflicts().stream()).toList();
    }

    private static Status summarize(BatchWriteOptions.Mode mode, List<BatchChunkResult> chunks) {
        if (chunks.stream().anyMatch(chunk -> chunk.status() == BatchChunkResult.Status.UNKNOWN)) {
            return Status.UNKNOWN;
        }
        boolean allCommitted = chunks.stream()
                                     .allMatch(chunk -> chunk.status() == BatchChunkResult.Status.COMMITTED);
        if (allCommitted) {
            return Status.COMMITTED;
        }
        boolean anyEnlisted = chunks.stream()
                                    .anyMatch(chunk -> chunk.status() == BatchChunkResult.Status.ENLISTED);
        if (anyEnlisted) {
            boolean allEnlisted = chunks.stream()
                                        .allMatch(chunk -> chunk.status() == BatchChunkResult.Status.ENLISTED);
            // ENLISTED 和其它状态混在一起不是正常执行结果，保守返回 UNKNOWN，不能猜测外层事务结局。
            return allEnlisted ? Status.ENLISTED : Status.UNKNOWN;
        }
        return mode == BatchWriteOptions.Mode.ATOMIC ? Status.ROLLED_BACK : Status.PARTIAL;
    }

    private static long sumExact(java.util.stream.LongStream counts) {
        try {
            return counts.reduce(0L, Math::addExact);
        } catch (ArithmeticException overflow) {
            throw new RdbException(RdbErrorKind.UNKNOWN,
                                   "database execution count exceeds supported range",
                                   null,
                                   null,
                                   overflow);
        }
    }

    /** 整次批量写入的汇总状态。 */
    public enum Status {
        /** 所有数据都已确认提交。 */
        COMMITTED,
        /** SQL 已成功加入外部事务，最终结果要等外层事务提交或回滚后才能确定。 */
        ENLISTED,
        /** 独立模式中同时存在已提交和未提交分片。 */
        PARTIAL,
        /** 原子事务已经确认整批回滚。 */
        ROLLED_BACK,
        /** 至少一个提交结果暂时无法确认。 */
        UNKNOWN
    }
}
