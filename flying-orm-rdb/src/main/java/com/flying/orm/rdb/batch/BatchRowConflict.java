package com.flying.orm.rdb.batch;

/**
 * 一条批量输入没有得到预期影响行数时的安全摘要。
 *
 * @param inputOffset 输入在整批数据中的位置，从 0 开始
 * @param expectedRows 期望影响行数
 * @param actualRows 数据库实际返回的影响行数
 * @param reason 冲突原因
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public record BatchRowConflict(long inputOffset,
                               long expectedRows,
                               long actualRows,
                               Reason reason) {

    public BatchRowConflict {
        if (inputOffset < 0) {
            throw new IllegalArgumentException("batch conflict input offset must not be negative");
        }
        if (expectedRows < 0 || actualRows < 0) {
            throw new IllegalArgumentException("batch conflict row counts must not be negative");
        }
        if (reason == null) {
            throw new IllegalArgumentException("batch conflict reason must not be null");
        }
    }

    /**
     * 根据实际影响行数生成原因。乐观锁最常见的是 NO_MATCH，也就是旧版本已经变了。
     */
    public static BatchRowConflict exactlyOne(long inputOffset, long actualRows) {
        if (actualRows == 1) {
            throw new IllegalArgumentException("actual rows already match the exactly-one policy");
        }
        return new BatchRowConflict(inputOffset,
                                    1,
                                    actualRows,
                                    actualRows == 0 ? Reason.NO_MATCH : Reason.TOO_MANY_ROWS);
    }

    public enum Reason {
        /** 条件没有匹配到数据，通常表示版本冲突或数据已被删除。 */
        NO_MATCH,
        /** 条件一次改到了多行，说明调用方给的定位条件不够精确。 */
        TOO_MANY_ROWS
    }
}
