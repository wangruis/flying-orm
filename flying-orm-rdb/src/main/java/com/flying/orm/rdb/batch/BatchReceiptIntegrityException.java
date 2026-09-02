package com.flying.orm.rdb.batch;

/**
 * 批量回执表违反唯一性或精确行数契约时抛出的异常。
 *
 * <p>回执是 UNKNOWN 写入恢复的事实来源，因此缺行或重复行都不能按普通数据库影响行数继续处理。</p>
 *
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class BatchReceiptIntegrityException extends RuntimeException {

    /**
     * 创建回执完整性异常。
     *
     * @param operation 回执操作
     * @param actualRows 实际影响或读取的行数
     */
    public BatchReceiptIntegrityException(String operation, long actualRows) {
        this(operation, "affect exactly one row", actualRows);
    }

    /**
     * 创建带明确基数期望的回执完整性异常。
     *
     * @param operation  回执操作
     * @param expectation 期望的行数约束
     * @param actualRows 实际影响或读取的行数
     */
    public BatchReceiptIntegrityException(String operation, String expectation, long actualRows) {
        super("batch receipt integrity check failed");
    }

    /**
     * 创建无法安全转换回执字段时使用的完整性异常。
     *
     * @param operation 回执操作或字段名
     * @param detail 不包含业务参数值的失败说明
     * @param cause codec 或驱动值转换的原始原因
     */
    public BatchReceiptIntegrityException(String operation, String detail, Throwable cause) {
        super("batch receipt integrity check failed", cause);
    }
}
