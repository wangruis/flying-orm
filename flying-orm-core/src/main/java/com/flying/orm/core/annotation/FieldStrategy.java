package com.flying.orm.core.annotation;

/**
 * 控制字段值为空时是否参加 INSERT 或 UPDATE。
 *
 * @author wangr
 * @date 2026-08-08
 * @version v2.0.0
 */
public enum FieldStrategy {

    /** 使用全局或框架默认策略。 */
    DEFAULT,

    /** 无论值是否为空都参与写入。 */
    ALWAYS,

    /** 只有值不为 {@code null} 时参与写入。 */
    NOT_NULL,

    /** 只有值不为 {@code null} 且不是空字符串时参与写入。 */
    NOT_EMPTY,

    /** 永不参与写入。 */
    NEVER
}
