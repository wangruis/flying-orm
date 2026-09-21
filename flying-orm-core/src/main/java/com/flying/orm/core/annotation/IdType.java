package com.flying.orm.core.annotation;

/**
 * 主键值的生成方式。
 *
 * <p>这些枚举只描述策略，不直接访问数据库。真正的生成、回填和方言差异由执行层完成。</p>
 *
 * @author wangr
 * @date 2026-08-08
 * @version v2.0.0
 */
public enum IdType {

    /** 不替调用方生成主键，由调用方自己提供或按普通字段处理。 */
    NONE,

    /** 使用 ORM 的分布式 ID 生成器生成主键。 */
    ASSIGN_ID,

    /** 由数据库自增列或 identity 列生成主键。 */
    AUTO,

    /** 主键由调用方在写入前明确提供。 */
    INPUT,

    /** 使用 ORM 生成 UUID 主键。 */
    ASSIGN_UUID
}
