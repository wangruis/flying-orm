package com.flying.orm.core.annotation;

/**
 * 实体字段的自动填充时机。
 *
 * @author wangr
 * @date 2026-08-08
 * @version v2.0.0
 */
public enum FieldFill {

    /** 不自动填充。 */
    DEFAULT,

    /** 只在插入前填充。 */
    INSERT,

    /** 只在更新前填充。 */
    UPDATE,

    /** 插入和更新前都填充。 */
    INSERT_UPDATE
}
