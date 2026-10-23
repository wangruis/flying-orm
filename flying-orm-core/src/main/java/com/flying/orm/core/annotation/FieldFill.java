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
    INSERT_UPDATE,

    /**
     * 插入或 upsert 取值时仅填充 null，保留调用方已提供的值；普通更新不填充。
     * upsert 冲突更新仍服从 updateStrategy，不自动排除本字段，也不回写实体。
     */
    INSERT_IF_NULL,

    /**
     * 插入或 upsert 时仅填充 null；本次生成的非 null 默认值只参与 INSERT，
     * 不覆盖冲突行的既有值。显式值仍服从 updateStrategy；普通更新不填充，不回写实体。
     */
    INSERT_DEFAULT
}
