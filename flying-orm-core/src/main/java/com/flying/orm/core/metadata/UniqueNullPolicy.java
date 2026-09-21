package com.flying.orm.core.metadata;

/**
 * 唯一键中的 NULL 参与规则；方言必须明确支持声明的语义。
 *
 * @author wangr
 * @version v4.0
 */
public enum UniqueNullPolicy {

    /** 沿用数据库对普通唯一约束的默认 NULL 语义。 */
    DEFAULT,

    /** 任一键列为 NULL 的行不参与唯一性判断；全部键列非 NULL 时才要求唯一。 */
    DISTINCT
}
