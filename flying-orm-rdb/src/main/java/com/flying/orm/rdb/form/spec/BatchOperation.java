package com.flying.orm.rdb.form.spec;

/**
 * 批量规格支持的数据库写入操作。
 *
 * @author wangr
 * @date 2026-08-04
 * @version v1.0
 */
public enum BatchOperation {
    /** 仅插入。 */
    INSERT,
    /** 按方言冲突规则插入或更新。 */
    UPSERT,
    /** 每行携带条件和版本的乐观更新。 */
    UPDATE
}
