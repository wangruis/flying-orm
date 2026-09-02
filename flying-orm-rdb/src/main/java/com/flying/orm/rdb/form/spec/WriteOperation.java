package com.flying.orm.rdb.form.spec;

/**
 * 单条写入规格声明的操作类型，用于在执行入口拒绝把更新规格误当插入、把删除规格误当更新。
 *
 * @author wangr
 * @date 2026-08-04
 * @version v1.0
 */
public enum WriteOperation {
    /** 插入。 */
    INSERT,
    /** 条件更新。 */
    UPDATE,
    /** 条件删除，可由执行入口选择逻辑删除或物理删除。 */
    DELETE
}
