package com.flying.orm.core.scope;

/**
 * 一个字段在当前 SQL 形状中的明确用途。
 *
 * <p>显示、条件、排序、关联、聚合和写入分别授权，避免“能看到字段”被误解成“也能过滤、
 * 排序或写入字段”。INSERT 与 UPDATE 走 {@link FieldScope} 的写权限，其余用途走读权限。</p>
 *
 * @author wangr
 * @version v3.2
 */
public enum FieldUse {
    PROJECT,
    FULL_VALUE,
    MASKED_VALUE,
    FILTER,
    HAVING,
    SORT,
    JOIN,
    GROUP,
    AGGREGATE,
    INSERT,
    UPDATE;

    /** @return 当前用途是否会把字段写入数据库 */
    public boolean write() {
        return this == INSERT || this == UPDATE;
    }

    /** @return 当前用途是否属于读取、比较或结果处理 */
    public boolean read() {
        return !write();
    }
}
