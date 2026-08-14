package com.flying.orm.rdb.dialect;

/**
 * 上层可以查询的数据库能力。它描述的是 flying-orm 在当前版本配置下敢于生成什么 SQL，
 * 不是数据库厂商宣传页里的全部功能。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public enum DialectFeature {
    OFFSET_FETCH_PAGINATION,
    MERGE_UPSERT,
    IDENTITY_COLUMNS,
    SEQUENCES,
    JSON_FUNCTIONS,
    NATIVE_JSON,
    NATIVE_BOOLEAN,
    LARGE_OBJECTS,
    /** PostgreSQL pgvector 的 VECTOR 类型、距离条件和最近邻查询。 */
    POSTGRESQL_VECTOR
}
