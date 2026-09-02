package com.flying.orm.rdb.schema;

/** 内置数据库设置 DDL/锁等待超时的会话语法。它只是 SchemaDialect 的实现细节，不属于业务 API。 */
enum SchemaLockTimeoutStyle {
    NONE,
    MYSQL,
    POSTGRESQL,
    ORACLE,
    SQL_SERVER
}
