package com.flying.orm.rdb.schema;

/**
 * flying-orm 对当前结构方言在线 DDL 的承诺范围。这里描述的是 ORM 敢自动生成什么，
 * 不是数据库产品理论上支持的全部语法。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public enum SchemaOnlineDdlSupport {
    /** 没有经过确认的在线 DDL 写法，要求在线时必须交给外部迁移工具。 */
    NONE,
    /** 可以把普通索引创建安全改写为 PostgreSQL 的 CREATE INDEX CONCURRENTLY。 */
    CONCURRENT_INDEX,
    /** 数据库支持部分在线 alter，但是否可用取决于具体操作、存储引擎或表结构。 */
    OPERATION_DEPENDENT,
    /** 在线能力还受数据库版本、版本授权或部署版本限制，不能由核心模块擅自开启。 */
    LICENSE_OR_EDITION_DEPENDENT
}
