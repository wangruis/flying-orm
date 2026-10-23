/**
 * RDB 方言、版本能力和名称解析入口。
 *
 * <p>方言只承接分页、upsert、参数标记、类型和 DDL 等数据库差异，不包含数据源或连接池管理。
 * 上层显式选择方言，也可用 RdbDialectResolver 解析已知名称；本包不获取连接或执行数据库路由。</p>
 */
package com.flying.orm.rdb.dialect;
