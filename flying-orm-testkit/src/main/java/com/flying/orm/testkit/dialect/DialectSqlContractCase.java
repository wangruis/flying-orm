package com.flying.orm.testkit.dialect;

import com.flying.orm.rdb.dialect.RdbDialect;

/**
 * 一种数据库方言应该稳定渲染出来的 SQL 样子。
 *
 * @param name          方言名字，报错时能直接看出是哪一家数据库
 * @param dialect       真正参与渲染的方言对象
 * @param createTableSql 动态表单建表 SQL
 * @param pagedSelectSql 带条件、排序、分页的查询 SQL
 * @param upsertSql     按主键插入或更新的 SQL
 * @author wangr
 * @date 2026-07-24
 * @version v1.0
 */
public record DialectSqlContractCase(String name,
                                     RdbDialect dialect,
                                     String createTableSql,
                                     String pagedSelectSql,
                                     String upsertSql) {
}
