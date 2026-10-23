/**
 * 数据库行、动态 Map 和 Java 实体之间的映射规则。
 *
 * <p>{@link com.flying.orm.rdb.mapping.RowMapper#of(Class)} 会在内部预先扫描 record 构造器、Bean setter
 * 和字段，返回可复用的只读计划；客户端通过 EntityModelRegistry 缓存计划，静态入口不使用全局缓存。逐行映射不重复做反射发现。具体计划是
 * 包内实现，不要求业务代码依赖。列名和 Java 属性名会统一归一化，因此 snake_case 列可以映射到
 * camelCase 属性。</p>
 *
 * <p>映射层只负责值和名称转换，不负责连接、事务或 SQL 执行。</p>
 */
package com.flying.orm.rdb.mapping;
