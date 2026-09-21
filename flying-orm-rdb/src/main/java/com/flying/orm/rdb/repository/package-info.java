/**
 * 基于实体约定和动态表单能力构建的薄 Repository。
 *
 * <p>Repository 负责实体与 Map 的转换、表名和字段名约定、逻辑删除、乐观锁以及基础 CRUD，不隐藏条件
 * AST、Scope 和执行保护。复杂查询仍可直接使用表单客户端或 Operator，避免 Repository 演变成另一套
 * 重型查询语言。</p>
 *
 * <p>实体映射计划会缓存并并发复用；Repository 本身不保存某次请求的可变条件。</p>
 */
package com.flying.orm.rdb.repository;
