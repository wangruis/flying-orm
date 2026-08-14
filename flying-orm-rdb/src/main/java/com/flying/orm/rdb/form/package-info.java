/**
 * 动态表单的响应式与同步 CRUD 客户端，以及前端结构化条件接入入口。
 *
 * <p>{@link com.flying.orm.rdb.form.ReactiveFormClient} 使用 Reactor/R2DBC 非阻塞链路；
 * {@link com.flying.orm.rdb.form.SyncFormClient} 使用原生 JDBC。查询、分页、更新、删除和批量写入复用相同的
 * 字段校验、Scope、逻辑删除、SQL 渲染和参数顺序。</p>
 *
 * <p>调用方需要忽略空条件时应显式使用可选条件入口，严格条件不会悄悄丢弃空值。</p>
 */
package com.flying.orm.rdb.form;
