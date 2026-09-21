/**
 * 基于 Reactor 和 R2DBC 的真正非阻塞 SQL 执行内核。
 *
 * <p>{@link com.flying.orm.rdb.reactive.ReactiveSqlExecutor} 是执行契约，
 * {@link com.flying.orm.rdb.reactive.R2dbcSqlExecutor} 调用上层连接端口并负责参数绑定、查询、写入和批量协调。
 * 执行链路不调用 {@code block()}，上层释放端口、取消和异常翻译都在 Publisher 生命周期中组合完成；
 * ORM 不直接关闭外部连接。</p>
 *
 * <p>上层需要同步调用时使用原生 JDBC 的 sync 入口。同步与响应式链路共享 SQL 和安全规则，但不共享
 * 数据库执行线程或连接；不要在响应式事件循环里调用任何同步 JDBC 入口。</p>
 */
package com.flying.orm.rdb.reactive;
