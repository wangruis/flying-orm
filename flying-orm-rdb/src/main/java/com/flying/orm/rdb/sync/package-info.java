/**
 * 原生 JDBC 同步执行契约。
 *
 * <p>{@link com.flying.orm.rdb.sync.SyncSqlExecutor} 由 JDBC 执行器实现，直接使用 Connection、PreparedStatement
 * 和 ResultSet。它与 R2DBC 共享 SQL 渲染、参数顺序、映射和安全规则，但不会创建 Reactor Publisher。</p>
 *
 * <p>同步 JDBC 会占用当前线程，适合普通工作线程或 Java 虚拟线程，不能在 Reactor non-blocking 线程调用。</p>
 */
package com.flying.orm.rdb.sync;
