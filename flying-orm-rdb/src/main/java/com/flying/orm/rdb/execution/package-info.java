/**
 * SQL 执行保护、结果分类和取消状态模型。
 *
 * <p>{@link com.flying.orm.rdb.execution.SqlExecutionOptions} 在连接可用后统一限制 SQL 执行时间、最大返回行数、
 * 结果内存和 LOB 大小；连接排队和获取超时由上层连接池治理。默认配置可在客户端组装时下沉，单次调用仍可
 * 显式覆盖；更严格的限制不能被内部入口绕过。</p>
 *
 * <p>这里的结果分类用于稳定的业务分支和指标聚合，调用方不应解析异常消息文本判断错误类型。</p>
 */
package com.flying.orm.rdb.execution;
