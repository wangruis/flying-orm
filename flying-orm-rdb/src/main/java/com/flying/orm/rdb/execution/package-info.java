/**
 * SQL 执行保护、结果分类和取消状态模型。
 *
 * <p>{@link com.flying.orm.rdb.execution.SqlExecutionOptions} 统一限制查询超时、连接获取超时、最大返回行数
 * 和 LOB 大小。默认配置可在客户端组装时下沉，单次调用仍可显式覆盖；更严格的限制不能被内部入口绕过。</p>
 *
 * <p>这里的结果分类用于稳定的业务分支和指标聚合，调用方不应解析异常消息文本判断错误类型。</p>
 */
package com.flying.orm.rdb.execution;
