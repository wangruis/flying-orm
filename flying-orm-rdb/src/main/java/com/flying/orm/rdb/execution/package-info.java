/**
 * SQL 执行保护、结果分类和取消状态模型。
 *
 * <p>{@link com.flying.orm.rdb.execution.SqlExecutionOptions} 保留最大返回行数、结果内存和 LOB 大小边界，
 * 以及驱动预取提示。语句、跨步骤执行、清理和连接获取等所有时间政策由上层或基础设施治理。
 * 结果容量和预取的默认配置可在客户端组装时下沉。</p>
 *
 * <p>这里的结果分类用于稳定的业务分支和指标聚合，调用方不应解析异常消息文本判断错误类型。</p>
 */
package com.flying.orm.rdb.execution;
