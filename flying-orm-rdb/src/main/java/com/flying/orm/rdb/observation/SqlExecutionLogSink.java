package com.flying.orm.rdb.observation;

/**
 * 框架无关的日志出口。可以接项目自己的日志门面或测试收集器，flying-orm 不绑定任何日志框架。
 *
 * <p>所有方法均在 SQL 执行链路的当前线程同步调用，实现必须快速、非阻塞。文件或网络 I/O
 * 应由上层日志设施异步处理，ORM 不为日志出口创建线程、队列或调度器。</p>
 * @author wangr
 * @version v1.0
 */
@FunctionalInterface
public interface SqlExecutionLogSink {

    /** 写入指定级别的日志；级别由执行事实和调用方筛选配置决定。 */
    void write(SqlExecutionLogLevel level, String message);

    /** 格式化之前检查日志级别；无需筛选的出口默认接收全部级别。 */
    default boolean isEnabled(SqlExecutionLogLevel level) {
        return true;
    }
}
