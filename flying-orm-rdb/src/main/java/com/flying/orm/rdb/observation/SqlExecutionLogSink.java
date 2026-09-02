package com.flying.orm.rdb.observation;

/**
 * 框架无关的日志出口。可以接项目自己的日志门面、文件写入器或测试收集器，flying-orm 不绑定任何日志框架。
 * @author wangr
 * @version v1.0
 */
@FunctionalInterface
public interface SqlExecutionLogSink {

    /**
     * 保留原来的单参数写入入口。老代码用 {@code messages::add} 之类的 Lambda 时仍然可以直接使用。
     */
    void write(String message);

    /**
     * 让上层在真正格式化 SQL 之前决定日志级别是否开启。
     *
     * <p>旧 sink 默认返回 {@code true}，这样不会改变原来“收到日志就写入”的行为；接入业务日志框架时，
     * 可以按自己的 DEBUG/WARN/ERROR 配置覆盖这个方法。</p>
     */
    default boolean isEnabled(SqlExecutionLogLevel level) {
        return true;
    }

    /**
     * 带级别的写入入口。旧 sink 默认回退到 {@link #write(String)}，新 sink 可以覆盖它来真正调用
     * 上层对应级别的日志方法。
     */
    default void write(SqlExecutionLogLevel level, String message) {
        write(message);
    }
}
