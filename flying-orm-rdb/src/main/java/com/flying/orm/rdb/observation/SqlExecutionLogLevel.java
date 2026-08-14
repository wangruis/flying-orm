package com.flying.orm.rdb.observation;

/**
 * SQL 执行日志的严重程度。
 *
 * <p>这个枚举只描述日志应该投到哪个级别，不绑定具体日志框架。上层可以把它映射到自己的
 * DEBUG、WARN、ERROR 日志方法，也可以在日志没有打开时提前拒绝格式化，避免为一条不会输出的
 * SQL 创建字符串和参数展示内容。</p>
 *
 * @author wangr
 * @version v1.0
 */
public enum SqlExecutionLogLevel {

    /** 普通成功的快速 SQL。 */
    DEBUG,

    /** 慢 SQL，以及取消、超时、连接故障和结果未知的执行。 */
    WARN,

    /** 其他已经明确失败的执行。 */
    ERROR
}
