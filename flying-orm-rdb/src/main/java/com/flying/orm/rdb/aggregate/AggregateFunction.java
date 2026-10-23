package com.flying.orm.rdb.aggregate;

/**
 * flying-orm 内置聚合及已注册函数的规划方式。
 *
 * <p>REGISTERED 使用后端函数注册表，不把调用方提供的函数 ID 拼成 SQL。</p>
 *
 * @author wangr
 * @version v3.2
 */
public enum AggregateFunction {
    COUNT_ROWS,
    COUNT,
    COUNT_DISTINCT,
    SUM,
    AVG,
    MIN,
    MAX,
    REGISTERED,
    STDDEV_POP,
    STDDEV_SAMPLE,
    VARIANCE_POP,
    VARIANCE_SAMPLE
}
