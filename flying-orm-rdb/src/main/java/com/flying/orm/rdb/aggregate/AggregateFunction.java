package com.flying.orm.rdb.aggregate;

/**
 * flying-orm 内置且可跨方言验证的常用聚合函数。
 *
 * <p>这里是封闭集合，不接受函数名字符串。窗口函数、厂商分析函数和任意 SQL 不借此入口进入。</p>
 *
 * @author wangr
 * @version v3.2
 */
public enum AggregateFunction {
    COUNT,
    COUNT_DISTINCT,
    SUM,
    AVG,
    MIN,
    MAX
}
