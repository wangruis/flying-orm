package com.flying.orm.rdb.execution;

/**
 * 查询在连接选择前就能公开的路由意图。
 *
 * <p>flying-orm 只描述约束，不选择数据源，也不实现主从路由。</p>
 *
 * @author wangr
 * @version v3.2
 */
public enum QueryRoutingIntent {
    DEFAULT,
    PRIMARY_REQUIRED
}
