package com.flying.orm.rdb.isolation;

import io.r2dbc.spi.ConnectionFactory;

/** 上层按稳定数据库键提供连接工厂；连接池创建、容量和生命周期仍由上层管理。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
@FunctionalInterface
public interface DatabaseConnectionRouter {

    ConnectionFactory route(String databaseKey);
}
