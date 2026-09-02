package com.flying.orm.rdb.reactive;

import io.r2dbc.spi.Connection;

/** 普通 SQL 与批量事务归还连接时共享的最小资源事实。 */
interface R2dbcConnectionLease {

    Connection connection();

    boolean external();

    R2dbcLargeObjectScope largeObjects();

    R2dbcLargeObjectScope largeObjectsIfCreated();
}
