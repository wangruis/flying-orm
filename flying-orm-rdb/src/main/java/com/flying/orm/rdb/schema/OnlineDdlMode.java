package com.flying.orm.rdb.schema;

/** 决定迁移计划怎样对待可能持有表锁的 DDL。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public enum OnlineDdlMode {
    ALLOW_BLOCKING,
    PREFER_ONLINE,
    REQUIRE_ONLINE
}
