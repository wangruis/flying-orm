/**
 * 从真实数据库读取动态表单元数据，并提供 Caffeine 缓存和精确失效入口。
 *
 * <p>reader 只负责当前数据源看到的结构；多实例缓存通知、数据源路由和指标框架接入由上层负责。
 * 同一 reader 可以并发复用，同 key 加载会合并，失败不会作为正常元数据缓存。</p>
 */
package com.flying.orm.rdb.metadata;
