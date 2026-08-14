/**
 * 纯 Java 的 ORM 统一组装入口。
 *
 * <p>{@link com.flying.orm.rdb.bootstrap.FlyingOrmClients} 根据 {@code ConnectionFactory} 元数据选择方言，
 * 并组装执行器、动态表单客户端、Schema 客户端、Repository 和 Operator。普通业务只需要保存组装后的
 * 客户端，不必在每次查询时重复创建渲染器或显式传方言。</p>
 *
 * <p>这个包不依赖应用框架。上层容器可以注册组装结果，但自动配置和动态数据源管理不进入 ORM 内核。</p>
 */
package com.flying.orm.rdb.bootstrap;
