package com.flying.orm.rdb.bootstrap;

import java.util.Objects;

/**
 * 把类型化配置和上层运行环境装成统一的 flying-orm 客户端对象图。
 *
 * <p>这个入口只在启动时工作，不进入 SQL 热路径。它先判断上层到底提供了哪条执行内核，
 * 再调用对应的 Builder：JDBC 走 JDBC，R2DBC 走 R2DBC，两者都提供时才组装双内核对象图。
 * 没有配置任何内核时直接失败，避免启动成功后第一次业务调用才暴露问题。</p>
 *
 * <p>配置、observer、迁移观测和 SQL 日志仍然由同一个 Builder 链路处理。这样上层框架只需
 * 负责把自己的配置和连接访问端口放进 {@link FlyingOrmEnvironment}，不会复制一套 ORM 装配规则。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v2.0
 */
public final class FlyingOrmBootstrap {

    private FlyingOrmBootstrap() {
    }

    /**
     * 完成启动校验并创建可共享的客户端对象图。
     *
     * <p>方言必须由调用方显式指定，构建过程不获取连接。具体连接来源和释放策略由上层端口实现。</p>
     *
     * @param configuration 类型化启动配置
     * @param environment   上层提供的连接访问端口和观测出口
     * @return 统一的客户端对象图
     */
    public static FlyingOrmClients create(FlyingOrmConfiguration configuration,
                                          FlyingOrmEnvironment environment) {
        FlyingOrmConfiguration safeConfiguration = Objects.requireNonNull(
                configuration, "flying-orm configuration must not be null");
        FlyingOrmEnvironment safeEnvironment = Objects.requireNonNull(
                environment, "flying-orm environment must not be null");

        FlyingOrmClientBuilder builder = selectBuilder(safeEnvironment);
        configureCommon(builder, safeConfiguration);
        configureObservers(builder, safeConfiguration, safeEnvironment);
        return builder.build();
    }

    /** 根据明确存在的连接访问端口选择执行内核，不允许隐式跨内核桥接。 */
    private static FlyingOrmClientBuilder selectBuilder(FlyingOrmEnvironment environment) {
        if (environment.jdbcConnectionAccess().isPresent() && environment.reactiveConnectionAccess().isPresent()) {
            return FlyingOrmClients.builder(environment.jdbcConnectionAccess().orElseThrow(),
                                            environment.reactiveConnectionAccess().orElseThrow());
        }
        if (environment.jdbcConnectionAccess().isPresent()) {
            return FlyingOrmClients.builder(environment.jdbcConnectionAccess().orElseThrow());
        }
        if (environment.reactiveConnectionAccess().isPresent()) {
            return FlyingOrmClients.builder(environment.reactiveConnectionAccess().orElseThrow());
        }
        throw new IllegalStateException("flying-orm requires a JDBC or R2DBC connection access port");
    }

    /** 把与具体内核无关的执行保护、缓存和迁移设置一次性下沉给 Builder。 */
    private static void configureCommon(FlyingOrmClientBuilder builder,
                                        FlyingOrmConfiguration configuration) {
        builder.executionOptions(configuration.executionOptions())
               .batchWriteOptions(configuration.batchWriteOptions())
               .batchMemoryLimits(configuration.batchMemoryLimits())
               .cachePolicy(configuration.cachePolicy())
               .migrationExecutionOptions(configuration.migrationExecutionOptions());
        if (configuration.dialect() != null) {
            builder.configuredDialect(configuration.dialect());
        }
    }

    /** 保留 SQL observer、批量 observer、迁移 observer 和安全 SQL 日志配置。 */
    private static void configureObservers(FlyingOrmClientBuilder builder,
                                           FlyingOrmConfiguration configuration,
                                           FlyingOrmEnvironment environment) {
        if (environment.sqlObserver().isPresent()) {
            builder.observers(environment.sqlObserver().orElseThrow(),
                              environment.batchObserver().orElseThrow());
        }
        environment.migrationObserver().ifPresent(builder::migrationObserver);
        if (configuration.sqlLog().enabled()) {
            builder.sqlExecutionLog(configuration.sqlLog().options(),
                                    configuration.sqlLog().selection(),
                                    environment.sqlLogSink().orElseThrow(() -> new IllegalStateException(
                                            "SQL logging is enabled but no SQL log sink was provided")));
        }
    }
}
