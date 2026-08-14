package com.flying.orm.rdb.bootstrap;

import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionLogSink;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.schema.SchemaMigrationObserver;
import com.flying.orm.rdb.transaction.JdbcTransactionParticipant;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipant;
import io.r2dbc.spi.ConnectionFactory;

import javax.sql.DataSource;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 上层系统交给 flying-orm 的运行时能力。
 *
 * <p>这里故意把 JDBC 和 R2DBC 分成两条独立的能力线。只配置 JDBC 时，R2DBC 的
 * {@code Optional} 为空；只配置 R2DBC 时，JDBC 的 {@code Optional} 为空。Bootstrap
 * 会据此选择真正的执行内核，任何一条线都不会偷偷通过另一条线执行。</p>
 *
 * <p>物理数据源清单只用于启动期逐个校验方言。清单中的对象由上层系统管理生命周期，
 * flying-orm 不会关闭、替换或重新路由这些数据源。</p>
 *
 * @param jdbcDataSource                  JDBC 主数据源；未配置 JDBC 时为空
 * @param connectionFactory               R2DBC 主连接工厂；未配置 R2DBC 时为空
 * @param physicalJdbcDataSources         用于 JDBC 方言校验的物理数据源清单
 * @param physicalReactiveDataSources     用于 R2DBC 方言校验的物理连接工厂清单
 * @param jdbcTransactionParticipant     上层 JDBC 事务参与者；没有外部事务时为空
 * @param r2dbcTransactionParticipant    上层 R2DBC 事务参与者；没有外部事务时为空
 * @param sqlObserver                     SQL 执行观测器
 * @param batchObserver                   批量执行观测器
 * @param migrationObserver               DDL 迁移观测器
 * @param sqlLogSink                      SQL 日志出口
 * @author wangr
 * @date 2026-08-07
 * @version v2.0
 */
public record FlyingOrmEnvironment(
        Optional<DataSource> jdbcDataSource,
        Optional<ConnectionFactory> connectionFactory,
        Map<String, DataSource> physicalJdbcDataSources,
        Map<String, ConnectionFactory> physicalReactiveDataSources,
        Optional<JdbcTransactionParticipant> jdbcTransactionParticipant,
        Optional<R2dbcTransactionParticipant> r2dbcTransactionParticipant,
        Optional<SqlExecutionObserver> sqlObserver,
        Optional<BatchExecutionObserver> batchObserver,
        Optional<SchemaMigrationObserver> migrationObserver,
        Optional<SqlExecutionLogSink> sqlLogSink) {

    public FlyingOrmEnvironment {
        jdbcDataSource = requireOptional(jdbcDataSource, "jdbc data source");
        connectionFactory = requireOptional(connectionFactory, "r2dbc connection factory");
        physicalJdbcDataSources = immutableJdbcDataSources(physicalJdbcDataSources);
        physicalReactiveDataSources = immutableReactiveDataSources(physicalReactiveDataSources);
        jdbcTransactionParticipant = requireOptional(jdbcTransactionParticipant,
                                                       "jdbc transaction participant");
        r2dbcTransactionParticipant = requireOptional(r2dbcTransactionParticipant,
                                                       "r2dbc transaction participant");
        sqlObserver = requireOptional(sqlObserver, "sql execution observer");
        batchObserver = requireOptional(batchObserver, "batch execution observer");
        migrationObserver = requireOptional(migrationObserver, "schema migration observer");
        sqlLogSink = requireOptional(sqlLogSink, "sql log sink");
        if (sqlObserver.isPresent() != batchObserver.isPresent()) {
            throw new IllegalArgumentException("sql and batch observers must be configured together");
        }
        if (jdbcTransactionParticipant.isPresent() && jdbcDataSource.isEmpty()) {
            throw new IllegalArgumentException("jdbc transaction participant requires a jdbc data source");
        }
        if (r2dbcTransactionParticipant.isPresent() && connectionFactory.isEmpty()) {
            throw new IllegalArgumentException(
                    "r2dbc transaction participant requires an r2dbc connection factory");
        }
    }

    /** 只配置原生 JDBC 内核；不会创建或包装 R2DBC 连接工厂。 */
    public static FlyingOrmEnvironment of(DataSource dataSource) {
        return empty(dataSource, null);
    }

    /** 只配置原生 R2DBC 内核；不会创建或包装 JDBC 数据源。 */
    public static FlyingOrmEnvironment of(ConnectionFactory factory) {
        return empty(null, factory);
    }

    /** 同时配置两条真正独立的执行内核；方言由 Bootstrap 分别校验。 */
    public static FlyingOrmEnvironment of(DataSource dataSource, ConnectionFactory factory) {
        return empty(dataSource, factory);
    }

    /** 为 JDBC 动态路由提供物理数据源快照。数据源本身仍由上层负责路由和关闭。 */
    public FlyingOrmEnvironment withPhysicalJdbcDataSources(
            Map<String, ? extends DataSource> dataSources) {
        return copy(jdbcDataSource, connectionFactory, copyJdbcDataSources(dataSources),
                    physicalReactiveDataSources, jdbcTransactionParticipant,
                    r2dbcTransactionParticipant, sqlObserver, batchObserver,
                    migrationObserver, sqlLogSink);
    }

    /** 为 R2DBC 动态路由提供物理连接工厂快照。不会和 JDBC 清单混用。 */
    public FlyingOrmEnvironment withPhysicalReactiveDataSources(
            Map<String, ? extends ConnectionFactory> dataSources) {
        return copy(jdbcDataSource, connectionFactory, physicalJdbcDataSources,
                    copyReactiveDataSources(dataSources), jdbcTransactionParticipant,
                    r2dbcTransactionParticipant, sqlObserver, batchObserver,
                    migrationObserver, sqlLogSink);
    }

    /** 接入上层 JDBC 事务；ORM 只参与和复用连接，不接管提交、回滚和关闭。 */
    public FlyingOrmEnvironment withTransactionParticipant(JdbcTransactionParticipant participant) {
        return copy(jdbcDataSource, connectionFactory, physicalJdbcDataSources,
                    physicalReactiveDataSources, Optional.of(Objects.requireNonNull(
                            participant, "jdbc transaction participant must not be null")),
                    r2dbcTransactionParticipant, sqlObserver, batchObserver,
                    migrationObserver, sqlLogSink);
    }

    /** 接入上层 R2DBC 事务；ORM 只参与和复用连接，不接管事务生命周期。 */
    public FlyingOrmEnvironment withTransactionParticipant(R2dbcTransactionParticipant participant) {
        return copy(jdbcDataSource, connectionFactory, physicalJdbcDataSources,
                    physicalReactiveDataSources, jdbcTransactionParticipant,
                    Optional.of(Objects.requireNonNull(
                            participant, "r2dbc transaction participant must not be null")),
                    sqlObserver, batchObserver, migrationObserver, sqlLogSink);
    }

    /** 一次设置 SQL 和批量 observer；只允许成对配置，避免观测数据不完整。 */
    public FlyingOrmEnvironment withObservers(SqlExecutionObserver configuredSqlObserver,
                                              BatchExecutionObserver configuredBatchObserver) {
        return copy(jdbcDataSource, connectionFactory, physicalJdbcDataSources,
                    physicalReactiveDataSources, jdbcTransactionParticipant,
                    r2dbcTransactionParticipant,
                    Optional.of(Objects.requireNonNull(configuredSqlObserver,
                                                       "sql execution observer must not be null")),
                    Optional.of(Objects.requireNonNull(configuredBatchObserver,
                                                       "batch execution observer must not be null")),
                    migrationObserver, sqlLogSink);
    }

    /** 添加 DDL 迁移观测器，不改变 SQL 执行和事务所有权。 */
    public FlyingOrmEnvironment withMigrationObserver(SchemaMigrationObserver observer) {
        return copy(jdbcDataSource, connectionFactory, physicalJdbcDataSources,
                    physicalReactiveDataSources, jdbcTransactionParticipant,
                    r2dbcTransactionParticipant, sqlObserver, batchObserver,
                    Optional.of(Objects.requireNonNull(observer, "schema migration observer must not be null")),
                    sqlLogSink);
    }

    /** 设置 SQL 日志出口；是否真的输出由配置中的 SQL 日志开关决定。 */
    public FlyingOrmEnvironment withSqlLogSink(SqlExecutionLogSink sink) {
        return copy(jdbcDataSource, connectionFactory, physicalJdbcDataSources,
                    physicalReactiveDataSources, jdbcTransactionParticipant,
                    r2dbcTransactionParticipant, sqlObserver, batchObserver,
                    migrationObserver, Optional.of(Objects.requireNonNull(sink,
                                                                         "sql log sink must not be null")));
    }

    private static FlyingOrmEnvironment empty(DataSource dataSource, ConnectionFactory factory) {
        return new FlyingOrmEnvironment(Optional.ofNullable(dataSource), Optional.ofNullable(factory),
                                        Map.of(), Map.of(), Optional.empty(), Optional.empty(),
                                        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    private FlyingOrmEnvironment copy(Optional<DataSource> configuredJdbcDataSource,
                                      Optional<ConnectionFactory> configuredConnectionFactory,
                                      Map<String, DataSource> jdbcDataSources,
                                      Map<String, ConnectionFactory> reactiveDataSources,
                                      Optional<JdbcTransactionParticipant> jdbcParticipant,
                                      Optional<R2dbcTransactionParticipant> reactiveParticipant,
                                      Optional<SqlExecutionObserver> configuredSqlObserver,
                                      Optional<BatchExecutionObserver> configuredBatchObserver,
                                      Optional<SchemaMigrationObserver> configuredMigrationObserver,
                                      Optional<SqlExecutionLogSink> configuredSqlLogSink) {
        return new FlyingOrmEnvironment(configuredJdbcDataSource, configuredConnectionFactory,
                                        jdbcDataSources, reactiveDataSources, jdbcParticipant,
                                        reactiveParticipant, configuredSqlObserver,
                                        configuredBatchObserver, configuredMigrationObserver,
                                        configuredSqlLogSink);
    }

    private static Map<String, DataSource> copyJdbcDataSources(
            Map<String, ? extends DataSource> dataSources) {
        Map<String, ? extends DataSource> safe = Objects.requireNonNull(
                dataSources, "physical jdbc data sources must not be null");
        Map<String, DataSource> copy = new LinkedHashMap<>();
        safe.forEach((name, source) -> copy.put(Objects.requireNonNull(name,
                                                                       "physical jdbc data source name must not be null"),
                                                 Objects.requireNonNull(source,
                                                                        "physical jdbc data source must not be null")));
        return Map.copyOf(copy);
    }

    private static Map<String, ConnectionFactory> copyReactiveDataSources(
            Map<String, ? extends ConnectionFactory> dataSources) {
        Map<String, ? extends ConnectionFactory> safe = Objects.requireNonNull(
                dataSources, "physical reactive data sources must not be null");
        Map<String, ConnectionFactory> copy = new LinkedHashMap<>();
        safe.forEach((name, factory) -> copy.put(Objects.requireNonNull(name,
                                                                        "physical reactive data source name must not be null"),
                                                  Objects.requireNonNull(factory,
                                                                         "physical reactive data source must not be null")));
        return Map.copyOf(copy);
    }

    private static Map<String, DataSource> immutableJdbcDataSources(
            Map<String, DataSource> dataSources) {
        return Map.copyOf(Objects.requireNonNull(dataSources,
                                                  "physical jdbc data sources must not be null"));
    }

    private static Map<String, ConnectionFactory> immutableReactiveDataSources(
            Map<String, ConnectionFactory> dataSources) {
        return Map.copyOf(Objects.requireNonNull(dataSources,
                                                  "physical reactive data sources must not be null"));
    }

    private static <T> Optional<T> requireOptional(Optional<T> value, String name) {
        return Objects.requireNonNull(value, name + " optional must not be null");
    }
}
