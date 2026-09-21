package com.flying.orm.rdb.bootstrap;

import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionLogSink;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.schema.SchemaMigrationObserver;
import com.flying.orm.rdb.reactive.R2dbcConnectionAccess;

import com.flying.orm.rdb.jdbc.JdbcConnectionAccess;
import java.util.Objects;
import java.util.Optional;

/**
 * 上层系统交给 flying-orm 的运行时能力。
 *
 * <p>这里故意把 JDBC 和 R2DBC 分成两条独立的能力线。只配置 JDBC 时，R2DBC 的
 * {@code Optional} 为空；只配置 R2DBC 时，JDBC 的 {@code Optional} 为空。Bootstrap
 * 会据此选择真正的执行内核，任何一条线都不会偷偷通过另一条线执行。</p>
 *
 * @param jdbcConnectionAccess                  上层 JDBC 连接访问端口；未配置 JDBC 时为空
 * @param reactiveConnectionAccess               上层 R2DBC 连接访问端口；未配置 R2DBC 时为空
 * @param sqlObserver                     SQL 执行观测器
 * @param batchObserver                   批量执行观测器
 * @param migrationObserver               DDL 迁移观测器
 * @param sqlLogSink                      SQL 日志出口
 * @author wangr
 * @date 2026-08-07
 * @version v2.0
 */
public record FlyingOrmEnvironment(
        Optional<JdbcConnectionAccess> jdbcConnectionAccess,
        Optional<R2dbcConnectionAccess> reactiveConnectionAccess,
        Optional<SqlExecutionObserver> sqlObserver,
        Optional<BatchExecutionObserver> batchObserver,
        Optional<SchemaMigrationObserver> migrationObserver,
        Optional<SqlExecutionLogSink> sqlLogSink) {

    public FlyingOrmEnvironment {
        jdbcConnectionAccess = requireOptional(jdbcConnectionAccess, "jdbc connection access");
        reactiveConnectionAccess = requireOptional(reactiveConnectionAccess, "r2dbc connection access");
        sqlObserver = requireOptional(sqlObserver, "sql execution observer");
        batchObserver = requireOptional(batchObserver, "batch execution observer");
        migrationObserver = requireOptional(migrationObserver, "schema migration observer");
        sqlLogSink = requireOptional(sqlLogSink, "sql log sink");
        if (sqlObserver.isPresent() != batchObserver.isPresent()) {
            throw new IllegalArgumentException("sql and batch observers must be configured together");
        }
    }

    /** 只配置原生 JDBC 内核；上层负责连接获取和释放。 */
    public static FlyingOrmEnvironment of(JdbcConnectionAccess jdbcAccess) {
        return empty(jdbcAccess, null);
    }

    /** 只配置原生 R2DBC 内核；上层负责连接获取和释放。 */
    public static FlyingOrmEnvironment of(R2dbcConnectionAccess reactiveAccess) {
        return empty(null, reactiveAccess);
    }

    /** 同时配置两条真正独立的执行内核；使用上层显式指定的方言。 */
    public static FlyingOrmEnvironment of(JdbcConnectionAccess jdbcAccess, R2dbcConnectionAccess reactiveAccess) {
        return empty(jdbcAccess, reactiveAccess);
    }

    /** 一次设置 SQL 和批量 observer；只允许成对配置，避免观测数据不完整。 */
    public FlyingOrmEnvironment withObservers(SqlExecutionObserver configuredSqlObserver,
                                              BatchExecutionObserver configuredBatchObserver) {
        return copy(jdbcConnectionAccess, reactiveConnectionAccess,
                    Optional.of(Objects.requireNonNull(configuredSqlObserver,
                                                       "sql execution observer must not be null")),
                    Optional.of(Objects.requireNonNull(configuredBatchObserver,
                                                       "batch execution observer must not be null")),
                    migrationObserver, sqlLogSink);
    }

    /** 添加 DDL 迁移观测器，不改变 SQL 执行和连接所有权。 */
    public FlyingOrmEnvironment withMigrationObserver(SchemaMigrationObserver observer) {
        return copy(jdbcConnectionAccess, reactiveConnectionAccess, sqlObserver, batchObserver,
                    Optional.of(Objects.requireNonNull(observer, "schema migration observer must not be null")),
                    sqlLogSink);
    }

    /** 设置 SQL 日志出口；是否真的输出由配置中的 SQL 日志开关决定。 */
    public FlyingOrmEnvironment withSqlLogSink(SqlExecutionLogSink sink) {
        return copy(jdbcConnectionAccess, reactiveConnectionAccess, sqlObserver, batchObserver,
                    migrationObserver, Optional.of(Objects.requireNonNull(sink,
                                                                         "sql log sink must not be null")));
    }

    private static FlyingOrmEnvironment empty(JdbcConnectionAccess jdbcAccess, R2dbcConnectionAccess reactiveAccess) {
        return new FlyingOrmEnvironment(Optional.ofNullable(jdbcAccess), Optional.ofNullable(reactiveAccess),
                                        Optional.empty(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    private FlyingOrmEnvironment copy(Optional<JdbcConnectionAccess> configuredJdbcAccess,
                                      Optional<R2dbcConnectionAccess> configuredReactiveAccess,
                                      Optional<SqlExecutionObserver> configuredSqlObserver,
                                      Optional<BatchExecutionObserver> configuredBatchObserver,
                                      Optional<SchemaMigrationObserver> configuredMigrationObserver,
                                      Optional<SqlExecutionLogSink> configuredSqlLogSink) {
        return new FlyingOrmEnvironment(configuredJdbcAccess, configuredReactiveAccess,
                                        configuredSqlObserver,
                                        configuredBatchObserver, configuredMigrationObserver,
                                        configuredSqlLogSink);
    }

    private static <T> Optional<T> requireOptional(Optional<T> value, String name) {
        return Objects.requireNonNull(value, name + " optional must not be null");
    }
}
