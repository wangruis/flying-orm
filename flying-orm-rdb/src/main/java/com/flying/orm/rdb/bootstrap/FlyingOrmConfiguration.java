package com.flying.orm.rdb.bootstrap;

import com.flying.orm.rdb.batch.BatchMemoryLimits;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.observation.SqlExecutionLogOptions;
import com.flying.orm.rdb.observation.SqlExecutionLogSelection;
import com.flying.orm.rdb.schema.SchemaMigrationExecutionOptions;

import java.util.Objects;

/**
 * 所有 Java 生态共享的 flying-orm 启动配置。
 *
 * <p>它不读取 YAML、Properties 或环境变量。应用容器和纯 Java 程序只需把自己的
 * 配置系统绑定到这份不可变数据，再交给 {@link FlyingOrmBootstrap}。这样不同框架不会各自解释超时、批量、缓存
 * 或日志默认值，也不会在适配层复制 ORM 装配逻辑。</p>
 *
 * <p>{@code dialect} 为空表示从统一运行入口自动识别；有值时直接采用显式配置，物理数据源拓扑由上层治理。
 * SQL 日志默认关闭，完整 SQL 和参数仍由 {@link SqlExecutionLogOptions} 单独控制。</p>
 *
 * @param dialect                  显式方言名，{@code null} 表示自动识别
 * @param executionOptions         普通 SQL 默认执行保护
 * @param batchWriteOptions        默认批量策略，默认 ATOMIC
 * @param batchMemoryLimits        进程级批量硬上限
 * @param cachePolicy              元数据、SQL 计划、条件计划和实体映射缓存策略
 * @param migrationExecutionOptions DDL 默认执行保护
 * @param sqlLog                   SQL 执行日志开关与安全展示选项
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
public record FlyingOrmConfiguration(String dialect,
                                     SqlExecutionOptions executionOptions,
                                     BatchWriteOptions batchWriteOptions,
                                     BatchMemoryLimits batchMemoryLimits,
                                     OrmCachePolicy cachePolicy,
                                     SchemaMigrationExecutionOptions migrationExecutionOptions,
                                     SqlLog sqlLog) {

    public FlyingOrmConfiguration {
        dialect = normalizeDialect(dialect);
        executionOptions = Objects.requireNonNull(executionOptions, "sql execution options must not be null");
        batchWriteOptions = Objects.requireNonNull(batchWriteOptions, "batch write options must not be null");
        batchMemoryLimits = Objects.requireNonNull(batchMemoryLimits, "batch memory limits must not be null");
        cachePolicy = Objects.requireNonNull(cachePolicy, "cache policy must not be null");
        migrationExecutionOptions = Objects.requireNonNull(
                migrationExecutionOptions, "schema migration execution options must not be null");
        sqlLog = Objects.requireNonNull(sqlLog, "sql log configuration must not be null");
        // 单次批量配置不能在启动时突破进程硬边界，否则第一次业务调用才报错会让配置问题难以定位。
        batchMemoryLimits.check(batchWriteOptions);
    }

    /** 返回与现有纯 Java Builder 相同的安全默认配置。 */
    public static FlyingOrmConfiguration defaults() {
        return new FlyingOrmConfiguration(null,
                                          SqlExecutionOptions.safeDefaults(),
                                          BatchWriteOptions.defaults(),
                                          BatchMemoryLimits.defaults(),
                                          OrmCachePolicy.safeDefaults(),
                                          SchemaMigrationExecutionOptions.defaults(),
                                          SqlLog.disabled());
    }

    /** 返回显式指定方言的新配置；传空白文本等价于恢复自动识别。 */
    public FlyingOrmConfiguration withDialect(String value) {
        return new FlyingOrmConfiguration(value, executionOptions, batchWriteOptions,
                                          batchMemoryLimits, cachePolicy, migrationExecutionOptions, sqlLog);
    }

    /** 返回带 SQL 日志配置的新值；日志出口由运行环境提供，配置对象不绑定日志框架。 */
    public FlyingOrmConfiguration withSqlLog(SqlLog value) {
        return new FlyingOrmConfiguration(dialect, executionOptions, batchWriteOptions,
                                          batchMemoryLimits, cachePolicy, migrationExecutionOptions, value);
    }

    private static String normalizeDialect(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        return value.trim();
    }

    /**
     * SQL 日志是否启用，以及启用后如何安全展示 SQL 和参数。
     *
     * @param enabled 是否安装日志 observer
     * @param options 脱敏、截断和展示范围；即使关闭也保留非空默认值，方便配置系统稳定绑定
     * @param selection 行数、耗时、慢 SQL 和批量事件的筛选策略
     */
    public record SqlLog(boolean enabled,
                         SqlExecutionLogOptions options,
                         SqlExecutionLogSelection selection) {

        public SqlLog {
            options = Objects.requireNonNull(options, "sql execution log options must not be null");
            selection = Objects.requireNonNull(selection, "sql execution log selection must not be null");
        }

        /** 保留 V1.0.0 的简短构造方式，未指定筛选项时完整记录原有字段和事件。 */
        public SqlLog(boolean enabled, SqlExecutionLogOptions options) {
            this(enabled, options, SqlExecutionLogSelection.defaults());
        }

        public static SqlLog disabled() {
            return new SqlLog(false, SqlExecutionLogOptions.defaults());
        }

        public static SqlLog enabled(SqlExecutionLogOptions options) {
            return new SqlLog(true, options);
        }

        /** 开启日志并明确指定内容与事件筛选策略。 */
        public static SqlLog enabled(SqlExecutionLogOptions options, SqlExecutionLogSelection selection) {
            return new SqlLog(true, options, selection);
        }
    }
}
