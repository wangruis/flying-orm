package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.execution.SqlExecutionOptions;

import java.time.Duration;
import java.util.Objects;

/**
 * DDL 执行阶段的保护选项。SQL timeout 复用统一执行保护，approval 只负责确认不可自动恢复的风险。
 *
 * @param sqlExecutionOptions 每条 DDL 的执行保护
 * @param approval 可选的精确计划批准
 * @param lockTimeout 数据库会话等待表锁或元数据锁的最长时间，0 表示不开启会话保护
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record SchemaMigrationExecutionOptions(SqlExecutionOptions sqlExecutionOptions,
                                              SchemaMigrationApproval approval,
                                              Duration lockTimeout) {

    public SchemaMigrationExecutionOptions(SqlExecutionOptions sqlExecutionOptions,
                                           SchemaMigrationApproval approval) {
        this(sqlExecutionOptions, approval, Duration.ZERO);
    }

    public SchemaMigrationExecutionOptions {
        sqlExecutionOptions = Objects.requireNonNull(sqlExecutionOptions,
                                                     "schema SQL execution options must not be null");
        lockTimeout = Objects.requireNonNull(lockTimeout, "schema lock timeout must not be null");
        if (lockTimeout.isNegative()) {
            throw new IllegalArgumentException("schema lock timeout must not be negative");
        }
    }

    /**
     * 创建带执行、连接获取和数据库锁等待上限且不携带危险计划批准的默认配置。
     * DDL 允许比普通 SQL 更长的 60 秒执行时间，但最多等待连接和数据库锁各 10 秒；
     * 有回滚缺口的计划仍会在执行前被拒绝。
     *
     * @return 默认 DDL 执行配置
     */
    public static SchemaMigrationExecutionOptions defaults() {
        SqlExecutionOptions ddlOptions = SqlExecutionOptions.safeDefaults()
                                                           .withTimeout(Duration.ofSeconds(60))
                                                           .withConnectionAcquireTimeout(Duration.ofSeconds(10));
        return new SchemaMigrationExecutionOptions(ddlOptions, null, Duration.ofSeconds(10));
    }

    /**
     * 给计划中的每一条 DDL 设置执行超时。超时保护沿用统一 SQL 执行模型，不会在结构模块里另造计时线程。
     *
     * @param timeout 单条 DDL 允许执行的最长时间
     * @return 保留其他设置的新配置
     */
    public SchemaMigrationExecutionOptions withTimeout(Duration timeout) {
        return new SchemaMigrationExecutionOptions(sqlExecutionOptions.withTimeout(timeout), approval, lockTimeout);
    }

    /**
     * 带上当前审核计划的精确批准。执行前还会重新核对指纹，旧计划的批准不能拿来执行新计划。
     *
     * @param approval 精确到计划指纹的批准对象
     * @return 保留其他设置的新配置
     */
    public SchemaMigrationExecutionOptions withApproval(SchemaMigrationApproval approval) {
        return new SchemaMigrationExecutionOptions(sqlExecutionOptions,
                                                    Objects.requireNonNull(approval,
                                                                           "schema migration approval must not be null"),
                                                    lockTimeout);
    }

    /**
     * 限制数据库等待表锁或元数据锁的时间。这个配置要求执行器支持同连接 sequence，
     * 因为设置、DDL 和恢复会话缺一不可。
     */
    public SchemaMigrationExecutionOptions withLockTimeout(Duration lockTimeout) {
        return new SchemaMigrationExecutionOptions(sqlExecutionOptions, approval, lockTimeout);
    }

    public boolean hasLockTimeout() {
        return !lockTimeout.isZero();
    }
}
