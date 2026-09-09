package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.execution.SqlExecutionOptions;

import java.time.Duration;
import java.util.Objects;

/**
 * DDL 执行选项。执行与会话锁等待时限由上层管理，approval 只负责确认不可自动恢复的风险。
 *
 * @param sqlExecutionOptions SQL 执行容量等选项，迁移执行 timeout 仅接受 ZERO
 * @param approval 可选的精确计划批准
 * @param lockTimeout 兼容参数，仅接受 ZERO；会话锁等待由上层管理
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
        if (sqlExecutionOptions.timeout().compareTo(Duration.ZERO) > 0) {
            throw new UnsupportedOperationException(
                    "schema migration execution timeout must be managed by the caller");
        }
        if (!lockTimeout.isZero()) {
            throw new UnsupportedOperationException(
                    "schema session lock timeout must be managed by the caller");
        }
    }

    /**
     * 创建不携带治理预算和危险计划批准的默认配置。
     * DDL 执行和数据库会话锁等待均为 ZERO；有回滚缺口的计划仍会在执行前被拒绝。
     *
     * @return 默认 DDL 执行配置
     */
    public static SchemaMigrationExecutionOptions defaults() {
        SqlExecutionOptions ddlOptions = SqlExecutionOptions.safeDefaults().withTimeout(Duration.ZERO);
        return new SchemaMigrationExecutionOptions(ddlOptions, null, Duration.ZERO);
    }

    /**
     * 保留旧执行超时入口，但迁移时限由上层管理，仅接受 ZERO。
     *
     * @param timeout 兼容执行预算，只能为 ZERO
     * @return 保留其他设置的新配置
     * @throws UnsupportedOperationException timeout 为正
     * @throws IllegalArgumentException timeout 为负
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
     * 保留旧会话锁等待入口，但会话治理由上层管理，仅接受 ZERO。
     *
     * @throws UnsupportedOperationException lockTimeout 为正
     * @throws IllegalArgumentException lockTimeout 为负
     */
    public SchemaMigrationExecutionOptions withLockTimeout(Duration lockTimeout) {
        return new SchemaMigrationExecutionOptions(sqlExecutionOptions, approval, lockTimeout);
    }

    public boolean hasLockTimeout() {
        return !lockTimeout.isZero();
    }
}
