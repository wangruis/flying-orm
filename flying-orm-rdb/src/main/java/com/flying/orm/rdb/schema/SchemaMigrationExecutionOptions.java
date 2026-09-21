package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.execution.SqlExecutionOptions;

import java.util.Objects;

/**
 * DDL 执行容量和精确计划批准。执行与会话锁等待时限全部由上层管理。
 *
 * @param sqlExecutionOptions SQL 执行容量等选项
 * @param approval 可选的精确计划批准
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public record SchemaMigrationExecutionOptions(SqlExecutionOptions sqlExecutionOptions,
                                              SchemaMigrationApproval approval) {

    public SchemaMigrationExecutionOptions {
        sqlExecutionOptions = Objects.requireNonNull(sqlExecutionOptions,
                                                     "schema SQL execution options must not be null");
    }

    /** 创建不携带危险计划批准的默认配置，计划风险审核继续由 Schema 执行入口负责。 */
    public static SchemaMigrationExecutionOptions defaults() {
        return new SchemaMigrationExecutionOptions(SqlExecutionOptions.safeDefaults(), null);
    }

    /** 带上当前审核计划的精确批准，执行前仍核对计划指纹。 */
    public SchemaMigrationExecutionOptions withApproval(SchemaMigrationApproval approval) {
        return new SchemaMigrationExecutionOptions(sqlExecutionOptions,
                Objects.requireNonNull(approval, "schema migration approval must not be null"));
    }
}
