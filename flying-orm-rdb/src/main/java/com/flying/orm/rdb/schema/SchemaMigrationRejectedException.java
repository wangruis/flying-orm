package com.flying.orm.rdb.schema;

import com.flying.orm.core.error.OrmErrorReport;
import com.flying.orm.core.error.OrmErrorReportProvider;

import java.util.Objects;

/**
 * 迁移在 SQL 下发前被安全策略拒绝。这个异常代表“没有执行”，与数据库执行到一半后的失败要分开处理。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class SchemaMigrationRejectedException extends IllegalStateException implements OrmErrorReportProvider {

    private final SchemaMigrationFailureCode failureCode;

    private final String planFingerprint;

    public SchemaMigrationRejectedException(SchemaMigrationFailureCode failureCode, String message) {
        this(failureCode, null, message);
    }

    public SchemaMigrationRejectedException(SchemaMigrationFailureCode failureCode,
                                            String planFingerprint,
                                            String message) {
        super(Objects.requireNonNull(message, "schema migration rejection message must not be null"));
        this.failureCode = Objects.requireNonNull(failureCode, "schema migration failure code must not be null");
        this.planFingerprint = planFingerprint;
        if (failureCode == SchemaMigrationFailureCode.NONE) {
            throw new IllegalArgumentException("rejected migration failure code must not be NONE");
        }
    }

    /** @return 可以直接用于业务分支、接口错误码和审计记录的稳定迁移失败码 */
    public SchemaMigrationFailureCode failureCode() {
        return failureCode;
    }

    /** @return 被拒绝计划的指纹；执行器能力不足等尚未绑定计划的错误可能为空 */
    public String planFingerprint() {
        return planFingerprint;
    }

    /** @return 上层可以直接转换成 HTTP 或 RPC 错误的迁移报告 */
    @Override
    public OrmErrorReport toErrorReport() {
        return new OrmErrorReport("MIGRATION",
                                  failureCode.name(),
                                  planFingerprint,
                                  null,
                                  null,
                                  getMessage());
    }
}
