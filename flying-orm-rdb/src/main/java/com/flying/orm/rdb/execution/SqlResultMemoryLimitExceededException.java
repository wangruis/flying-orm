package com.flying.orm.rdb.execution;

import com.flying.orm.core.error.OrmErrorReport;
import com.flying.orm.core.error.OrmErrorReportProvider;
import com.flying.orm.rdb.observation.SqlStatementType;

import java.util.Objects;

/**
 * 查询结果累计占用超过执行预算时抛出。
 *
 * <p>这里记录的字节数是面向保护的保守估算，不承诺等于 JVM 对象布局的精确值。异常不携带行内容和参数值，
 * 可以安全地进入日志、指标和上层错误响应。</p>
 *
 * @author wangr
 * @date 2026-08-04
 * @version v1.0
 */
public final class SqlResultMemoryLimitExceededException extends IllegalStateException
        implements OrmErrorReportProvider {

    private static final long serialVersionUID = 1L;

    private final SqlStatementType statementType;

    private final long maxResultBytes;

    private final long attemptedBytes;

    private final long overflowIndex;

    public SqlResultMemoryLimitExceededException(SqlStatementType statementType,
                                                 long maxResultBytes,
                                                 long attemptedBytes,
                                                 long overflowIndex) {
        super("sql result memory exceeds limit: statementType=" + statementType
                + ", maxResultBytes=" + maxResultBytes
                + ", attemptedBytes=" + attemptedBytes
                + ", overflowIndex=" + overflowIndex);
        this.statementType = Objects.requireNonNull(statementType, "SQL statement type must not be null");
        if (maxResultBytes < 0 || attemptedBytes < 0 || overflowIndex < 0) {
            throw new IllegalArgumentException("SQL result memory limit values must not be negative");
        }
        this.maxResultBytes = maxResultBytes;
        this.attemptedBytes = attemptedBytes;
        this.overflowIndex = overflowIndex;
    }

    public SqlStatementType statementType() {
        return statementType;
    }

    public long maxResultBytes() {
        return maxResultBytes;
    }

    public long attemptedBytes() {
        return attemptedBytes;
    }

    public long overflowIndex() {
        return overflowIndex;
    }

    @Override
    public OrmErrorReport toErrorReport() {
        return new OrmErrorReport("EXECUTION",
                                  "RESULT_MEMORY_LIMIT_EXCEEDED",
                                  statementType.name(),
                                  "rows[" + overflowIndex + "]",
                                  null,
                                  getMessage());
    }
}
