package com.flying.orm.rdb.execution;

import com.flying.orm.core.error.OrmErrorReport;
import com.flying.orm.core.error.OrmErrorReportProvider;
import com.flying.orm.rdb.observation.SqlStatementType;

import java.util.Objects;

/**
 * 查询结果超过上限时抛出。异常里不放参数值，避免日志泄漏业务数据。
 *
 * @author wangr
 * @date 2026-07-30
 * @version v1.0
 */
public final class SqlRowLimitExceededException extends IllegalStateException implements OrmErrorReportProvider {

    private static final long serialVersionUID = 1L;

    private final SqlStatementType statementType;

    private final long maxRows;

    private final long overflowIndex;

    /**
     * @param statementType SQL 类型
     * @param maxRows 允许返回的最大行数
     * @param overflowIndex 首个越界行的零基索引
     */
    public SqlRowLimitExceededException(SqlStatementType statementType, long maxRows, long overflowIndex) {
        super("sql result row count exceeds max rows: statementType=" + statementType
                + ", maxRows=" + maxRows
                + ", overflowIndex=" + overflowIndex);
        this.statementType = Objects.requireNonNull(statementType, "SQL statement type must not be null");
        if (maxRows < 0 || overflowIndex < 0) {
            throw new IllegalArgumentException("SQL row limit and overflow index must not be negative");
        }
        this.maxRows = maxRows;
        this.overflowIndex = overflowIndex;
    }

    /** @return SQL 类型 */
    public SqlStatementType statementType() {
        return statementType;
    }

    /** @return 允许返回的最大行数 */
    public long maxRows() {
        return maxRows;
    }

    /** @return 首个越界行的零基索引 */
    public long overflowIndex() {
        return overflowIndex;
    }

    /** @return 能定位首个越界行，但不会泄露查询参数的统一报告 */
    @Override
    public OrmErrorReport toErrorReport() {
        return new OrmErrorReport("EXECUTION",
                                  "ROW_LIMIT_EXCEEDED",
                                  statementType.name(),
                                  "rows[" + overflowIndex + "]",
                                  null,
                                  getMessage());
    }
}
