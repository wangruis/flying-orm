package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.execution.SqlExecutionOptions;

import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Objects;

/**
 * 把共享执行保护转换成 JDBC Statement 能理解的设置。
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
final class JdbcStatementOptions {

    private JdbcStatementOptions() {
    }

    static void apply(Statement statement, SqlExecutionOptions options) throws SQLException {
        Statement safeStatement = Objects.requireNonNull(statement, "jdbc statement must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        if (!safeOptions.timeout().isZero()) {
            safeStatement.setQueryTimeout(timeoutSeconds(safeOptions.timeout()));
        }
        if (safeOptions.fetchSize() > 0) {
            safeStatement.setFetchSize(safeOptions.fetchSize());
        }
        if (safeOptions.maxRows() > 0) {
            applyMaxRows(safeStatement, safeOptions.maxRows());
        }
    }

    /**
     * 滚动游标没有框架读取循环去发现“第 maxRows + 1 行”，因此这里直接把驱动可见行数封顶为 maxRows。
     * 这样回调即使调用 last()、absolute() 也不会绕过统一的结果集上限。
     */
    static void applyForScrollableCursor(Statement statement, SqlExecutionOptions options) throws SQLException {
        Statement safeStatement = Objects.requireNonNull(statement, "jdbc statement must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        if (!safeOptions.timeout().isZero()) {
            safeStatement.setQueryTimeout(timeoutSeconds(safeOptions.timeout()));
        }
        if (safeOptions.fetchSize() > 0) {
            safeStatement.setFetchSize(safeOptions.fetchSize());
        }
        if (safeOptions.maxRows() > 0) {
            applyMaxRowsExactly(safeStatement, safeOptions.maxRows());
        }
    }

    private static int timeoutSeconds(Duration timeout) {
        long seconds = timeout.toSeconds();
        if (seconds >= Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        // JDBC 只接受整秒。小于一秒也必须保留保护，向上取整为一秒而不是悄悄变成不限时。
        if (timeout.minusSeconds(seconds).isZero()) {
            return (int) seconds;
        }
        return (int) seconds + 1;
    }

    private static void applyMaxRows(Statement statement, long maxRows) throws SQLException {
        // 常见限制都能放进 int，直接走老接口。某些驱动会对 setLargeMaxRows 抛出功能不支持，
        // Hikari 收到这个异常后可能把连接判坏；即使随后回退，连接也已经不能继续执行 SQL。
        if (maxRows < Integer.MAX_VALUE) {
            statement.setMaxRows((int) maxRows + 1);
            return;
        }
        try {
            statement.setLargeMaxRows(maxRows == Long.MAX_VALUE ? maxRows : maxRows + 1L);
        } catch (SQLFeatureNotSupportedException | AbstractMethodError unsupported) {
            // int 已经表达不了这个边界，查询读取循环仍会按 long 限制兜底。
        }
    }

    private static void applyMaxRowsExactly(Statement statement, long maxRows) throws SQLException {
        if (maxRows <= Integer.MAX_VALUE) {
            statement.setMaxRows((int) maxRows);
            return;
        }
        try {
            statement.setLargeMaxRows(maxRows);
        } catch (SQLFeatureNotSupportedException | AbstractMethodError unsupported) {
            // int 版本没有足够表达力时不把限制截断成错误的更小值，读取方仍负责最终边界。
        }
    }
}
