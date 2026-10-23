package com.flying.orm.rdb.internal.plan;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.rdb.internal.dialect.DatabaseProduct;
import com.flying.orm.rdb.internal.template.SqlStatements;

import java.util.Objects;

/**
 * SQL 结构进入缓存前唯一执行的单语句校验与驱动参数标记编译入口。
 *
 * @author wangr
 * @date 2026-08-24
 * @version v3.1
 */
public final class SqlStatementCompiler {

    private SqlStatementCompiler() {
    }

    public static SqlStatementPlan compile(String sql,
                                           int parameterCount,
                                           SqlBindMarkerStyle markerStyle,
                                           String databaseProductName) {
        return compileVerified(sql, parameterCount, markerStyle, databaseProductName);
    }

    /** 保留渲染器给出的 INSERT 结构边界，不从任意 SQL 文本猜测可重复的 VALUES 部分。 */
    public static SqlStatementPlan insert(String prefix, String tuple, int parameterCount,
                                           String databaseProductName) {
        String safePrefix = Objects.requireNonNull(prefix, "insert prefix must not be null").stripLeading();
        String safeTuple = Objects.requireNonNull(tuple, "insert tuple must not be null").stripTrailing();
        VerifiedSqlStatementPlan statement = compileVerified(safePrefix + safeTuple, parameterCount,
                SqlBindMarkerStyle.CANONICAL, databaseProductName);
        return new VerifiedSqlStatementPlan(statement, safePrefix.length(), DatabaseProduct.detect(databaseProductName));
    }

    /** 由支持返回输入序号的 UPSERT 渲染器保留结构来源，普通编译不会取得该能力。 */
    public static SqlStatementPlan upsert(String sql, int parameterCount, String databaseProductName) {
        VerifiedSqlStatementPlan statement = compileVerified(sql, parameterCount,
                SqlBindMarkerStyle.CANONICAL, databaseProductName);
        return new VerifiedSqlStatementPlan(statement, DatabaseProduct.detect(databaseProductName), true, false);
    }

    /** 保留冲突忽略语义；侧索引只能跟随实际插入的输入行。 */
    public static SqlStatementPlan ignoreConflicts(String sql, int parameterCount,
                                                   String databaseProductName, boolean batchRowIndex) {
        VerifiedSqlStatementPlan statement = compileVerified(sql, parameterCount,
                SqlBindMarkerStyle.CANONICAL, databaseProductName);
        return new VerifiedSqlStatementPlan(statement, DatabaseProduct.detect(databaseProductName), batchRowIndex, true);
    }

    static VerifiedSqlStatementPlan compileVerified(String sql,
                                                     int parameterCount,
                                                     SqlBindMarkerStyle markerStyle,
                                                     String databaseProductName) {
        String source = Objects.requireNonNull(sql, "sql must not be null");
        String productName = Objects.requireNonNullElse(databaseProductName, "");
        DatabaseProduct product = DatabaseProduct.detect(productName);
        SqlBindMarkerStyle safeStyle = Objects.requireNonNull(
                markerStyle, "sql bind marker style must not be null");
        return new VerifiedSqlStatementPlan(
                source,
                safeStyle,
                parameterCount,
                product,
                SqlStatements.compile(source, parameterCount, safeStyle, product));
    }
}
