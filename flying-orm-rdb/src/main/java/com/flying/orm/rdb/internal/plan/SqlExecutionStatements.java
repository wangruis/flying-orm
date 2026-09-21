package com.flying.orm.rdb.internal.plan;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.internal.dialect.DatabaseProduct;

import java.util.Objects;

/**
 * 统一解析可信计划或在不可信公共执行边界完成一次编译。
 *
 * @author wangr
 * @version v1.0
 */
@InternalApi
public final class SqlExecutionStatements {

    private SqlExecutionStatements() {
    }

    /** Validate the request for the actual database and return its canonical JDBC SQL. */
    public static String canonical(SqlRequest request, String databaseProductName) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        return canonical(safeRequest.statement(), databaseProductName);
    }

    /** Validate the statement for the actual database and return its canonical JDBC SQL. */
    public static String canonical(SqlStatementPlan statement, String databaseProductName) {
        return verified(statement, databaseProductName).sql();
    }

    /** Validate the request and return the bind-marker form required by the R2DBC driver. */
    public static String r2dbc(SqlRequest request, String databaseProductName) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        return r2dbc(safeRequest.statement(), databaseProductName);
    }

    /** Validate the statement and return the bind-marker form required by the R2DBC driver. */
    public static String r2dbc(SqlStatementPlan statement, String databaseProductName) {
        VerifiedSqlStatementPlan verified = verified(statement, databaseProductName);
        DatabaseProduct product = DatabaseProduct.detect(databaseProductName);
        return verified.transportSql(product.name()).orElseThrow(
                () -> new IllegalStateException("verified SQL plan must contain transport SQL"));
    }

    private static VerifiedSqlStatementPlan verified(SqlStatementPlan statement,
                                                      String databaseProductName) {
        SqlStatementPlan safeStatement = Objects.requireNonNull(
                statement, "sql statement plan must not be null");
        DatabaseProduct product = DatabaseProduct.detect(databaseProductName);
        if (safeStatement instanceof VerifiedSqlStatementPlan verified && verified.matches(product)) {
            return verified;
        }
        return SqlStatementCompiler.compileVerified(
                safeStatement.sql(),
                safeStatement.parameterCount(),
                safeStatement.bindMarkerStyle(),
                databaseProductName);
    }
}
