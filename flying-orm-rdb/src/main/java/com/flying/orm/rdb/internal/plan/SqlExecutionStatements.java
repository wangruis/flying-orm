package com.flying.orm.rdb.internal.plan;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.rdb.internal.dialect.DatabaseProduct;

import java.util.Objects;

/**
 * 统一解析可信计划或在不可信公共执行边界完成一次编译。
 *
 * @author wangr
 * @version v1.0
 */
public final class SqlExecutionStatements {

    private SqlExecutionStatements() {
    }

    /** 编译器保留的冲突忽略语义，不从 SQL 文本推测。 */
    public static boolean ignoresConflicts(SqlStatementPlan statement) {
        return statement instanceof VerifiedSqlStatementPlan verified && verified.ignoreConflicts;
    }

    /** Validate the request for the actual database and return its canonical JDBC SQL. */
    public static String canonical(SqlRequest request, String databaseProductName) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        return canonical(safeRequest.statement(), databaseProductName);
    }

    /** Validate the statement for the actual database and return its canonical JDBC SQL. */
    public static String canonical(SqlStatementPlan statement, String databaseProductName) {
        return verified(statement, databaseProductName, DatabaseProduct.detect(databaseProductName)).sql();
    }

    /** Validate the request and return the bind-marker form required by the R2DBC driver. */
    public static String r2dbc(SqlRequest request, String databaseProductName) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        return r2dbc(safeRequest.statement(), databaseProductName);
    }

    /** Validate the statement and return the bind-marker form required by the R2DBC driver. */
    public static String r2dbc(SqlStatementPlan statement, String databaseProductName) {
        DatabaseProduct product = DatabaseProduct.detect(databaseProductName);
        VerifiedSqlStatementPlan verified = verified(statement, databaseProductName, product);
        return verified.transportSql(product.name()).orElseThrow(
                () -> new IllegalStateException("verified SQL plan must contain transport SQL"));
    }

    /** 只展开本库 INSERT 渲染器保留的结构；其余请求沿用驱动参数批次。 */
    public static String r2dbcInsertBatch(SqlStatementPlan statement, String databaseProductName, int rows) {
        DatabaseProduct product = DatabaseProduct.detect(databaseProductName);
        if (rows < 2 || product != DatabaseProduct.POSTGRESQL
                || !(statement instanceof VerifiedSqlStatementPlan verified)
                || !verified.matches(product) || verified.insertValuesOffset < 0) return null;
        // PostgreSQL 协议的参数上限，不限制开发者的输入缓冲；超出时使用原驱动批次。
        if ((long) rows * statement.parameterCount() > 65_535) return null;
        String source = verified.sql();
        StringBuilder sql = new StringBuilder();
        sql.append(source, 0, verified.insertValuesOffset);
        for (int row = 0; row < rows; row++) {
            if (row > 0) sql.append(", ");
            sql.append(source, verified.insertValuesOffset, source.length());
        }
        return r2dbc(SqlStatementCompiler.compile(sql.toString(), rows * statement.parameterCount(),
                statement.bindMarkerStyle(), databaseProductName), databaseProductName);
    }

    /** 原生单行 UPSERT 每组参数返回自己的序号，不依赖驱动的 Result 分组或顺序。 */
    public static String r2dbcUpsertBatch(SqlStatementPlan statement, String databaseProductName) {
        DatabaseProduct product = DatabaseProduct.detect(databaseProductName);
        if (product != DatabaseProduct.POSTGRESQL || !(statement instanceof VerifiedSqlStatementPlan verified)
                || !verified.matches(product) || !verified.batchRowIndex) return null;
        return verified.transportSql(product.name()).orElseThrow()
                + " returning cast($" + (statement.parameterCount() + 1) + " as integer)";
    }

    /** JDBC 通过显式 RETURNING 的结果集取得输入序号，不能从 SUCCESS_NO_INFO 猜对应关系。 */
    public static String jdbcUpsertBatch(SqlStatementPlan statement, String databaseProductName) {
        DatabaseProduct product = DatabaseProduct.detect(databaseProductName);
        if (product != DatabaseProduct.POSTGRESQL || !(statement instanceof VerifiedSqlStatementPlan verified)
                || !verified.matches(product) || !verified.batchRowIndex) return null;
        return verified.sql() + " returning cast(? as integer)";
    }

    private static VerifiedSqlStatementPlan verified(SqlStatementPlan statement,
                                                      String databaseProductName,
                                                      DatabaseProduct product) {
        SqlStatementPlan safeStatement = Objects.requireNonNull(
                statement, "sql statement plan must not be null");
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
