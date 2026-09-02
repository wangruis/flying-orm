package com.flying.orm.rdb.internal.plan;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.internal.dialect.DatabaseProduct;
import com.flying.orm.rdb.internal.dialect.SqlBindMarkerCompiler;
import com.flying.orm.rdb.internal.template.SqlStatements;

import java.util.Objects;

/**
 * SQL 结构进入缓存前唯一执行的单语句校验与驱动参数标记编译入口。
 *
 * @author wangr
 * @date 2026-08-24
 * @version v3.1
 */
@InternalApi
public final class SqlStatementCompiler {

    private SqlStatementCompiler() {
    }

    public static SqlStatementPlan compile(String sql,
                                           int parameterCount,
                                           SqlBindMarkerStyle markerStyle,
                                           String databaseProductName) {
        return compileVerified(sql, parameterCount, markerStyle, databaseProductName);
    }

    static VerifiedSqlStatementPlan compileVerified(String sql,
                                                     int parameterCount,
                                                     SqlBindMarkerStyle markerStyle,
                                                     String databaseProductName) {
        String source = Objects.requireNonNull(sql, "sql must not be null");
        String productName = Objects.requireNonNullElse(databaseProductName, "");
        DatabaseProduct product = DatabaseProduct.detect(productName);
        if (product == DatabaseProduct.UNKNOWN) {
            SqlStatements.requirePortableSingle(source);
        } else {
            SqlStatements.requireSingleForDatabaseProduct(source, productName);
        }
        return new VerifiedSqlStatementPlan(
                source,
                Objects.requireNonNull(markerStyle, "sql bind marker style must not be null"),
                parameterCount,
                product,
                SqlBindMarkerCompiler.compile(source, parameterCount, markerStyle, productName));
    }
}
