package com.flying.orm.rdb.internal.plan;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.internal.dialect.DatabaseProduct;
import com.flying.orm.rdb.internal.template.SqlLexicalScanner;
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
        SqlBindMarkerStyle safeStyle = Objects.requireNonNull(
                markerStyle, "sql bind marker style must not be null");
        return new VerifiedSqlStatementPlan(
                source,
                safeStyle,
                parameterCount,
                product,
                compileBindMarkers(source, parameterCount, safeStyle, productName, product));
    }

    /** 在单语句边界内完成唯一一次参数标记编译，不再公开第二个编译入口。 */
    private static String compileBindMarkers(String source,
                                             int parameterCount,
                                             SqlBindMarkerStyle markerStyle,
                                             String databaseProductName,
                                             DatabaseProduct product) {
        if (parameterCount < 0) {
            throw new IllegalArgumentException("sql parameter count must not be negative");
        }
        if (markerStyle == SqlBindMarkerStyle.NATIVE) {
            return source;
        }
        if (source.indexOf('?') < 0) {
            if (parameterCount != 0) {
                throw markerCountMismatch();
            }
            return source;
        }

        SqlLexicalScanner.Rules rules = SqlLexicalScanner.rulesFor(databaseProductName);
        StringBuilder adapted = new StringBuilder(source.length() + parameterCount * 2);
        int markerIndex = 0;
        for (int index = 0; index < source.length();) {
            long protectedSegment = SqlLexicalScanner.protectedSegmentAt(
                    source, index, rules, false);
            if (protectedSegment >= 0L) {
                int end = SqlLexicalScanner.segmentEnd(protectedSegment);
                adapted.append(source, index, end);
                index = end;
                continue;
            }
            if (source.charAt(index) == '?') {
                appendMarker(adapted, markerIndex++, product);
            } else {
                adapted.append(source.charAt(index));
            }
            index++;
        }
        if (markerIndex != parameterCount) {
            throw markerCountMismatch();
        }
        return adapted.toString();
    }

    private static void appendMarker(StringBuilder sql,
                                     int markerIndex,
                                     DatabaseProduct product) {
        switch (product) {
            case POSTGRESQL -> sql.append('$').append(markerIndex + 1);
            case SQL_SERVER -> sql.append("@P").append(markerIndex);
            default -> sql.append('?');
        }
    }

    private static IllegalArgumentException markerCountMismatch() {
        return new IllegalArgumentException(
                "sql parameter marker count does not match parameter count");
    }
}
