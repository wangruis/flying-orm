package com.flying.orm.rdb.internal.dialect;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.internal.template.SqlLexicalScanner;

import java.util.Objects;

/**
 * 把规范问号参数标记一次性编译为目标 R2DBC 驱动需要的传输形式。
 *
 * @author wangr
 * @date 2026-08-24
 * @version v3.1
 */
@InternalApi
public final class SqlBindMarkerCompiler {

    private SqlBindMarkerCompiler() {
    }

    public static String compile(String sql,
                                 int parameterCount,
                                 SqlBindMarkerStyle markerStyle,
                                 String databaseProductName) {
        String source = Objects.requireNonNull(sql, "sql must not be null");
        SqlBindMarkerStyle safeStyle = Objects.requireNonNull(
                markerStyle, "sql bind marker style must not be null");
        if (parameterCount < 0) {
            throw new IllegalArgumentException("sql parameter count must not be negative");
        }
        if (safeStyle == SqlBindMarkerStyle.NATIVE) {
            return source;
        }
        if (source.indexOf('?') < 0) {
            if (parameterCount != 0) {
                throw markerCountMismatch();
            }
            return source;
        }

        DatabaseProduct product = DatabaseProduct.detect(databaseProductName);
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
