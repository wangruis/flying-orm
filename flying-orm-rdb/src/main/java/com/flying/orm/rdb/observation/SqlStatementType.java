package com.flying.orm.rdb.observation;

import java.util.Locale;

/**
 * 从 SQL 文本里粗略识别语句类型，用来做指标维度，不参与 SQL 语义判断。
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public enum SqlStatementType {
    SELECT,
    INSERT,
    UPDATE,
    DELETE,
    MERGE,
    CREATE,
    ALTER,
    DROP,
    TRUNCATE,
    UNKNOWN;

    public static SqlStatementType fromSql(String sql) {
        if (sql == null || sql.isBlank()) {
            return UNKNOWN;
        }
        String normalized = trimLeadingComments(sql);
        if (normalized.isEmpty()) {
            return UNKNOWN;
        }
        int end = 0;
        while (end < normalized.length() && Character.isLetter(normalized.charAt(end))) {
            end++;
        }
        if (end == 0) {
            return UNKNOWN;
        }
        return switch (normalized.substring(0, end).toLowerCase(Locale.ROOT)) {
            case "select", "with" -> SELECT;
            case "insert" -> INSERT;
            case "update" -> UPDATE;
            case "delete" -> DELETE;
            case "merge" -> MERGE;
            case "create" -> CREATE;
            case "alter" -> ALTER;
            case "drop" -> DROP;
            case "truncate" -> TRUNCATE;
            default -> UNKNOWN;
        };
    }

    private static String trimLeadingComments(String sql) {
        int offset = 0;
        while (offset < sql.length()) {
            while (offset < sql.length() && Character.isWhitespace(sql.charAt(offset))) {
                offset++;
            }
            if (sql.startsWith("--", offset)) {
                int lineEnd = sql.indexOf('\n', offset + 2);
                if (lineEnd < 0) {
                    return "";
                }
                offset = lineEnd + 1;
                continue;
            }
            if (sql.startsWith("/*", offset)) {
                int commentEnd = sql.indexOf("*/", offset + 2);
                if (commentEnd < 0) {
                    return "";
                }
                offset = commentEnd + 2;
                continue;
            }
            break;
        }
        return sql.substring(offset).trim();
    }
}
