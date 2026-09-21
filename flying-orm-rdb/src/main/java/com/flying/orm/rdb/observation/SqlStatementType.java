package com.flying.orm.rdb.observation;

import com.flying.orm.rdb.internal.template.SqlLexicalScanner;

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

    private static final SqlLexicalScanner.Rules LEXICAL_RULES = SqlLexicalScanner.genericRules();

    public static SqlStatementType fromSql(String sql) {
        if (sql == null || sql.isBlank()) {
            return UNKNOWN;
        }
        int start;
        try {
            start = firstCodeOffset(sql);
        } catch (IllegalArgumentException ignored) {
            // 观测分类不能把损坏或方言未知的 SQL 从执行链改写成 ORM 失败。
            return UNKNOWN;
        }
        int end = start;
        while (end < sql.length() && Character.isLetter(sql.charAt(end))) {
            end++;
        }
        if (matches(sql, start, end, "select") || matches(sql, start, end, "with")) {
            return SELECT;
        }
        if (matches(sql, start, end, "insert")) {
            return INSERT;
        }
        if (matches(sql, start, end, "update")) {
            return UPDATE;
        }
        if (matches(sql, start, end, "delete")) {
            return DELETE;
        }
        if (matches(sql, start, end, "merge")) {
            return MERGE;
        }
        if (matches(sql, start, end, "create")) {
            return CREATE;
        }
        if (matches(sql, start, end, "alter")) {
            return ALTER;
        }
        if (matches(sql, start, end, "drop")) {
            return DROP;
        }
        return matches(sql, start, end, "truncate") ? TRUNCATE : UNKNOWN;
    }

    private static int firstCodeOffset(String sql) {
        int offset = 0;
        while (offset < sql.length()) {
            while (offset < sql.length() && Character.isWhitespace(sql.charAt(offset))) {
                offset++;
            }
            if (offset >= sql.length()) {
                return offset;
            }
            long segment = SqlLexicalScanner.protectedSegmentAt(sql, offset, LEXICAL_RULES, false);
            if (segment >= 0L) {
                SqlLexicalScanner.SegmentKind kind = SqlLexicalScanner.segmentKind(segment);
                if (kind == SqlLexicalScanner.SegmentKind.LINE_COMMENT
                        || kind == SqlLexicalScanner.SegmentKind.BLOCK_COMMENT) {
                    offset = SqlLexicalScanner.segmentEnd(segment);
                    continue;
                }
            }
            break;
        }
        return offset;
    }

    private static boolean matches(String sql, int start, int end, String keyword) {
        return end - start == keyword.length() && sql.regionMatches(true, start, keyword, 0, keyword.length());
    }
}
