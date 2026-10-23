package com.flying.orm.rdb.internal.template;

import java.util.Objects;

/**
 * 原生 DDL 默认表达式的词法边界。只处理引用、括号和语句边界，不判断函数的业务语义或副作用。
 *
 * @author wangr
 * @version v4.1.2
 */
public final class SchemaDefaultExpressions {

    private SchemaDefaultExpressions() {
    }

    /** 校验一个表达式并去掉完整包围它的括号，不改写内部 SQL 或引用文本。 */
    public static String normalize(String expression, String dialectName) {
        String sql = Objects.requireNonNull(expression, "default expression must not be null").trim();
        SqlLexicalScanner.Rules rules = SqlLexicalScanner.rulesFor(dialectName);
        int depth = 0;
        int arrayDepth = 0;
        int wrappers = 0;
        boolean prefix = true;
        int removable = Integer.MAX_VALUE;
        for (int index = 0; index < sql.length(); index++) {
            long segment = SqlLexicalScanner.protectedSegmentAt(sql, index, rules, false);
            if (segment >= 0) {
                SqlLexicalScanner.SegmentKind kind = SqlLexicalScanner.segmentKind(segment);
                if (kind == SqlLexicalScanner.SegmentKind.LINE_COMMENT
                        || kind == SqlLexicalScanner.SegmentKind.BLOCK_COMMENT) {
                    throw invalid();
                }
                prefix = false;
                removable = Math.min(removable, depth);
                index = SqlLexicalScanner.segmentEnd(segment) - 1;
                continue;
            }
            char character = sql.charAt(index);
            if (Character.isWhitespace(character)) {
                continue;
            }
            if (character == '(') {
                if (!prefix) {
                    removable = Math.min(removable, depth);
                }
                depth++;
                if (prefix) {
                    wrappers++;
                }
            } else if (character == ')') {
                if (--depth < 0) {
                    throw invalid();
                }
            } else {
                if (character == '[') {
                    arrayDepth++;
                } else if (character == ']' && --arrayDepth < 0) {
                    throw invalid();
                }
                if (character == ';' || character == ',' && depth == 0 && arrayDepth == 0) {
                    throw invalid();
                }
                prefix = false;
                removable = Math.min(removable, depth);
            }
        }
        if (depth != 0 || arrayDepth != 0 || prefix) {
            throw invalid();
        }
        int count = Math.min(wrappers, removable);
        if (count == 0) {
            return sql;
        }
        int start = 0;
        int end = sql.length();
        for (int index = 0; index < count; index++) {
            while (Character.isWhitespace(sql.charAt(start))) start++;
            while (Character.isWhitespace(sql.charAt(end - 1))) end--;
            start++;
            end--;
        }
        return sql.substring(start, end).trim();
    }

    private static IllegalArgumentException invalid() {
        return new IllegalArgumentException("default SQL must be one balanced expression without comments or statements");
    }
}
