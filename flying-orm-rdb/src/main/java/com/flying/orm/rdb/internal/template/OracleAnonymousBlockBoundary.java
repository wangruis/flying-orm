package com.flying.orm.rdb.internal.template;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Locale;

/**
 * Oracle {@code BEGIN ... END;} 或带简单声明区的 {@code DECLARE ... BEGIN ... END;} 匿名块单语句词法边界。
 *
 * <p>匿名块内部允许过程语句分号，并只跟踪 {@code BEGIN}/{@code IF}/{@code LOOP}/{@code CASE} 的结束范围；
 * 外层 {@code END;} 完成后只允许空白和注释。具体 PL/SQL 语法仍由 Oracle 驱动校验。</p>
 *
 * @author wangr
 * @date 2026-08-17
 * @version v2.0
 */
final class OracleAnonymousBlockBoundary {

    private OracleAnonymousBlockBoundary() {
    }

    static boolean startsWithBlock(String sql) {
        String firstWord = firstWord(sql);
        return "BEGIN".equals(firstWord) || "DECLARE".equals(firstWord);
    }

    private static String firstWord(String sql) {
        int offset = triviaEnd(sql, 0);
        if (offset >= sql.length() || !isWordStart(sql.charAt(offset))) {
            return "";
        }
        int end = wordEnd(sql, offset);
        return sql.substring(offset, end).toUpperCase(Locale.ROOT);
    }

    static void validate(String sql) {
        Deque<String> scopes = new ArrayDeque<>();
        boolean declarationPreamble = "DECLARE".equals(firstWord(sql));
        boolean leadingDeclare = declarationPreamble;
        boolean pendingEnd = false;
        boolean endLabel = false;
        boolean completed = false;
        for (int index = 0; index < sql.length();) {
            int next = triviaEnd(sql, index);
            if (next != index) {
                index = next;
                continue;
            }
            if (completed) {
                throw multipleStatements();
            }
            int alternativeQuoteEnd = SqlStatements.oracleAlternativeQuoteEnd(sql, index);
            if (alternativeQuoteEnd >= 0) {
                index = alternativeQuoteEnd;
                continue;
            }
            char current = sql.charAt(index);
            if (current == '\'' || current == '"') {
                index = quotedEnd(sql, index, current);
            } else if (current == ';') {
                if (pendingEnd) {
                    closeScope(scopes);
                    pendingEnd = false;
                }
                endLabel = false;
                completed = scopes.isEmpty() && !declarationPreamble;
                index++;
            } else if (isWordStart(current)) {
                int end = wordEnd(sql, index);
                String word = sql.substring(index, end).toUpperCase(Locale.ROOT);
                if (leadingDeclare && "DECLARE".equals(word)) {
                    leadingDeclare = false;
                } else if (pendingEnd) {
                    closePendingEnd(scopes, word);
                    pendingEnd = false;
                    endLabel = true;
                } else if (endLabel) {
                    throw multipleStatements();
                } else if ("END".equals(word)) {
                    pendingEnd = true;
                } else if (isScopeStart(word)) {
                    scopes.push(word);
                    if ("BEGIN".equals(word)) {
                        declarationPreamble = false;
                    }
                }
                index = end;
            } else {
                if (endLabel && !Character.isWhitespace(current)) {
                    throw multipleStatements();
                }
                index++;
            }
        }
        if (!completed || pendingEnd || endLabel || !scopes.isEmpty()) {
            throw multipleStatements();
        }
    }

    private static void closePendingEnd(Deque<String> scopes, String word) {
        if (isEndModifier(word)) {
            if (scopes.isEmpty() || !word.equals(scopes.peek())) {
                throw multipleStatements();
            }
            scopes.pop();
            return;
        }
        closeScope(scopes);
    }

    private static void closeScope(Deque<String> scopes) {
        if (scopes.isEmpty()) {
            throw multipleStatements();
        }
        scopes.pop();
    }

    private static boolean isScopeStart(String word) {
        return "BEGIN".equals(word) || "IF".equals(word) || "LOOP".equals(word) || "CASE".equals(word);
    }

    private static boolean isEndModifier(String word) {
        return "IF".equals(word) || "LOOP".equals(word) || "CASE".equals(word);
    }

    private static int triviaEnd(String sql, int offset) {
        int index = offset;
        while (index < sql.length()) {
            char current = sql.charAt(index);
            if (Character.isWhitespace(current)) {
                index++;
            } else if (current == '-' && index + 1 < sql.length() && sql.charAt(index + 1) == '-') {
                index = lineCommentEnd(sql, index + 2);
            } else if (current == '/' && index + 1 < sql.length() && sql.charAt(index + 1) == '*') {
                index = blockCommentEnd(sql, index + 2);
            } else {
                return index;
            }
        }
        return index;
    }

    private static int quotedEnd(String sql, int offset, char quote) {
        for (int index = offset + 1; index < sql.length(); index++) {
            if (sql.charAt(index) != quote) {
                continue;
            }
            if (index + 1 < sql.length() && sql.charAt(index + 1) == quote) {
                index++;
            } else {
                return index + 1;
            }
        }
        throw new IllegalArgumentException("SQL contains an unclosed quoted value or identifier");
    }

    private static int lineCommentEnd(String sql, int offset) {
        for (int index = offset; index < sql.length(); index++) {
            char current = sql.charAt(index);
            if (current == '\n' || current == '\r') {
                return index + 1;
            }
        }
        return sql.length();
    }

    private static int blockCommentEnd(String sql, int offset) {
        for (int index = offset; index + 1 < sql.length(); index++) {
            if (sql.charAt(index) == '*' && sql.charAt(index + 1) == '/') {
                return index + 2;
            }
        }
        throw new IllegalArgumentException("SQL contains an unclosed block comment");
    }

    private static int wordEnd(String sql, int offset) {
        int end = offset + 1;
        while (end < sql.length() && isWordPart(sql.charAt(end))) {
            end++;
        }
        return end;
    }

    private static boolean isWordStart(char value) {
        return Character.isLetter(value) || value == '_';
    }

    private static boolean isWordPart(char value) {
        return Character.isLetterOrDigit(value) || value == '_';
    }

    private static IllegalArgumentException multipleStatements() {
        return new IllegalArgumentException("SQL must contain exactly one statement");
    }
}
