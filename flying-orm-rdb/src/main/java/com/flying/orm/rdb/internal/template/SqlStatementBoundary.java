package com.flying.orm.rdb.internal.template;

import java.util.Locale;

/**
 * SQL 单语句词法边界，不承担数据库方言语法解析。
 *
 * <p>所有方言统一跳过字符串、标识符、注释和模板槽，拒绝顶层分号后的正文；只有 SQL Server 需要额外识别
 * 不使用分号分隔的批处理。其他数据库的合法语法交给驱动判断，不能由通用关键字状态机猜测。</p>
 *
 * @author wangr
 * @date 2026-08-16
 * @version v2.0
 */
final class SqlStatementBoundary {

    private SqlStatementBoundary() {
    }

    static void validate(String sql,
                          boolean hashLineComments,
                          boolean nestedBlockComments,
                          boolean mysqlDialect,
                          boolean sqlServerDialect,
                          boolean oracleDialect) {
        if (oracleDialect && OracleAnonymousBlockBoundary.startsWithBlock(sql)) {
            OracleAnonymousBlockBoundary.validate(sql);
            return;
        }
        SqlStatementBoundaryState state = sqlServerDialect ? new SqlStatementBoundaryState() : null;
        int depth = 0;
        boolean terminated = false;
        boolean hasContent = false;
        for (int index = 0; index < sql.length();) {
            if (terminated) {
                index = trailingTriviaEnd(sql, index, hashLineComments, nestedBlockComments, mysqlDialect);
                continue;
            }
            int alternativeQuoteEnd = SqlStatements.oracleAlternativeQuoteEnd(sql, index);
            if (alternativeQuoteEnd >= 0) {
                hasContent = true;
                index = alternativeQuoteEnd;
                continue;
            }
            char current = sql.charAt(index);
            if (current == '\'' || current == '"' || current == '`') {
                hasContent = true;
                index = quotedEnd(sql, index, current);
            } else if (current == '[') {
                hasContent = true;
                index = bracketIdentifierEnd(sql, index);
            } else if (isDoubleDashCommentStart(sql, index, mysqlDialect)) {
                index = lineCommentEnd(sql, index + 2);
            } else if (current == '#' && hashLineComments) {
                index = lineCommentEnd(sql, index + 1);
            } else if (current == '/' && index + 1 < sql.length() && sql.charAt(index + 1) == '*') {
                index = blockCommentEnd(sql, index + 2, nestedBlockComments);
            } else if (current == '$' && index + 1 < sql.length() && sql.charAt(index + 1) == '{') {
                hasContent = true;
                index = templateSlotEnd(sql, index + 2);
            } else if (current == '$') {
                int dollarEnd = dollarQuotedEnd(sql, index);
                hasContent = true;
                index = dollarEnd < 0 ? index + 1 : dollarEnd;
            } else if ((current == ':' || current == '@') && index + 1 < sql.length()
                    && isWordStart(sql.charAt(index + 1))) {
                hasContent = true;
                index = wordEnd(sql, index + 1);
            } else if (current == '(') {
                hasContent = true;
                depth++;
                index++;
            } else if (current == ')') {
                hasContent = true;
                depth = Math.max(0, depth - 1);
                index++;
            } else if (current == ';') {
                if (depth != 0) {
                    throw multipleStatements();
                }
                requireContent(state, hasContent);
                terminated = true;
                index++;
            } else if (depth == 0 && current == ',') {
                hasContent = true;
                if (state != null) {
                    state.comma();
                }
                index++;
            } else if (depth == 0 && isWordStart(current)) {
                int end = wordEnd(sql, index);
                hasContent = true;
                if (state != null) {
                    state.accept(sql.substring(index, end).toUpperCase(Locale.ROOT),
                                 mysqlDialect, true);
                }
                index = end;
            } else {
                if (!Character.isWhitespace(current)) {
                    hasContent = true;
                }
                index++;
            }
        }
        requireContent(state, hasContent);
    }

    private static void requireContent(SqlStatementBoundaryState state, boolean hasContent) {
        if (state != null) {
            state.terminate();
        } else if (!hasContent) {
            throw multipleStatements();
        }
    }

    private static int trailingTriviaEnd(String sql,
                                         int offset,
                                         boolean hashLineComments,
                                         boolean nestedBlockComments,
                                         boolean mysqlDialect) {
        char current = sql.charAt(offset);
        if (Character.isWhitespace(current)) {
            return offset + 1;
        }
        if (isDoubleDashCommentStart(sql, offset, mysqlDialect)) {
            return lineCommentEnd(sql, offset + 2);
        }
        if (current == '#' && hashLineComments) {
            return lineCommentEnd(sql, offset + 1);
        }
        if (current == '/' && offset + 1 < sql.length() && sql.charAt(offset + 1) == '*') {
            return blockCommentEnd(sql, offset + 2, nestedBlockComments);
        }
        throw multipleStatements();
    }

    private static boolean isDoubleDashCommentStart(String sql, int offset, boolean mysqlDialect) {
        if (sql.charAt(offset) != '-' || offset + 1 >= sql.length() || sql.charAt(offset + 1) != '-') {
            return false;
        }
        if (!mysqlDialect || offset + 2 >= sql.length()) {
            return true;
        }
        char following = sql.charAt(offset + 2);
        return Character.isWhitespace(following) || Character.isISOControl(following);
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

    private static int bracketIdentifierEnd(String sql, int offset) {
        for (int index = offset + 1; index < sql.length(); index++) {
            if (sql.charAt(index) == ']') {
                if (index + 1 < sql.length() && sql.charAt(index + 1) == ']') {
                    index++;
                } else {
                    return index + 1;
                }
            }
        }
        throw new IllegalArgumentException("SQL contains an unclosed bracket-quoted identifier");
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

    private static int blockCommentEnd(String sql, int offset, boolean nestedBlockComments) {
        int depth = 1;
        for (int index = offset; index + 1 < sql.length(); index++) {
            char current = sql.charAt(index);
            char next = sql.charAt(index + 1);
            if (nestedBlockComments && current == '/' && next == '*') {
                depth++;
                index++;
            } else if (current == '*' && next == '/') {
                depth--;
                index++;
                if (depth == 0) {
                    return index + 1;
                }
            }
        }
        throw new IllegalArgumentException("SQL contains an unclosed block comment");
    }

    private static int templateSlotEnd(String sql, int offset) {
        int end = sql.indexOf('}', offset);
        if (end < 0) {
            throw new IllegalArgumentException("SQL contains an unclosed template slot");
        }
        return end + 1;
    }

    private static int dollarQuotedEnd(String sql, int offset) {
        if (offset > 0 && isWordPart(sql.charAt(offset - 1))) {
            return -1;
        }
        int tagEnd = offset + 1;
        while (tagEnd < sql.length() && isWordPart(sql.charAt(tagEnd))) {
            tagEnd++;
        }
        if (tagEnd >= sql.length() || sql.charAt(tagEnd) != '$') {
            return -1;
        }
        String delimiter = sql.substring(offset, tagEnd + 1);
        int end = sql.indexOf(delimiter, tagEnd + 1);
        if (end < 0) {
            throw new IllegalArgumentException("SQL contains an unclosed dollar-quoted value");
        }
        return end + delimiter.length();
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

    static boolean isAlterColumnDropClause(String word) {
        return switch (word) {
            case "COMPRESSION", "DEFAULT", "EXPRESSION", "IDENTITY", "NOT" -> true;
            default -> false;
        };
    }

    static boolean isAlterAction(String word) {
        return switch (word) {
            case "ADD", "RENAME", "MODIFY", "CHANGE", "ENABLE", "DISABLE", "RESET", "ATTACH", "DETACH" -> true;
            default -> false;
        };
    }

    static boolean isSqlServerDropObject(String word) {
        return switch (word) {
            case "TABLE", "VIEW", "INDEX", "PROCEDURE", "PROC", "FUNCTION", "TRIGGER", "TYPE", "SEQUENCE",
                    "SYNONYM", "SCHEMA", "DATABASE", "USER", "ROLE" -> true;
            default -> false;
        };
    }

    static boolean isStatementStart(String word, boolean sqlServerDialect) {
        if (sqlServerDialect && switch (word) {
            case "ADD", "BREAK", "CONTINUE", "DUMP", "GOTO", "IF", "LOAD", "READTEXT", "RECEIVE", "REVERT",
                    "SEND", "SETUSER", "UPDATETEXT", "WHILE", "WRITETEXT" -> true;
            default -> false;
        }) {
            return true;
        }
        return switch (word) {
            case "SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "MERGE", "REPLACE", "UPSERT", "CALL",
                    "CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT", "REVOKE", "COMMENT", "EXPLAIN", "SHOW",
                    "DESCRIBE", "DESC", "SET", "VALUES", "EXEC", "EXECUTE", "BEGIN", "COMMIT", "ROLLBACK",
                    "WAITFOR", "USE", "DECLARE", "BACKUP", "BULK", "CHECKPOINT", "DBCC", "DENY", "KILL",
                    "PRINT", "RAISERROR", "RECONFIGURE", "RESTORE", "RETURN", "SAVE", "SHUTDOWN", "THROW",
                    "OPEN", "CLOSE", "DEALLOCATE", "FETCH", "ENABLE", "DISABLE" -> true;
            default -> false;
        };
    }

    private static IllegalArgumentException multipleStatements() {
        return new IllegalArgumentException("SQL must contain exactly one statement");
    }

}
