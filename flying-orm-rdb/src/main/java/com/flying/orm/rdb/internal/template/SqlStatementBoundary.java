package com.flying.orm.rdb.internal.template;

import java.util.Locale;

/**
 * SQL 单语句语法边界。引用、注释和方言文本区域统一由 {@link SqlLexicalScanner} 识别；这里仅判断顶层
 * 分号和 SQL Server 无分号批处理是否仍属于同一条语句。
 */
final class SqlStatementBoundary {

    private SqlStatementBoundary() {
    }

    static void validate(String sql,
                         SqlLexicalScanner.Rules lexicalRules,
                         boolean sqlServerDialect,
                         boolean oracleDialect) {
        if (oracleDialect && OracleAnonymousBlockBoundary.startsWithBlock(sql)) {
            OracleAnonymousBlockBoundary.validate(sql);
            return;
        }
        BoundaryValidator validator = new BoundaryValidator(sql, sqlServerDialect);
        SqlLexicalScanner.scan(sql, lexicalRules, true, validator::accept);
        validator.finish();
    }

    private static final class BoundaryValidator {
        private final String sql;
        private final SqlStatementBoundaryState state;
        private int depth;
        private boolean terminated;
        private boolean hasContent;

        private BoundaryValidator(String sql, boolean sqlServerDialect) {
            this.sql = sql;
            this.state = sqlServerDialect ? new SqlStatementBoundaryState() : null;
        }

        private void accept(SqlLexicalScanner.SegmentKind kind, int start, int end) {
            if (kind == SqlLexicalScanner.SegmentKind.LINE_COMMENT
                    || kind == SqlLexicalScanner.SegmentKind.BLOCK_COMMENT) {
                return;
            }
            if (kind != SqlLexicalScanner.SegmentKind.CODE) {
                if (terminated) {
                    throw multipleStatements();
                }
                hasContent = true;
                return;
            }
            acceptCode(start, end);
        }

        private void acceptCode(int start, int end) {
            for (int index = start; index < end;) {
                char current = sql.charAt(index);
                if (terminated) {
                    if (!Character.isWhitespace(current)) {
                        throw multipleStatements();
                    }
                    index++;
                } else if ((current == ':' || current == '@') && index + 1 < end
                        && isWordStart(sql.charAt(index + 1))) {
                    hasContent = true;
                    index = wordEnd(sql, index + 1, end);
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
                    requireContent();
                    terminated = true;
                    index++;
                } else if (depth == 0 && current == ',') {
                    hasContent = true;
                    if (state != null) {
                        state.comma();
                    }
                    index++;
                } else if (depth == 0 && isWordStart(current)) {
                    int wordEnd = wordEnd(sql, index, end);
                    hasContent = true;
                    if (state != null) {
                        state.accept(sql.substring(index, wordEnd).toUpperCase(Locale.ROOT), false, true);
                    }
                    index = wordEnd;
                } else {
                    if (!Character.isWhitespace(current)) {
                        hasContent = true;
                    }
                    index++;
                }
            }
        }

        private void requireContent() {
            if (state != null) {
                state.terminate();
            } else if (!hasContent) {
                throw multipleStatements();
            }
        }

        private void finish() {
            requireContent();
        }
    }

    private static int wordEnd(String sql, int offset, int limit) {
        int end = offset + 1;
        while (end < limit && isWordPart(sql.charAt(end))) {
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
