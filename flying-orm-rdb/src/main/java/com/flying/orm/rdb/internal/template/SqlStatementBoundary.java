package com.flying.orm.rdb.internal.template;

import com.flying.orm.rdb.internal.dialect.DatabaseProduct;

import java.util.Locale;

/**
 * SQL 单语句语法边界及参数标记。二者消费同一次词法扫描，引用和注释中的问号不参与绑定。
 */
final class SqlStatementBoundary {

    private SqlStatementBoundary() {
    }

    static String compile(String sql,
                         SqlLexicalScanner.Rules lexicalRules,
                         DatabaseProduct product,
                         int parameterCount,
                         boolean templateSlots) {
        boolean oracleBlock = product == DatabaseProduct.ORACLE && OracleAnonymousBlockBoundary.startsWithBlock(sql);
        if (oracleBlock) {
            OracleAnonymousBlockBoundary.validate(sql);
            if (parameterCount < 0) {
                return sql;
            }
        }
        BoundaryValidator validator = new BoundaryValidator(sql, product, parameterCount, oracleBlock);
        SqlLexicalScanner.scan(sql, lexicalRules, templateSlots, validator::accept);
        return validator.finish();
    }

    private static final class BoundaryValidator {
        private final String sql;
        private final SqlStatementBoundaryState state;
        private final int parameterCount;
        private final boolean oracleBlock;
        private final boolean postgresql;
        private final StringBuilder adapted;
        private int markerIndex;
        private int literalStart;
        private int depth;
        private boolean terminated;
        private boolean hasContent;

        private BoundaryValidator(String sql, DatabaseProduct product, int parameterCount, boolean oracleBlock) {
            this.sql = sql;
            this.state = product == DatabaseProduct.SQL_SERVER ? new SqlStatementBoundaryState() : null;
            this.parameterCount = parameterCount;
            this.oracleBlock = oracleBlock;
            this.postgresql = product == DatabaseProduct.POSTGRESQL;
            this.adapted = parameterCount > 0 && (postgresql || product == DatabaseProduct.SQL_SERVER)
                    ? new StringBuilder(sql.length() + parameterCount * 2) : null;
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
            if (oracleBlock) {
                for (int index = sql.indexOf('?', start); index >= 0 && index < end;
                     index = sql.indexOf('?', index + 1)) {
                    marker(index);
                }
                return;
            }
            for (int index = start; index < end;) {
                char current = sql.charAt(index);
                if (terminated) {
                    if (!Character.isWhitespace(current)) {
                        throw multipleStatements();
                    }
                    index++;
                } else if (current == '?') {
                    hasContent = true;
                    marker(index++);
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
                        state.accept(sql.substring(index, wordEnd).toUpperCase(Locale.ROOT));
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

        private void marker(int offset) {
            if (adapted != null) {
                adapted.append(sql, literalStart, offset);
                if (postgresql) {
                    adapted.append('$').append(markerIndex + 1);
                } else {
                    adapted.append("@P").append(markerIndex);
                }
                literalStart = offset + 1;
            }
            markerIndex++;
        }

        private String finish() {
            if (!oracleBlock) {
                requireContent();
            }
            if (parameterCount >= 0 && markerIndex != parameterCount) {
                throw new IllegalArgumentException("sql parameter marker count does not match parameter count");
            }
            return adapted == null ? sql : adapted.append(sql, literalStart, sql.length()).toString();
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

    static boolean isStatementStart(String word) {
        return switch (word) {
            case "ADD", "BREAK", "CONTINUE", "DUMP", "GOTO", "IF", "LOAD", "READTEXT", "RECEIVE", "REVERT",
                    "SEND", "SETUSER", "UPDATETEXT", "WHILE", "WRITETEXT",
                    "SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "MERGE", "UPSERT", "CALL",
                    "CREATE", "ALTER", "DROP", "TRUNCATE", "GRANT", "REVOKE", "COMMENT", "EXPLAIN", "SHOW",
                    "DESCRIBE", "SET", "VALUES", "EXEC", "EXECUTE", "BEGIN", "COMMIT", "ROLLBACK",
                    "WAITFOR", "USE", "DECLARE", "BACKUP", "BULK", "CHECKPOINT", "DBCC", "DENY", "KILL",
                    "PRINT", "RAISERROR", "RECONFIGURE", "RESTORE", "RETURN", "SAVE", "SHUTDOWN", "THROW",
                    "OPEN", "CLOSE", "DEALLOCATE", "FETCH", "ENABLE", "DISABLE" -> true;
            // SQL Server 的 REPLACE 是函数，DESC 是排序方向，都不是独立语句起点。
            default -> false;
        };
    }

    private static IllegalArgumentException multipleStatements() {
        return new IllegalArgumentException("SQL must contain exactly one statement");
    }
}
