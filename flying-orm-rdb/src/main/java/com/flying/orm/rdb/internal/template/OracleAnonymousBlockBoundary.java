package com.flying.orm.rdb.internal.template;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Locale;

/** Oracle 匿名块的单语句语法边界；所有文本和注释区域由共享词法扫描器跳过。 */
final class OracleAnonymousBlockBoundary {

    private static final SqlLexicalScanner.Rules ORACLE_RULES = SqlLexicalScanner.rulesFor("oracle");

    private OracleAnonymousBlockBoundary() {
    }

    static boolean startsWithBlock(String sql) {
        FirstWord first = new FirstWord(sql);
        SqlLexicalScanner.scan(sql, ORACLE_RULES, false, first::accept);
        return "BEGIN".equals(first.value) || "DECLARE".equals(first.value);
    }

    static void validate(String sql) {
        BlockValidator validator = new BlockValidator(sql);
        SqlLexicalScanner.scan(sql, ORACLE_RULES, false, validator::accept);
        validator.finish();
    }

    private static final class FirstWord {
        private final String sql;
        private String value;

        private FirstWord(String sql) {
            this.sql = sql;
        }

        private void accept(SqlLexicalScanner.SegmentKind kind, int start, int end) {
            if (value != null || kind != SqlLexicalScanner.SegmentKind.CODE) {
                return;
            }
            for (int index = start; index < end; index++) {
                if (isWordStart(sql.charAt(index))) {
                    int wordEnd = wordEnd(sql, index, end);
                    value = sql.substring(index, wordEnd).toUpperCase(Locale.ROOT);
                    return;
                }
                if (!Character.isWhitespace(sql.charAt(index))) {
                    value = "";
                    return;
                }
            }
        }
    }

    private static final class BlockValidator {
        private final String sql;
        private final Deque<String> scopes = new ArrayDeque<>();
        private boolean declarationPreamble;
        private boolean leadingDeclare;
        private boolean pendingEnd;
        private boolean endLabel;
        private boolean completed;

        private BlockValidator(String sql) {
            this.sql = sql;
            FirstWord first = new FirstWord(sql);
            SqlLexicalScanner.scan(sql, ORACLE_RULES, false, first::accept);
            declarationPreamble = "DECLARE".equals(first.value);
            leadingDeclare = declarationPreamble;
        }

        private void accept(SqlLexicalScanner.SegmentKind kind, int start, int end) {
            if (kind != SqlLexicalScanner.SegmentKind.CODE) {
                return;
            }
            for (int index = start; index < end;) {
                char current = sql.charAt(index);
                if (completed) {
                    if (!Character.isWhitespace(current)) {
                        throw multipleStatements();
                    }
                    index++;
                } else if (current == ';') {
                    if (pendingEnd) {
                        closeScope(scopes);
                        pendingEnd = false;
                    }
                    endLabel = false;
                    completed = scopes.isEmpty() && !declarationPreamble;
                    index++;
                } else if (isWordStart(current)) {
                    int wordEnd = wordEnd(sql, index, end);
                    acceptWord(sql.substring(index, wordEnd).toUpperCase(Locale.ROOT));
                    index = wordEnd;
                } else {
                    if (endLabel && !Character.isWhitespace(current)) {
                        throw multipleStatements();
                    }
                    index++;
                }
            }
        }

        private void acceptWord(String word) {
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
        }

        private void finish() {
            if (!completed || pendingEnd || endLabel || !scopes.isEmpty()) {
                throw multipleStatements();
            }
        }
    }

    private static void closePendingEnd(Deque<String> scopes, String word) {
        if (isEndModifier(word)) {
            if (scopes.isEmpty() || !word.equals(scopes.peek())) {
                throw multipleStatements();
            }
            scopes.pop();
        } else {
            closeScope(scopes);
        }
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

    private static IllegalArgumentException multipleStatements() {
        return new IllegalArgumentException("SQL must contain exactly one statement");
    }
}
