package com.flying.orm.rdb.internal.template;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

class SqlLexicalScannerTest {

    @Test
    void separatesPostgresqlCodeFromQuotedTextAndComments() {
        String sql = "select ?, $$?$$, $tag$hidden?$tag$ -- ?\n from t where id = ?";

        assertEquals(
                List.of(
                        "CODE:select ?, ",
                        "DOLLAR_QUOTED:$$?$$",
                        "CODE:, ",
                        "DOLLAR_QUOTED:$tag$hidden?$tag$",
                        "CODE: ",
                        "LINE_COMMENT:-- ?\n",
                        "CODE: from t where id = ?"),
                segments(sql, SqlLexicalScanner.rulesFor("PostgreSQL"), false));
    }

    @Test
    void keepsPostgresqlEscapeStringContentsOutOfCodeSegments() {
        String sql = "select E'it\\'s; still text', ?";

        assertEquals(
                List.of("CODE:select ", "SINGLE_QUOTED:E'it\\'s; still text'", "CODE:, ?"),
                segments(sql, SqlLexicalScanner.rulesFor("PostgreSQL"), false));
        assertEquals(
                List.of("CODE:select ", "SINGLE_QUOTED:E'it\\'s; still text'", "CODE:, ?"),
                segments(sql, SqlLexicalScanner.portableRules(), false));
    }

    @Test
    void rejectsMysqlSingleQuoteBoundaryThatDependsOnBackslashMode() {
        String sql = "select 'it\\'s; still text', ?";

        assertThrows(
                IllegalArgumentException.class,
                () -> segments(sql, SqlLexicalScanner.rulesFor("MySQL"), false));
        assertEquals(
                List.of("CODE:select ", "SINGLE_QUOTED:'it\\'s; still text'", "CODE:, ?"),
                segments(sql, SqlLexicalScanner.portableRules(), false));
    }

    @Test
    void rejectsMysqlDoubleQuoteBoundaryThatDependsOnBackslashMode() {
        String sql = "select \"a\\\"; still text\", ?";

        assertThrows(
                IllegalArgumentException.class,
                () -> segments(sql, SqlLexicalScanner.rulesFor("MySQL"), false));
        assertEquals(
                List.of("CODE:select ", "DOUBLE_QUOTED:\"a\\\"; still text\"", "CODE:, ?"),
                segments(sql, SqlLexicalScanner.portableRules(), false));
    }

    @Test
    void keepsMysqlQuotesWhoseBoundaryIsIndependentOfBackslashMode() {
        String sql = "select 'C:\\\\temp', 'it''s', ?";

        assertEquals(
                List.of(
                        "CODE:select ",
                        "SINGLE_QUOTED:'C:\\\\temp'",
                        "CODE:, ",
                        "SINGLE_QUOTED:'it''s'",
                        "CODE:, ?"),
                segments(sql, SqlLexicalScanner.rulesFor("MySQL"), false));
    }

    @Test
    void appliesMysqlDoubleDashAndExecutableCommentRulesOnce() {
        assertEquals(
                List.of("CODE:select 1--not-comment\n, ?"),
                segments("select 1--not-comment\n, ?", SqlLexicalScanner.rulesFor("MySQL"), false));
        assertEquals(
                List.of("CODE:select 1", "LINE_COMMENT:-- comment\n", "CODE:, ?"),
                segments("select 1-- comment\n, ?", SqlLexicalScanner.rulesFor("MySQL"), false));
        assertThrows(
                IllegalArgumentException.class,
                () -> segments("select 1 /*! hidden */", SqlLexicalScanner.rulesFor("MySQL"), false));
    }

    @Test
    void recognizesOracleAlternativeQuotesAndTemplateSlotsWithoutAllocatingTokens() {
        String sql = "select q'[secret ? :value]' from ${table} where id=:id";

        assertEquals(
                List.of(
                        "CODE:select ",
                        "ORACLE_QUOTED:q'[secret ? :value]'",
                        "CODE: from ",
                        "TEMPLATE_SLOT:${table}",
                        "CODE: where id=:id"),
                segments(sql, SqlLexicalScanner.rulesFor("Oracle"), true));
    }

    @Test
    void rejectsEveryUnclosedProtectedRegion() {
        assertThrows(IllegalArgumentException.class,
                () -> segments("select 'secret", SqlLexicalScanner.genericRules(), false));
        assertThrows(IllegalArgumentException.class,
                () -> segments("select /* secret", SqlLexicalScanner.genericRules(), false));
        assertThrows(IllegalArgumentException.class,
                () -> segments("select $tag$secret", SqlLexicalScanner.genericRules(), false));
        assertThrows(IllegalArgumentException.class,
                () -> segments("select ${table", SqlLexicalScanner.genericRules(), true));
    }

    private static List<String> segments(String sql, SqlLexicalScanner.Rules rules, boolean templateSlots) {
        List<String> result = new ArrayList<>();
        SqlLexicalScanner.scan(sql, rules, templateSlots,
                (kind, start, end) -> result.add(kind + ":" + sql.substring(start, end)));
        return result;
    }
}
