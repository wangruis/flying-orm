package com.flying.orm.rdb.template;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.template.SqlStatements;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SqlDialectBoundaryContractRegressionTest {
    @Test
    void sqlServerReplaceFunctionCompilesInStaticAndDynamicTemplates() {
        for (String function : List.of("REPLACE", "replace /* comment */ ", "RePlAcE\n")) {
            String sql = "SELECT " + function + "(:text, :pattern, :replacement) AS result";
            var registry = SqlTemplateRegistry.builder()
                    .register(SqlTemplate.query("static", sql, Set.of()))
                    .register(SqlTemplate.query("dynamic", sql + " ORDER BY ${sort}", Set.of("sort")))
                    .build();
            var engine = SqlTemplateEngine.create(registry, RdbDialect.sqlServer(), ValueCodecRegistry.standard());
            for (String id : List.of("static", "dynamic")) {
                Map<String, String> identifiers = id.equals("dynamic") ? Map.of("sort", "result") : Map.of();
                String suffix = id.equals("dynamic") ? " ORDER BY [result]" : "";
                var reactive = engine.render(id, replaceValues(), identifiers);
                var jdbc = engine.forJdbc().render(id, replaceValues(), identifiers);
                assertEquals("SELECT " + function + "(@P0, @P1, @P2) AS result" + suffix, reactive.sql());
                assertEquals("SELECT " + function + "(?, ?, ?) AS result" + suffix, jdbc.sql());
                assertEquals(List.of("abc", "b", "x"), reactive.parameters());
                assertEquals(reactive.parameters(), jdbc.parameters());
            }
        }
    }

    @Test
    void sqlServerReplaceFunctionCompilesInNativeQueriesAndWrites() {
        for (String prefix : List.of("SELECT ", "SELECT 1 WHERE 'axc' = ",
                "UPDATE records SET value = ", "INSERT INTO records(value) SELECT ")) {
            String sql = prefix + "REPLACE(:text, :pattern, :replacement)";
            var reactive = SqlTemplateEngine.compileNative(
                    sql, replaceValues(), RdbDialect.sqlServer(), ValueCodecRegistry.standard());
            var jdbc = SqlTemplateEngine.compileNativeJdbc(
                    sql, replaceValues(), RdbDialect.sqlServer(), ValueCodecRegistry.standard());
            assertEquals(prefix + "REPLACE(@P0, @P1, @P2)", reactive.sql());
            assertEquals(prefix + "REPLACE(?, ?, ?)", jdbc.sql());
            assertEquals(List.of("abc", "b", "x"), reactive.parameters());
            assertEquals(reactive.parameters(), jdbc.parameters());
            assertEquals(reactive.sql(), SqlStatements.requireSingleForDatabaseProduct(
                    reactive.sql(), "Microsoft SQL Server"));
            assertEquals(jdbc.sql(), SqlStatements.requireSingleForDatabaseProduct(
                    jdbc.sql(), "Microsoft SQL Server"));
        }
    }

    @Test
    void sqlServerReplaceFunctionDoesNotHideASecondStatement() {
        for (String suffix : List.of("; SELECT 2", " SELECT 2", " DELETE FROM records",
                " UPDATE records SET value = 'x'", " EXEC p", " WITH c AS (SELECT 2 AS v) SELECT v FROM c")) {
            String sql = "SELECT REPLACE(:text, :pattern, :replacement) AS result" + suffix;
            assertThrows(IllegalArgumentException.class, () -> SqlTemplateEngine.compileNative(
                    sql, replaceValues(), RdbDialect.sqlServer(), ValueCodecRegistry.standard()));
            assertThrows(IllegalArgumentException.class, () -> SqlTemplateEngine.compileNativeJdbc(
                    sql, replaceValues(), RdbDialect.sqlServer(), ValueCodecRegistry.standard()));
            assertThrows(IllegalArgumentException.class, () -> SqlTemplateEngine.create(
                    SqlTemplateRegistry.builder().register(SqlTemplate.query("bad", sql, Set.of())).build(),
                    RdbDialect.sqlServer(), ValueCodecRegistry.standard()));
        }
    }

    private static Map<String, String> replaceValues() {
        return Map.of("text", "abc", "pattern", "b", "replacement", "x");
    }

    @Test
    void sqlServerDynamicDescendingTemplatePreservesParameters() {
        var registry = SqlTemplateRegistry.builder().register(SqlTemplate.query(
                "descending", "select :v as v order by ${sort} desc", Set.of("sort"))).build();
        var engine = SqlTemplateEngine.create(registry, RdbDialect.sqlServer(), ValueCodecRegistry.standard());
        var request = engine.render("descending", Map.of("v", 7), Map.of("sort", "v"));
        assertTrue(request.sql().endsWith("[v] desc"));
        assertEquals(List.of(7), request.parameters());
        var jdbc = engine.forJdbc().render("descending", Map.of("v", 7), Map.of("sort", "v"));
        assertEquals("select ? as v order by [v] desc", jdbc.sql());
        assertEquals(List.of(7), jdbc.parameters());
    }

    @Test
    void sqlServerStaticAndNativeDescendingQueriesCompile() {
        var registry = SqlTemplateRegistry.builder().register(SqlTemplate.query(
                "descending", "select :v as v order by v desc", Set.of())).build();
        var engine = SqlTemplateEngine.create(registry, RdbDialect.sqlServer(), ValueCodecRegistry.standard());
        assertEquals(List.of(7), engine.render("descending", Map.of("v", 7), Map.of()).parameters());
        assertEquals(List.of(7), SqlTemplateEngine.compileNativeJdbc(
                "select :v as v order by v desc", Map.of("v", 7),
                RdbDialect.sqlServer(), ValueCodecRegistry.standard()).parameters());
    }

    @Test
    void sqlServerStillRejectsSecondStatements() {
        for (String suffix : List.of("; select 2", " delete from account", " update account set active = 0")) {
            assertThrows(IllegalArgumentException.class, () -> SqlTemplateEngine.compileNative(
                    "select 1 as v order by v desc" + suffix, Map.of(),
                    RdbDialect.sqlServer(), ValueCodecRegistry.standard()));
        }
    }

    @Test
    void oracleCaseExpressionInIfConditionCompilesOnBothPaths() {
        assertOracle("BEGIN IF CASE WHEN 1 = 1 THEN 1 ELSE 0 END = 1 THEN NULL; END IF; END;");
    }

    @Test
    void oracleCaseExpressionCanContinueWithWordsAndOperators() {
        assertOracle("BEGIN IF CASE WHEN 1 = 1 THEN 1 ELSE 0 END IS NOT NULL THEN NULL; END IF; END;");
        assertOracle("BEGIN IF CASE WHEN 1 = 1 THEN 1 ELSE 0 END + CASE WHEN 2 = 2 THEN 1 ELSE 0 END = 2 THEN NULL; END IF; END;");
        assertOracle("BEGIN IF (CASE WHEN 1 = 1 THEN 1 ELSE 0 END) = 1 THEN NULL; END IF; END;");
    }

    @Test
    void oracleNestedCaseExpressionsDoNotCloseTheirContainingBlock() {
        assertOracle("BEGIN IF CASE WHEN 1 = 1 THEN CASE WHEN 2 = 2 THEN 1 ELSE 0 END ELSE 0 END = 1 THEN NULL; END IF; END;");
    }

    @Test
    void oracleStatementCaseAndOrdinaryBlocksRemainSupported() {
        assertOracle("BEGIN CASE WHEN 1 = 1 THEN NULL; ELSE NULL; END CASE; END;");
        assertOracle("DECLARE v NUMBER; BEGIN v := CASE WHEN 1 = 1 THEN 1 ELSE 0 END; END;");
        assertOracle("BEGIN IF 1 = 1 THEN BEGIN NULL; END; END IF; END;");
        assertOracle("BEGIN LOOP EXIT; END LOOP; END;");
    }

    @Test
    void oracleSecondBlockAndSqlAreStillRejected() {
        String first = "BEGIN IF CASE WHEN 1 = 1 THEN 1 ELSE 0 END = 1 THEN NULL; END IF; END;";
        for (String suffix : List.of(" BEGIN NULL; END;", " SELECT 1 FROM dual", " DELETE FROM account")) {
            assertThrows(IllegalArgumentException.class, () -> SqlTemplateEngine.compileNative(
                    first + suffix, Map.of(), RdbDialect.oracle(), ValueCodecRegistry.standard()));
        }
    }

    private static void assertOracle(String sql) {
        var nativeRequest = SqlTemplateEngine.compileNative(
                sql, Map.of(), RdbDialect.oracle(), ValueCodecRegistry.standard());
        var jdbcRequest = SqlTemplateEngine.compileNativeJdbc(
                sql, Map.of(), RdbDialect.oracle(), ValueCodecRegistry.standard());
        assertEquals(sql, nativeRequest.sql());
        assertEquals(sql, jdbcRequest.sql());
        assertTrue(nativeRequest.parameters().isEmpty());
        assertTrue(jdbcRequest.parameters().isEmpty());
    }

    @Test
    void oracleLocalProcedureReturnsToOuterDeclarations() {
        assertOracle("DECLARE PROCEDURE p IS BEGIN NULL; END; BEGIN p; END;");
        assertOracle("DECLARE PROCEDURE p IS BEGIN NULL; END p; v NUMBER := 1; BEGIN p; END;");
    }

    @Test
    void oracleLocalFunctionsAndNestedDeclarationsKeepTheirOwners() {
        assertOracle("DECLARE FUNCTION f RETURN NUMBER IS BEGIN RETURN 1; END f; "
                + "PROCEDURE p IS BEGIN NULL; END p; BEGIN p; IF f() = 1 THEN NULL; END IF; END;");
        assertOracle("DECLARE PROCEDURE p IS PROCEDURE q IS BEGIN NULL; END q; "
                + "BEGIN q; END p; BEGIN p; END;");
        assertOracle("BEGIN DECLARE PROCEDURE p IS BEGIN NULL; END p; BEGIN p; END; END;");
    }

    @Test
    void oracleForwardDeclarationsDoNotConsumeOuterBegin() {
        assertOracle("DECLARE PROCEDURE p; PROCEDURE p IS BEGIN NULL; END p; BEGIN p; END;");
        assertOracle("DECLARE PROCEDURE p(v BOOLEAN := 1 IS NULL); "
                + "PROCEDURE p(v BOOLEAN := 1 IS NULL) IS BEGIN NULL; END p; BEGIN p; END;");
    }

    @Test
    void oracleLocalDeclarationsDoNotHideASecondOuterStatement() {
        String first = "DECLARE PROCEDURE p IS BEGIN NULL; END p; BEGIN p; END;";
        for (String suffix : List.of(" BEGIN NULL; END;", " SELECT 1 FROM dual", " DELETE FROM account")) {
            assertThrows(IllegalArgumentException.class, () -> SqlTemplateEngine.compileNative(
                    first + suffix, Map.of(), RdbDialect.oracle(), ValueCodecRegistry.standard()));
            assertThrows(IllegalArgumentException.class, () -> SqlTemplateEngine.compileNativeJdbc(
                    first + suffix, Map.of(), RdbDialect.oracle(), ValueCodecRegistry.standard()));
        }
    }
}
