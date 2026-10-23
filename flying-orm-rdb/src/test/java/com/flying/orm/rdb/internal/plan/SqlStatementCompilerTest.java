package com.flying.orm.rdb.internal.plan;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.rdb.internal.dialect.DatabaseProduct;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlStatementCompilerTest {

    @Test
    void unknownProductsUseTheSamePortableRegionsForValidationAndBinding() {
        for (String sql : new String[]{"select $$?$$, ?", "select [column?], ?"}) {
            SqlStatementPlan plan = SqlStatementCompiler.compile(sql, 1, SqlBindMarkerStyle.CANONICAL, null);
            assertEquals(sql, plan.transportSql("UNKNOWN").orElseThrow());
            assertThrows(IllegalArgumentException.class,
                    () -> SqlStatementCompiler.compile(sql, 2, SqlBindMarkerStyle.CANONICAL, null));
            assertThrows(IllegalArgumentException.class,
                    () -> SqlStatementCompiler.compile(sql + "; delete from users", 1,
                            SqlBindMarkerStyle.CANONICAL, null));
        }
    }

    @Test
    void oracleBlocksKeepTheirStatementBoundaryAndOnlyCountExecutableMarkers() {
        String sql = "DECLARE v VARCHAR2(30) := q'[literal ?]'; BEGIN v := ?; END;";
        SqlStatementPlan plan = SqlStatementCompiler.compile(sql, 1, SqlBindMarkerStyle.CANONICAL, "oracle");
        assertEquals(sql, plan.transportSql("ORACLE").orElseThrow());
        assertThrows(IllegalArgumentException.class,
                () -> SqlStatementCompiler.compile(sql, 2, SqlBindMarkerStyle.CANONICAL, "oracle"));
        assertThrows(IllegalArgumentException.class,
                () -> SqlStatementCompiler.compile(sql + " BEGIN NULL; END;", 1,
                        SqlBindMarkerStyle.CANONICAL, "oracle"));
    }

    @Test
    void reusesSqlTextWhenTheDialectKeepsQuestionMarkBindings() {
        String sql = "select '?' as label, ? as value /* ? */";
        for (String product : new String[]{"mysql", "MariaDB", "oracle", "h2", "unknown"}) {
            SqlStatementPlan plan = SqlStatementCompiler.compile(sql, 1, SqlBindMarkerStyle.CANONICAL, product);
            assertSame(sql, plan.transportSql(DatabaseProduct.detect(product).name()).orElseThrow(), product);
            assertThrows(IllegalArgumentException.class,
                    () -> SqlStatementCompiler.compile(sql, 2, SqlBindMarkerStyle.CANONICAL, product));
            assertThrows(IllegalArgumentException.class,
                    () -> SqlStatementCompiler.compile(sql + "; delete from users", 1,
                            SqlBindMarkerStyle.CANONICAL, product));
        }
    }

    @Test
    void reusesUnchangedSqlWithOnlyProtectedQuestionMarks() {
        String sql = "select '?' as label /* ? */";
        for (String product : new String[]{"mysql", "oracle", "h2", "unknown"}) {
            SqlStatementPlan plan = SqlStatementCompiler.compile(sql, 0, SqlBindMarkerStyle.CANONICAL, product);
            assertSame(sql, plan.transportSql(DatabaseProduct.detect(product).name()).orElseThrow(), product);
            assertThrows(IllegalArgumentException.class,
                    () -> SqlStatementCompiler.compile(sql, 1, SqlBindMarkerStyle.CANONICAL, product));
        }
    }

    @Test
    void ownsBindMarkerCompilationWithoutASecondTopLevelCompiler() {
        assertThrows(ClassNotFoundException.class, () -> Class.forName(
                "com.flying.orm.rdb.internal.dialect.SqlBindMarkerCompiler"));
    }

    @Test
    void exposesNoPublicTrustPublicationOrLookupApi() {
        Set<String> publicMethods = Arrays.stream(SqlStatementCompiler.class.getDeclaredMethods())
                .filter(method -> Modifier.isPublic(method.getModifiers()))
                .map(method -> method.getName())
                .collect(Collectors.toSet());

        assertEquals(Set.of("compile", "insert", "upsert", "ignoreConflicts"), publicMethods);
    }

    @Test
    void conflictIgnoreKeepsValidationAndDoesNotGrantReturningToCustomDialects() {
        String sql = "insert into users(id) values (?) on conflict (id) do nothing";
        assertThrows(IllegalArgumentException.class, () -> SqlStatementCompiler.ignoreConflicts(
                sql + "; delete from users", 1, "postgresql", false));
        assertThrows(IllegalArgumentException.class, () -> SqlStatementCompiler.ignoreConflicts(
                sql, 2, "postgresql", false));
        SqlStatementPlan plan = SqlStatementCompiler.ignoreConflicts(sql, 1, "postgresql", false);
        assertTrue(SqlExecutionStatements.ignoresConflicts(plan));
        org.junit.jupiter.api.Assertions.assertNull(SqlExecutionStatements.jdbcUpsertBatch(plan, "postgresql"));
        org.junit.jupiter.api.Assertions.assertNull(SqlExecutionStatements.r2dbcUpsertBatch(plan, "postgresql"));
        org.junit.jupiter.api.Assertions.assertFalse(SqlExecutionStatements.ignoresConflicts(
                SqlStatementPlan.prepared(sql, plan.bindMarkerStyle(), 1, "POSTGRESQL",
                        plan.transportSql("POSTGRESQL").orElseThrow())));
    }

    @Test
    void insertCompilationCannotBypassStatementOrMarkerValidation() {
        assertThrows(IllegalArgumentException.class, () -> SqlStatementCompiler.insert(
                "insert into users(id) values ", "(?); delete from users", 1, "postgresql"));
        assertThrows(IllegalArgumentException.class, () -> SqlStatementCompiler.insert(
                "insert into users(id) values ", "(?)", 2, "postgresql"));
        SqlStatementPlan plan = SqlStatementCompiler.insert(
                "insert into users(id) values ", "(?)", 1, "postgresql");
        assertEquals("insert into users(id) values ($1)", plan.transportSql("POSTGRESQL").orElseThrow());
    }

    @Test
    void upsertCompilationCannotBypassStatementOrMarkerValidation() {
        String sql = "insert into users(id) values (?) on conflict (id) do nothing";
        assertThrows(IllegalArgumentException.class, () -> SqlStatementCompiler.upsert(
                sql + "; delete from users", 1, "postgresql"));
        assertThrows(IllegalArgumentException.class, () -> SqlStatementCompiler.upsert(
                sql, 2, "postgresql"));
        SqlStatementPlan plan = SqlStatementCompiler.upsert(sql, 1, "postgresql");
        assertEquals("insert into users(id) values ($1) on conflict (id) do nothing",
                plan.transportSql("POSTGRESQL").orElseThrow());
    }

    @Test
    void compilerHintsKeepSqlValueEqualityWithoutGrantingTrustToEqualPlans() {
        SqlStatementPlan compiled = SqlStatementCompiler.upsert(
                "insert into users(id) values (?) on conflict (id) do nothing", 1, "postgresql");
        SqlStatementPlan external = SqlStatementPlan.prepared(compiled.sql(), compiled.bindMarkerStyle(),
                compiled.parameterCount(), "POSTGRESQL", compiled.transportSql("POSTGRESQL").orElseThrow());
        assertEquals(external, compiled);
        assertEquals(compiled, external);
        assertEquals(external.hashCode(), compiled.hashCode());
        org.junit.jupiter.api.Assertions.assertNull(SqlExecutionStatements.r2dbcUpsertBatch(external, "postgresql"));
        org.junit.jupiter.api.Assertions.assertNotNull(SqlExecutionStatements.r2dbcUpsertBatch(compiled, "postgresql"));
    }

    @Test
    void validatesAndCompilesPostgresqlSqlOnce() {
        SqlStatementPlan plan = SqlStatementCompiler.compile(
                "select ?, $tag$?$tag$ where id = ?",
                2,
                SqlBindMarkerStyle.CANONICAL,
                "postgresql");

        assertEquals(
                "select $1, $tag$?$tag$ where id = $2",
                plan.transportSql("POSTGRESQL").orElseThrow());
        assertTrue(plan.preparedFor("postgresql"));
    }

    @Test
    void compilesSqlServerMarkersWithoutTouchingProtectedText() {
        SqlStatementPlan plan = SqlStatementCompiler.compile(
                "select ?, '?' -- ?\nwhere id = ?",
                2,
                SqlBindMarkerStyle.CANONICAL,
                "Microsoft SQL Server");

        assertEquals(
                "select @P0, '?' -- ?\nwhere id = @P1",
                plan.transportSql("SQL_SERVER").orElseThrow());
    }

    @Test
    void preservesNativeMarkersAndMarkerMismatchContract() {
        SqlStatementPlan nativePlan = SqlStatementCompiler.compile(
                "select ?",
                7,
                SqlBindMarkerStyle.NATIVE,
                "PostgreSQL");
        assertEquals("select ?", nativePlan.transportSql("POSTGRESQL").orElseThrow());

        IllegalArgumentException mismatch = assertThrows(
                IllegalArgumentException.class,
                () -> SqlStatementCompiler.compile(
                        "select ?", 2, SqlBindMarkerStyle.CANONICAL, "PostgreSQL"));
        assertEquals(
                "sql parameter marker count does not match parameter count",
                mismatch.getMessage());
    }

    @Test
    void rejectsMoreThanOneExecutableStatementBeforeCachingThePlan() {
        assertThrows(IllegalArgumentException.class, () -> SqlStatementCompiler.compile(
                "select ?; delete from users",
                1,
                SqlBindMarkerStyle.CANONICAL,
                "postgresql"));
    }

    @Test
    void preservesAllLiteralSegmentsAndWhitespaceAroundMarkers() {
        for (String product : new String[]{"postgresql", "sqlserver", "mysql", "oracle", "h2", "unknown"}) {
            for (int count : new int[]{0, 1, 2, 12}) {
                StringBuilder source = new StringBuilder("  select '中?😀''文' as label");
                StringBuilder expected = new StringBuilder(source);
                for (int index = 0; index < count; index++) {
                    source.append(", /*?*/ ? as p").append(index);
                    String marker = product.equals("postgresql") ? "$" + (index + 1)
                            : product.equals("sqlserver") ? "@P" + index : "?";
                    expected.append(", /*?*/ ").append(marker).append(" as p").append(index);
                }
                source.append(" -- ?\r\n ");
                expected.append(" -- ?\r\n ");
                SqlStatementPlan plan = SqlStatementCompiler.compile(
                        source.toString(), count, SqlBindMarkerStyle.CANONICAL, product);
                assertEquals(source.toString().trim(), plan.sql());
                assertEquals(expected.toString().trim(), plan.transportSql(DatabaseProduct.detect(product).name()).orElseThrow());
            }
        }
    }

}
