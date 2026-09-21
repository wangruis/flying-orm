package com.flying.orm.rdb.internal.plan;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlStatementCompilerTest {

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

        assertEquals(Set.of("compile"), publicMethods);
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

}
