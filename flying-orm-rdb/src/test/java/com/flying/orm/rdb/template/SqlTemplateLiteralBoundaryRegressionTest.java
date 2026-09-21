package com.flying.orm.rdb.template;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.template.SqlLexicalScanner;
import com.flying.orm.rdb.internal.template.SqlStatements;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqlTemplateLiteralBoundaryRegressionTest {

    @Test
    void h2DollarStringKeepsItsTextAndOnlyBindsTheExternalSlot() {
        String sql = "SELECT $$:v $$ AS txt, :v AS n";
        SqlTemplateEngine engine = engine(sql, RdbDialect.h2());

        assertRequest(engine.render("literal", Map.of("v", 7), Map.of()),
                "SELECT $$:v $$ AS txt, ? AS n");
        assertRequest(engine.forJdbc().render("literal", Map.of("v", 7), Map.of()),
                "SELECT $$:v $$ AS txt, ? AS n");
    }

    @Test
    void h2DollarStringDoesNotCreateIdentifierSlotsOrStatements() {
        SqlRequest request = engine("SELECT $$${not_a_slot}; :ignored $$ AS txt, :v AS n", RdbDialect.h2())
                .render("literal", Map.of("v", 7), Map.of());

        assertRequest(request, "SELECT $$${not_a_slot}; :ignored $$ AS txt, ? AS n");
    }

    @Test
    void directH2SqlUsesTheSameLiteralBoundary() {
        SqlRequest request = SqlTemplateEngine.compileNativeJdbc(
                "SELECT $$:v $$ AS txt, :v AS n", Map.of("v", 7),
                RdbDialect.h2(), ValueCodecRegistry.standard());

        assertRequest(request, "SELECT $$:v $$ AS txt, ? AS n");
    }

    @Test
    void postgresqlStandardBackslashStringRegistersAndRenders() {
        SqlTemplateEngine engine = engine("SELECT '\\' AS slash, :v AS n", RdbDialect.postgresql());

        assertRequest(engine.render("literal", Map.of("v", 7), Map.of()),
                "SELECT '\\' AS slash, $1 AS n");
        assertRequest(engine.forJdbc().render("literal", Map.of("v", 7), Map.of()),
                "SELECT '\\' AS slash, ? AS n");
    }

    @Test
    void postgresqlExplicitEscapeStringsKeepTheirExistingBehavior() {
        String sql = "SELECT E'" + "\\" + "':ignored' AS txt, :v AS n";
        SqlRequest request = engine(sql, RdbDialect.postgresql())
                .forJdbc().render("literal", Map.of("v", 7), Map.of());

        assertRequest(request, sql.substring(0, sql.lastIndexOf(":v")) + "? AS n");
    }

    @Test
    void mysqlDoubledQuotesRemainSupportedAndModeDependentQuotesRemainRejected() {
        assertRequest(engine("SELECT 'a''b' AS txt, :v AS n", RdbDialect.mysql())
                .forJdbc().render("literal", Map.of("v", 7), Map.of()),
                "SELECT 'a''b' AS txt, ? AS n");
        assertThrows(IllegalArgumentException.class,
                () -> engine("SELECT '\\' AS txt, :v AS n", RdbDialect.mysql()));
    }

    @Test
    void actualAdditionalStatementsRemainRejectedAtRegistration() {
        assertThrows(IllegalArgumentException.class,
                () -> SqlTemplate.query("bad", "SELECT :v; DELETE FROM records", Set.of()));
        assertThrows(IllegalArgumentException.class,
                () -> SqlTemplate.query("bad", "SELECT '\\'; DELETE FROM records", Set.of()));
    }

    @Test
    void h2DoesNotGainPostgresqlTaggedStrings() {
        assertEquals(-1L, SqlLexicalScanner.protectedSegmentAt(
                "$tag$:ignored$tag$", 0, SqlLexicalScanner.rulesFor("h2"), false));
    }

    @Test
    void unclosedH2DollarStringIsRejectedByTheActualDialect() {
        assertThrows(IllegalArgumentException.class,
                () -> SqlStatements.requireSingleForDatabaseProduct("SELECT $$unclosed", "H2"));
    }

    private static SqlTemplateEngine engine(String sql, RdbDialect dialect) {
        SqlTemplateRegistry registry = SqlTemplateRegistry.builder()
                .register(SqlTemplate.query("literal", sql, Set.of())).build();
        return SqlTemplateEngine.create(registry, dialect, ValueCodecRegistry.standard());
    }

    private static void assertRequest(SqlRequest request, String expectedSql) {
        assertEquals(expectedSql, request.sql());
        assertEquals(List.of(7), request.parameters());
    }
}
