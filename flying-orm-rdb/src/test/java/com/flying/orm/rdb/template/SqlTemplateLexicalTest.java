package com.flying.orm.rdb.template;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqlTemplateLexicalTest {

    @Test
    void postgresqlTemplateUsesOneLexicalBoundaryForSlotsValuesAndQuotedText() {
        SqlTemplate template = SqlTemplate.query(
                "lookup",
                "select :id, ':ignored', $$:ignored$$, ${column} from ${table} -- :ignored\n"
                        + "where tenant_id=:tenant",
                Set.of("column", "table"));

        SqlRequest request = engine(template, RdbDialect.postgresql()).render(
                "lookup", Map.of("id", 7, "tenant", 9), Map.of("column", "DisplayName", "table", "Forms"));

        assertEquals(
                "select $1, ':ignored', $$:ignored$$, \"DisplayName\" from \"Forms\" -- :ignored\n"
                        + "where tenant_id=$2",
                request.sql());
        assertEquals(java.util.List.of(7, 9), request.parameters());
    }

    @Test
    void mysqlRequiresWhitespaceForDashCommentsAndRejectsExecutableComments() {
        SqlTemplate template = SqlTemplate.query("lookup", "select :head--:tail", Set.of());

        assertEquals(
                "select ?--?",
                engine(template, RdbDialect.mysql()).render(
                        "lookup", Map.of("head", 1, "tail", 2), Map.of()).sql());
        assertThrows(IllegalArgumentException.class,
                () -> SqlTemplate.query("unsafe", "select 1 /*! UPDATE t SET value=1 */", Set.of()));
    }

    @Test
    void actualDialectCannotReuseAnotherDialectsQuotedBoundaryToHideWrites() {
        SqlTemplate postgresqlOnly = SqlTemplate.query(
                "dialect-boundary", "select $tag$ UPDATE secret SET value=1 $tag$", Set.of());

        assertDoesNotThrow(() -> engine(postgresqlOnly, RdbDialect.mysql()));
    }

    @Test
    void registersTrustedTopLevelQueriesWithoutPretendingToProveFunctionSemantics() {
        assertDoesNotThrow(() -> SqlTemplate.query(
                "sequence", "select nextval(:sequence)", Set.of()));
        assertDoesNotThrow(() -> SqlTemplate.query(
                "with-function",
                "with source as (select update(:value) as value) select value from source",
                Set.of()));
    }

    @Test
    void stillRejectsNonQueryAndMultipleStatementsAtRegistration() {
        assertThrows(IllegalArgumentException.class,
                () -> SqlTemplate.query("write", "update accounts set active = true", Set.of()));
        assertThrows(IllegalArgumentException.class,
                () -> SqlTemplate.query("multiple", "select 1; delete from accounts", Set.of()));
        assertThrows(IllegalArgumentException.class,
                () -> SqlTemplate.query("unsafe-slot", "select * from ${table-name}", Set.of("table-name")));
    }

    @Test
    void mysqlTemplateCannotHideSideEffectsBehindASessionDependentQuoteBoundary() {
        SqlTemplate ambiguous = SqlTemplate.query(
                "mysql-mode-boundary", "select 'a\\' INTO @leak -- '", Set.of());

        assertThrows(IllegalArgumentException.class,
                () -> engine(ambiguous, RdbDialect.mysql()));
    }

    @Test
    void oracleAndSqlServerQuotedRegionsDoNotCreateParameters() {
        SqlTemplate oracle = SqlTemplate.query("oracle", "select q'[ :ignored ]', :value from dual", Set.of());
        SqlTemplate sqlServer = SqlTemplate.query("sqlserver", "select [name:ignored], :value", Set.of());

        assertEquals(
                "select q'[ :ignored ]', ? from dual",
                engine(oracle, RdbDialect.oracle()).render("oracle", Map.of("value", 1), Map.of()).sql());
        assertEquals(
                "select [name:ignored], @P0",
                engine(sqlServer, RdbDialect.sqlServer()).render(
                        "sqlserver", Map.of("value", 1), Map.of()).sql());
    }

    private static SqlTemplateEngine engine(SqlTemplate template, RdbDialect dialect) {
        return SqlTemplateEngine.create(
                SqlTemplateRegistry.builder().register(template).build(),
                dialect,
                ValueCodecRegistry.standard());
    }
}
