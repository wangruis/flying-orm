package com.flying.orm.rdb.template;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqlTemplateRendererTest {

    @Test
    void directJdbcPreparationPreservesStaticPlansDynamicIdentifiersAndServerParameters() {
        SqlTemplateRegistry registry = SqlTemplateRegistry.builder()
                .register(SqlTemplate.query("static", "select :value, :tenant, :value", Set.of()), Set.of("tenant"))
                .register(SqlTemplate.query("dynamic", "select :value from ${table}", Set.of("table")))
                .register(SqlTemplate.query("json", "select payload ? :key", Set.of()))
                .build();
        for (RdbDialect dialect : java.util.List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.sqlServer(), RdbDialect.oracle())) {
            SqlTemplateEngine engine = SqlTemplateEngine.createJdbc(registry, dialect, ValueCodecRegistry.standard());
            SqlTemplateEngine.PreparedSql prepared = engine.prepare("static");
            SqlRequest first = prepared.render(Map.of("value", 3, "tenant", 7), Map.of());
            SqlRequest second = prepared.render(Map.of("value", 4, "tenant", 8), Map.of());
            assertEquals("select ?, ?, ?", first.sql());
            assertEquals(java.util.List.of(3, 7, 3), first.parameters());
            assertSame(first.statement(), second.statement());
            assertEquals(Set.of("tenant"), prepared.serverParameters());
            assertEquals(java.util.List.of(5),
                    engine.render("dynamic", Map.of("value", 5), Map.of("table", "people")).parameters());
            assertThrows(IllegalArgumentException.class,
                    () -> engine.render("dynamic", Map.of("value", 5), Map.of("table", "people; drop table people")));
        }
        SqlTemplateEngine postgres = SqlTemplateEngine.createJdbc(
                registry, RdbDialect.postgresql(), ValueCodecRegistry.standard());
        assertEquals("select payload ?? ?", postgres.render("json", Map.of("key", "enabled"), Map.of()).sql());
        SqlTemplateRegistry invalid = SqlTemplateRegistry.builder()
                .register(SqlTemplate.query("missing", "select :value", Set.of()), Set.of("tenant")).build();
        assertThrows(IllegalArgumentException.class, () -> SqlTemplateEngine.createJdbc(
                invalid, RdbDialect.postgresql(), ValueCodecRegistry.standard()));
    }

    @Test
    void readsOnlyReferencedValuesFromAReusableParameterMap() {
        Object[] unusedCycle = new Object[1];
        unusedCycle[0] = unusedCycle;
        Map<String, Object> values = new HashMap<>();
        values.put("value", null);
        values.put("unrelated", unusedCycle);
        SqlTemplateRegistry registry = SqlTemplateRegistry.builder()
                .register(SqlTemplate.query("static", "select :value, :value", Set.of()))
                .register(SqlTemplate.query("dynamic", "select :value from ${table}", Set.of("table")))
                .build();
        SqlTemplateEngine engine = SqlTemplateEngine.create(
                registry, RdbDialect.postgresql(), ValueCodecRegistry.standard());
        for (SqlTemplateEngine backend : java.util.List.of(engine, engine.forJdbc())) {
            assertEquals(Arrays.asList(null, null), backend.render("static", values, Map.of()).parameters());
            assertEquals(Arrays.asList((Object) null),
                    backend.render("dynamic", values, Map.of("table", "items")).parameters());
            assertThrows(IllegalArgumentException.class,
                    () -> backend.render("static", Map.of("unrelated", 1), Map.of()));
            assertThrows(IllegalArgumentException.class,
                    () -> backend.render("dynamic", values, Map.of()));
        }
    }

    @Test
    void preparedTemplateKeepsRegistrationAndBackendWithItsStatement() {
        Set<String> serverParameters = new java.util.HashSet<>(Set.of("tenant"));
        SqlTemplateRegistry.Builder builder = SqlTemplateRegistry.builder()
                .register(SqlTemplate.query("lookup", "select :value, :tenant", Set.of()), serverParameters);
        SqlTemplateRegistry registry = builder.build();
        serverParameters.clear();
        builder.register(SqlTemplate.query("later", "select 1", Set.of()));
        SqlTemplateEngine engine = SqlTemplateEngine.create(
                registry, RdbDialect.postgresql(), ValueCodecRegistry.standard());

        SqlTemplateEngine.PreparedSql prepared = engine.prepare(" LOOKUP ");
        SqlTemplateEngine.PreparedSql jdbc = engine.forJdbc().prepare("lookup");
        assertEquals(Set.of("tenant"), prepared.serverParameters());
        assertEquals(Set.of("tenant"), jdbc.serverParameters());
        assertThrows(IllegalArgumentException.class, () -> engine.prepare("later"));
        assertThrows(UnsupportedOperationException.class, () -> prepared.serverParameters().clear());
        SqlRequest first = prepared.render(Map.of("value", 3, "tenant", 7), Map.of());
        SqlRequest second = prepared.render(Map.of("value", 4, "tenant", 8), Map.of());
        assertSame(first.statement(), second.statement());
        assertEquals("select $1, $2", first.sql());
        assertEquals(java.util.List.of(3, 7), first.parameters());
        assertEquals(java.util.List.of(4, 8), second.parameters());
        assertEquals("select ?, ?", jdbc.render(Map.of("value", 3, "tenant", 7), Map.of()).sql());
    }

    @Test
    void rendersSlotsValuesAndPostgresqlJdbcOperatorsFromOneState() {
        SqlTemplate template = SqlTemplate.query(
                "lookup",
                "select ${payload} ? :key from ${table}",
                Set.of("payload", "table"));
        SqlTemplateEngine engine = SqlTemplateEngine.create(
                SqlTemplateRegistry.builder().register(template).build(),
                RdbDialect.postgresql(), ValueCodecRegistry.standard()).forJdbc();

        SqlRequest request = engine.render(
                "lookup",
                Map.of("key", "enabled"),
                Map.of("payload", "payload", "table", "events"));

        assertEquals("select \"payload\" ?? ? from \"events\"", request.sql());
        assertEquals(java.util.List.of("enabled"), request.parameters());
    }

    @Test
    void engineCompilesAndValidatesStaticSlotsAtAssembly() {
        SqlTemplate template = SqlTemplate.query(
                "invalid-static-slots", "select :value", Set.of("unused"));
        SqlTemplateRegistry registry = SqlTemplateRegistry.builder().register(template).build();

        assertThrows(IllegalArgumentException.class, () -> SqlTemplateEngine.create(
                registry, RdbDialect.postgresql(), ValueCodecRegistry.standard()));
    }

    @Test
    void engineRejectsDeclaredServerParametersAbsentFromSql() {
        for (String sql : java.util.List.of("select :value", "select ':tenant', :value /* :tenant */")) {
            SqlTemplateRegistry registry = SqlTemplateRegistry.builder()
                    .register(SqlTemplate.query("lookup", sql, Set.of()), Set.of("tenant"))
                    .build();

            IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                    () -> SqlTemplateEngine.create(registry, RdbDialect.postgresql(), ValueCodecRegistry.standard()));

            assertEquals("SQL template server parameters are not all used by SQL template", error.getMessage());
        }
    }

    @Test
    void registryReturnsTemplateAndServerParametersAsOneEntry() {
        SqlTemplate template = SqlTemplate.query("lookup", "select :tenant", Set.of());
        SqlTemplateRegistry registry = SqlTemplateRegistry.builder()
                .register(template, Set.of("tenant"))
                .build();
        SqlTemplateRegistry.Entry entry = registry.entry("lookup");

        assertSame(template, entry.template());
        assertEquals(Set.of("tenant"), entry.serverParameters());
    }

    @Test
    void registeredRenderingReusesTheStatementCompiledAtEngineAssembly() {
        SqlTemplate template = SqlTemplate.query("lookup", "select :value", Set.of());
        SqlTemplateRegistry registry = SqlTemplateRegistry.builder().register(template).build();
        SqlTemplateEngine engine = SqlTemplateEngine.create(
                registry, RdbDialect.postgresql(), ValueCodecRegistry.standard());

        SqlRequest first = engine.render("lookup", Map.of("value", 1), Map.of());
        SqlRequest second = engine.render("lookup", Map.of("value", 2), Map.of());

        assertSame(first.statement(), second.statement());
        assertEquals("VerifiedSqlStatementPlan", first.statement().getClass().getSimpleName());
        assertEquals("select $1", first.statement().transportSql("postgresql").orElseThrow());
    }

    @Test
    void registeredStaticRenderingBindsValuesAgainstTheCompiledStatement() {
        SqlTemplate template = SqlTemplate.query("lookup", "select :first, :second", Set.of());
        SqlTemplateEngine engine = SqlTemplateEngine.create(
                SqlTemplateRegistry.builder().register(template).build(),
                RdbDialect.postgresql(), ValueCodecRegistry.standard());
        Map<String, Object> values = new HashMap<>();
        values.put("first", 11);
        values.put("second", null);

        SqlRequest request = engine.render("lookup", values, Map.of());

        assertEquals("select $1, $2", request.sql());
        assertEquals(Arrays.asList(11, null), request.parameters());
        values.put("first", 12);
        assertSame(request.statement(), engine.render("lookup", values, Map.of()).statement());
    }

    @Test
    void jdbcEngineRetargetsTheCompiledTemplateAndPreservesPostgresqlOperators() {
        SqlTemplate template = SqlTemplate.query(
                "lookup", "select payload ? :key", Set.of());
        SqlTemplateRegistry registry = SqlTemplateRegistry.builder().register(template).build();
        SqlTemplateEngine engine = SqlTemplateEngine.create(
                registry, RdbDialect.postgresql(), ValueCodecRegistry.standard()).forJdbc();

        SqlRequest first = engine.render("lookup", Map.of("key", "enabled"), Map.of());
        SqlRequest second = engine.render("lookup", Map.of("key", "disabled"), Map.of());

        assertSame(first.statement(), second.statement());
        assertEquals("select payload ?? ?", first.sql());
        assertEquals("select payload ?? ?",
                     first.statement().transportSql("postgresql").orElseThrow());
    }
}
