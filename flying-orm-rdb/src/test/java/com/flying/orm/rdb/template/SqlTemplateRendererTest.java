package com.flying.orm.rdb.template;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqlTemplateRendererTest {

    @Test
    void rendersSlotsValuesAndPostgresqlJdbcOperatorsFromOneState() {
        SqlTemplate template = SqlTemplate.query(
                "lookup",
                "select ${payload} ? :key from ${table}",
                Set.of("payload", "table"));
        SqlTemplateRenderer.Backend backend = SqlTemplateRenderer.Backend.create(
                RdbDialect.postgresql(), ValueCodecRegistry.standard(), true);

        SqlRequest request = SqlTemplateRenderer.render(
                template,
                Map.of("key", "enabled"),
                Map.of("payload", "payload", "table", "events"),
                backend);

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
    void registryReturnsTemplateAndServerParametersAsOneEntry() throws Exception {
        SqlTemplate template = SqlTemplate.query("lookup", "select :tenant", Set.of());
        SqlTemplateRegistry registry = SqlTemplateRegistry.builder()
                .register(template, Set.of("tenant"))
                .build();
        Method lookup = java.util.Arrays.stream(SqlTemplateRegistry.class.getDeclaredMethods())
                .filter(candidate -> candidate.getName().equals("entry")
                        && candidate.getParameterCount() == 1)
                .findFirst()
                .orElseThrow(() -> new AssertionError("registry must expose one complete internal entry lookup"));

        Object entry = lookup.invoke(registry, "lookup");
        Method templateAccessor = entry.getClass().getDeclaredMethod("template");
        Method parametersAccessor = entry.getClass().getDeclaredMethod("serverParameters");

        assertSame(template, templateAccessor.invoke(entry));
        assertEquals(Set.of("tenant"), parametersAccessor.invoke(entry));
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
