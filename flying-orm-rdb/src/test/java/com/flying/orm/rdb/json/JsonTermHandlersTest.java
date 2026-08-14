package com.flying.orm.rdb.json;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 验证 MySQL/PostgreSQL JSON 条件的路径和值始终参数化，并拒绝路径注入。 */
class JsonTermHandlersTest {

    @Test
    void rendersMysqlJsonPathEqualsAsParameterizedSql() {
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addTermPackage(JsonTermHandlers.mysql())
                                          .build();

        SqlFragment fragment = renderer.renderWhere(ConditionGroup.and()
                                                                  .where("profile",
                                                                         "json-path-eq",
                                                                         JsonConditionValue.pathEquals(
                                                                                 List.of("name"),
                                                                                 "Alice"))
                                                                  .build());

        assertEquals("json_unquote(json_extract(profile, ?)) = ?", fragment.sql());
        assertEquals(List.of("$.name", "Alice"), fragment.parameters());
    }

    @Test
    void rendersPostgresqlJsonPathEqualsAsParameterizedSql() {
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addTermPackage(JsonTermHandlers.postgresql())
                                          .build();

        SqlFragment fragment = renderer.renderWhere(ConditionGroup.and()
                                                                  .where("profile",
                                                                         "json-path-eq",
                                                                         JsonConditionValue.pathEquals(
                                                                                 List.of("name"),
                                                                                 "Alice"))
                                                                  .build());

        assertEquals("profile #>> ? = ?", fragment.sql());
        assertArrayEquals(new String[]{"name"}, (String[]) fragment.parameters().getFirst());
        assertEquals("Alice", fragment.parameters().get(1));
    }

    @Test
    void compilesMysqlFrontendJsonMapConditionAsParameterizedSql() {
        DynamicForm form = profilesForm();
        StructuredConditionInput input = StructuredConditionInput.term("profile",
                                                                       "json-path-eq",
                                                                       Map.of("key", "name", "value", "Alice"));

        ConditionGroup where = JsonStructuredConditions.standard().compile(form, input);
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addTermPackage(JsonTermHandlers.mysql())
                                          .build();

        SqlFragment fragment = renderer.renderWhere(where);

        assertEquals("json_unquote(json_extract(profile, ?)) = ?", fragment.sql());
        assertEquals(List.of("$.name", "Alice"), fragment.parameters());
    }

    @Test
    void compilesPostgresqlFrontendJsonMapConditionAsParameterizedSql() {
        DynamicForm form = profilesForm();
        StructuredConditionInput input = StructuredConditionInput.term("profile",
                                                                       "json-path-eq",
                                                                       Map.of("key", "name", "value", "Alice"));

        ConditionGroup where = JsonStructuredConditions.standard().compile(form, input);
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addTermPackage(JsonTermHandlers.postgresql())
                                          .build();

        SqlFragment fragment = renderer.renderWhere(where);

        assertEquals("profile #>> ? = ?", fragment.sql());
        assertArrayEquals(new String[]{"name"}, (String[]) fragment.parameters().getFirst());
        assertEquals("Alice", fragment.parameters().get(1));
    }

    @Test
    void rejectsUnsafeFrontendJsonKey() {
        DynamicForm form = profilesForm();
        StructuredConditionInput input = StructuredConditionInput.term("profile",
                                                                       "json-path-eq",
                                                                       Map.of("key", "name;drop", "value", "Alice"));

        JsonStructuredConditions compiler = JsonStructuredConditions.standard();

        assertThrows(IllegalArgumentException.class, () -> compiler.compile(form, input));
    }

    @Test
    void compilesNestedPostgresqlJsonPathWithoutPuttingPathIntoSql() {
        StructuredConditionInput input = StructuredConditionInput.term(
                "profile",
                "json-path-eq",
                Map.of("path", "$.contact.name", "value", "Alice"));
        ConditionGroup where = JsonStructuredConditions.standard().compile(profilesForm(), input);
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addTermPackage(JsonTermHandlers.postgresql())
                                          .build();

        SqlFragment fragment = renderer.renderWhere(where);

        assertEquals("profile #>> ? = ?", fragment.sql());
        assertEquals(2, fragment.parameters().size());
        assertArrayEquals(new String[]{"contact", "name"}, (String[]) fragment.parameters().getFirst());
        assertEquals("Alice", fragment.parameters().get(1));
    }

    @Test
    void rendersMysqlJsonContainsAndExistsWithBoundValues() {
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addTermPackage(JsonTermHandlers.mysql())
                                          .build();
        SqlFragment contains = renderer.renderWhere(ConditionGroup.and()
                                                                   .where("profile",
                                                                          "json-contains",
                                                                          JsonConditionValue.contains(
                                                                                  Map.of("role", "admin")))
                                                                   .build());
        SqlFragment exists = renderer.renderWhere(ConditionGroup.and()
                                                                 .where("profile",
                                                                        "json-exists",
                                                                        JsonConditionValue.exists(
                                                                                List.of("contact", "name")))
                                                                 .build());

        assertEquals("json_contains(profile, cast(? as json))", contains.sql());
        assertEquals(List.of("{\"role\":\"admin\"}"), contains.parameters());
        assertEquals("json_contains_path(profile, 'one', ?)", exists.sql());
        assertEquals(List.of("$.contact.name"), exists.parameters());
    }

    /**
     * 结构化 JSON 条件在编译后才可能由响应式链路渲染；调用方随后修改原始容器不能改变已经冻结的绑定值。
     */
    @Test
    void snapshotsMutableJsonContainsValueBeforeDeferredRendering() {
        Map<String, Object> value = new LinkedHashMap<>();
        value.put("role", "user");
        List<String> roles = new ArrayList<>(List.of("reader"));
        value.put("roles", roles);
        ConditionGroup where = JsonStructuredConditions.standard().compile(
                profilesForm(), StructuredConditionInput.term("profile", "json-contains", value));

        value.put("role", "admin");
        roles.add("admin");

        SqlFragment fragment = SqlRenderer.builder()
                                          .addTermPackage(JsonTermHandlers.mysql())
                                          .build()
                                          .renderWhere(where);

        assertEquals(List.of("{\"role\":\"user\",\"roles\":[\"reader\"]}"), fragment.parameters());
    }

    @Test
    void keepsPostgresqlExistsPathAsOneArrayParameter() {
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addTermPackage(JsonTermHandlers.postgresql())
                                          .build();

        SqlFragment exists = renderer.renderWhere(ConditionGroup.and()
                                                                 .where("profile",
                                                                        "json-exists",
                                                                        JsonConditionValue.exists(
                                                                                List.of("contact", "name")))
                                                                 .build());

        assertEquals("profile #> ? is not null", exists.sql());
        assertEquals(1, exists.parameters().size());
        assertArrayEquals(new String[]{"contact", "name"}, (String[]) exists.parameters().getFirst());
    }

    @Test
    void rendersJsonArrayElementContainsWithBoundPathAndValue() {
        StructuredConditionInput input = StructuredConditionInput.term(
                "profile",
                "json-array-contains",
                Map.of("path", "roles", "value", "admin"));

        SqlFragment mysql = SqlRenderer.builder()
                                       .addTermPackage(JsonTermHandlers.mysql())
                                       .build()
                                       .renderWhere(JsonStructuredConditions.standard().compile(profilesForm(), input));
        SqlFragment postgresql = SqlRenderer.builder()
                                            .addTermPackage(JsonTermHandlers.postgresql())
                                            .build()
                                            .renderWhere(JsonStructuredConditions.standard()
                                                                                  .compile(profilesForm(), input));

        assertEquals("json_contains(json_extract(profile, ?), cast(? as json))", mysql.sql());
        assertEquals(List.of("$.roles", "\"admin\""), mysql.parameters());
        assertEquals("(profile #> ?) @> cast(? as jsonb)", postgresql.sql());
        assertArrayEquals(new String[]{"roles"}, (String[]) postgresql.parameters().getFirst());
        assertEquals("[\"admin\"]", postgresql.parameters().get(1));
    }

    @Test
    void rejectsJsonArrayElementConditionsWithNonScalarValues() {
        StructuredConditionInput input = StructuredConditionInput.term(
                "profile",
                "json-array-contains",
                Map.of("path", "roles", "value", List.of("admin")));

        assertThrows(IllegalArgumentException.class,
                     () -> JsonStructuredConditions.standard().compile(profilesForm(), input));
        assertThrows(IllegalArgumentException.class,
                     () -> JsonConditionValue.arrayContains(List.of("roles"), List.of("admin")));
    }

    private static DynamicForm profilesForm() {
        return DynamicForm.builder("profiles", "Profiles")
                          .addField(DynamicField.primaryKey("id", "number"))
                          .addField(DynamicField.of("profile", "json"))
                          .build();
    }
}
