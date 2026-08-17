package com.flying.orm.rdb.json;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionErrorCode;
import com.flying.orm.core.condition.StructuredConditionException;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
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

        assertEquals("json_extract(profile, ?) = cast(? as json)", fragment.sql());
        assertEquals(List.of("$.name", "\"Alice\""), fragment.parameters());
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

        assertEquals("profile #>> cast(? as text[]) = cast(? as text)", fragment.sql());
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

        assertEquals("json_extract(profile, ?) = cast(? as json)", fragment.sql());
        assertEquals(List.of("$.name", "\"Alice\""), fragment.parameters());
    }

    /** MySQL JSON 标量比较统一按 JSON 类型比较，布尔值和数字不能退化成字符串或数值隐式转换。 */
    @Test
    void rendersMysqlJsonScalarEqualityWithoutTypeCoercion() {
        SqlRenderer renderer = SqlRenderer.builder().addTermPackage(JsonTermHandlers.mysql()).build();

        SqlFragment bool = renderer.renderWhere(ConditionGroup.and()
                                                              .where("profile", "json-path-eq",
                                                                     JsonConditionValue.pathEquals(
                                                                             List.of("enabled"), true))
                                                              .build());
        SqlFragment number = renderer.renderWhere(ConditionGroup.and()
                                                                .where("profile", "json-path-eq",
                                                                       JsonConditionValue.pathEquals(
                                                                               List.of("attempts"), 3))
                                                                .build());

        assertEquals("json_extract(profile, ?) = cast(? as json)", bool.sql());
        assertEquals(List.of("$.enabled", "true"), bool.parameters());
        assertEquals(List.of("$.attempts", "3"), number.parameters());
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

        assertEquals("profile #>> cast(? as text[]) = cast(? as text)", fragment.sql());
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

        assertEquals("profile #>> cast(? as text[]) = cast(? as text)", fragment.sql());
        assertEquals(2, fragment.parameters().size());
        assertArrayEquals(new String[]{"contact", "name"}, (String[]) fragment.parameters().getFirst());
        assertEquals("Alice", fragment.parameters().get(1));
    }

    /** PostgreSQL 的 #>> 返回 text，数值 JSON 标量也必须通过显式 text 比较，不能形成 text = integer。 */
    @Test
    void castsPostgresqlJsonPathAndScalarTypesExplicitly() {
        SqlRenderer renderer = SqlRenderer.builder()
                                          .addTermPackage(JsonTermHandlers.postgresql())
                                          .build();

        SqlFragment fragment = renderer.renderWhere(ConditionGroup.and()
                                                                  .where("profile",
                                                                         "json-path-eq",
                                                                         JsonConditionValue.pathEquals(
                                                                                 List.of("age"), 18))
                                                                  .build());

        assertEquals("profile #>> cast(? as text[]) = cast(? as text)", fragment.sql());
        assertArrayEquals(new String[]{"age"}, (String[]) fragment.parameters().getFirst());
        assertEquals(18, fragment.parameters().get(1));
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

    /** JSON 序列化前就要检查原始容器预算，不能让包装后的 JSON 字符串掩盖超限集合。 */
    @Test
    void rejectsOversizedRawJsonBeforeSerialization() {
        StructuredConditionException error = assertThrows(
                StructuredConditionException.class,
                () -> JsonStructuredConditions.standard().compile(
                        profilesForm(),
                        StructuredConditionInput.term(
                                "profile", "json-contains", Map.of("roles", List.of("a", "b", "c"))),
                        StructuredConditionPolicy.defaults().withMaxCollectionSize(2)));

        assertEquals(StructuredConditionErrorCode.VALUE_COLLECTION_TOO_LARGE, error.code());
    }

    /** JSON 序列化前必须拒绝过深、过宽的原始容器图，不能把风险推给序列化器。 */
    @Test
    void rejectsRawJsonGraphBeyondDepthAndNodeBudgets() {
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults()
                                                                    .withMaxDepth(2)
                                                                    .withMaxNodes(2);
        StructuredConditionInput deep = StructuredConditionInput.term(
                "profile", "json-contains", Map.of("a", Map.of("b", Map.of("c", 1))));
        StructuredConditionInput wide = StructuredConditionInput.term(
                "profile", "json-contains", Map.of("a", Map.of(), "b", Map.of()));

        StructuredConditionException depth = assertThrows(
                StructuredConditionException.class,
                () -> JsonStructuredConditions.standard().compile(profilesForm(), deep, policy));
        StructuredConditionException nodes = assertThrows(
                StructuredConditionException.class,
                () -> JsonStructuredConditions.standard().compile(profilesForm(), wide, policy));

        assertEquals(StructuredConditionErrorCode.DEPTH_EXCEEDED, depth.code());
        assertEquals(StructuredConditionErrorCode.NODE_COUNT_EXCEEDED, nodes.code());
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

        assertEquals("profile #> cast(? as text[]) is not null", exists.sql());
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
        assertEquals("(profile #> cast(? as text[])) @> cast(? as jsonb)", postgresql.sql());
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
