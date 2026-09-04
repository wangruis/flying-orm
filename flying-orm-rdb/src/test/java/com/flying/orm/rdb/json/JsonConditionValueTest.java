package com.flying.orm.rdb.json;

import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderContext;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.core.sql.render.SqlTermPackage;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/** JSON 条件直接入口必须与结构化入口使用同一套冻结和路径安全边界。 */
class JsonConditionValueTest {

    @Test
    void postgresqlContainsCastsNativeJsonToJsonb() {
        assertPostgresqlContainment(JsonStructuredConditions.JSON_CONTAINS,
                JsonConditionValue.contains(Map.of("active", true)), Map.of("active", true),
                "cast(\"payload\" as jsonb) @> cast(? as jsonb)", List.of("{\"active\":true}"));
    }

    @Test
    void postgresqlArrayContainsCastsNativeJsonPathToJsonb() {
        assertPostgresqlContainment(JsonStructuredConditions.JSON_ARRAY_CONTAINS,
                JsonConditionValue.arrayContains(List.of("tags"), "active"),
                Map.of("path", "tags", "value", "active"),
                "cast(\"payload\" #> cast(? as text[]) as jsonb) @> cast(? as jsonb)",
                List.of(new String[]{"tags"}, "[\"active\"]"));
    }

    private static void assertPostgresqlContainment(String operator, JsonConditionValue value,
                                                    Object structuredValue, String predicate,
                                                    List<Object> parameters) {
        SqlTermHandler handler = JsonTermHandlers.postgresql().handlers().stream()
                .filter(candidate -> candidate.id().equals(operator)).findFirst().orElseThrow();
        SqlFragment fragment = handler.render(new TermCondition("payload", operator, value),
                name -> '"' + name + '"');
        assertEquals(predicate, fragment.sql());
        assertContainmentParameters(parameters, fragment.parameters());

        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms()
                .addTermPackage(JsonTermHandlers.postgresql()).build(), RdbDialect.postgresql());
        for (String type : List.of("JSON", "JSONB")) {
            DynamicForm form = DynamicForm.builder("documents", "documents")
                    .addField(DynamicField.of("payload", type)).build();
            SqlRequest request = renderer.select(form, JsonStructuredConditions.standard().compile(form,
                    StructuredConditionInput.term("payload", operator, structuredValue)));
            assertTrue(request.sql().contains(predicate), request.sql());
            assertContainmentParameters(parameters, request.parameters());
        }
    }

    private static void assertContainmentParameters(List<Object> expected, List<Object> actual) {
        assertEquals(expected.size(), actual.size());
        for (int index = 0; index < expected.size(); index++) {
            if (expected.get(index) instanceof String[] path) {
                assertArrayEquals(path, (String[]) actual.get(index));
            } else {
                assertEquals(expected.get(index), actual.get(index));
            }
        }
    }

    @Test
    void postgresqlPathEqualityPreservesJsonScalarTypes() {
        SqlTermHandler handler = JsonTermHandlers.postgresql().handlers().stream()
                .filter(candidate -> candidate.id().equals(JsonStructuredConditions.JSON_PATH_EQUALS))
                .findFirst().orElseThrow();
        for (Object scalar : List.of("1", 1, "true", true, false)) {
            SqlFragment fragment = handler.render(new TermCondition(
                    "payload", JsonStructuredConditions.JSON_PATH_EQUALS,
                    JsonConditionValue.pathEquals(List.of("value"), scalar)), name -> '"' + name + '"');
            assertEquals("cast(\"payload\" #> cast(? as text[]) as jsonb) = cast(? as jsonb)", fragment.sql());
            assertEquals(JsonValueCodec.writeLiteral(scalar), fragment.parameters().get(1));
        }
    }

    @Test
    void freezesContainsInputAsCanonicalJson() {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("active", true);

        JsonConditionValue condition = JsonConditionValue.contains(source);
        source.put("active", false);

        assertEquals("{\"active\":true}", condition.value());
    }

    @Test
    void containsHandlersBindTheCanonicalValueOwnedByTheCondition() {
        JsonConditionValue condition = JsonConditionValue.contains(Map.of("active", true));
        TermCondition term = new TermCondition(
                "payload", JsonStructuredConditions.JSON_CONTAINS, condition);
        SqlRenderContext context = name -> '"' + name + '"';

        for (SqlTermPackage termPackage : List.of(JsonTermHandlers.mysql(), JsonTermHandlers.postgresql())) {
            SqlTermHandler handler = termPackage.handlers().stream()
                    .filter(candidate -> candidate.id().equals(JsonStructuredConditions.JSON_CONTAINS))
                    .findFirst()
                    .orElseThrow();
            SqlFragment fragment = handler.render(term, context);

            assertSame(condition.value(), fragment.parameters().getFirst());
        }
    }

    @Test
    void rejectsUnvalidatedDirectPathSegments() {
        assertThrows(IllegalArgumentException.class,
                     () -> JsonConditionValue.pathEquals(List.of("users[*]"), "active"));
    }

    @Test
    void rejectsStructuredValueForTextPathComparison() {
        assertThrows(IllegalArgumentException.class,
                     () -> JsonConditionValue.pathEquals(List.of("status"), Map.of("active", true)));
    }

    @Test
    void snapshotsMutableNumericScalar() {
        AtomicInteger source = new AtomicInteger(1);

        JsonConditionValue condition = JsonConditionValue.pathEquals(List.of("status"), source);
        source.set(2);

        assertEquals(1, condition.value());
    }

    @Test
    void rejectsArbitraryObjectAsJsonScalar() {
        assertThrows(IllegalArgumentException.class,
                     () -> JsonConditionValue.pathEquals(List.of("status"), new Object()));
        assertThrows(IllegalArgumentException.class,
                     () -> JsonConditionValue.pathEquals(List.of("status"), Double.NaN));
    }
}
