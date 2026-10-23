package com.flying.orm.rdb.json;

import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.condition.StructuredConditionException;
import com.flying.orm.core.condition.StructuredConditionErrorCode;
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

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;

/** JSON 条件直接入口必须与结构化入口使用同一套冻结和路径安全边界。 */
class JsonConditionValueTest {

    @Test
    void membershipSnapshotsCollectionsAndArraysOnce() {
        AtomicInteger number = new AtomicInteger(7);
        StringBuilder text = new StringBuilder("active");
        List<Object> source = new java.util.ArrayList<>(java.util.Arrays.asList(number, text, null));
        JsonConditionValue value = JsonConditionValue.pathIn(List.of("status"), source);
        number.set(8);
        text.append("changed");
        source.clear();
        assertEquals(java.util.Arrays.asList(7, "active", null), value.value());
        assertThrows(UnsupportedOperationException.class, () -> ((List<?>) value.value()).clear());
        int[] array = {1, 2};
        JsonConditionValue arrayValue = JsonConditionValue.pathIn(List.of("age"), array);
        array[0] = 99;
        assertEquals(List.of(1, 2), arrayValue.value());
        assertThrows(IllegalArgumentException.class, () -> JsonConditionValue.pathIn(List.of("age"), "1,2"));
        assertThrows(IllegalArgumentException.class, () -> JsonConditionValue.pathIn(List.of("age"), List.of(Double.NaN)));
        assertThrows(IllegalArgumentException.class, () -> JsonConditionValue.pathIn(List.of("age"), List.of(Map.of("x", 1))));
    }

    @Test
    void membershipBudgetCountsMembersNotThePathValueEnvelope() {
        DynamicForm form = DynamicForm.builder("documents", "documents")
                .addField(DynamicField.of("payload", "JSON")).build();
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults().withMaxCollectionSize(1);
        assertEquals(1, policy.maxCollectionSize());
        for (String operator : List.of("json-path-in", "json-path-not-in")) {
            for (Object values : List.of(List.of("x"), new String[]{"x"}, List.of())) {
                assertDoesNotThrow(() -> JsonStructuredConditions.standard().compile(form,
                        StructuredConditionInput.term("payload", operator, Map.of("path", "status", "value", values)), policy));
            }
            for (Object values : List.of(List.of("x", "y"), new String[]{"x", "y"})) {
                StructuredConditionException failure = assertThrows(StructuredConditionException.class,
                        () -> JsonStructuredConditions.standard().compile(form, StructuredConditionInput.or(
                                StructuredConditionInput.term("payload", operator,
                                        Map.of("path", "status", "value", values))), policy));
                assertEquals(StructuredConditionErrorCode.VALUE_COLLECTION_TOO_LARGE, failure.code());
                assertEquals("payload", failure.field());
                assertEquals(operator, failure.operator());
            }
            StructuredConditionException failure = assertThrows(StructuredConditionException.class,
                    () -> JsonStructuredConditions.standard().compile(form, StructuredConditionInput.term(
                            "payload", operator, JsonConditionValue.pathIn(List.of("status"), List.of("x", "y"))), policy));
            assertEquals(StructuredConditionErrorCode.VALUE_COLLECTION_TOO_LARGE, failure.code());
            failure = assertThrows(StructuredConditionException.class,
                    () -> JsonStructuredConditions.standard().compile(form, StructuredConditionInput.term(
                            "payload", operator, Map.of("path", "status", "value", List.of("x".repeat(33)))),
                            policy.withMaxStringLength(32)));
            assertEquals(StructuredConditionErrorCode.VALUE_TOO_LONG, failure.code());
        }
    }

    @Test
    void membershipPreservesScalarTypesAndUsesDialectSupportedPredicates() {
        DynamicForm form = DynamicForm.builder("documents", "documents")
                .addField(DynamicField.of("payload", "JSON")).build();
        List<Object> members = java.util.Arrays.asList(1, "1", true, "", null, "x') OR TRUE --");
        for (SqlTermPackage terms : List.of(JsonTermHandlers.mysql(), JsonTermHandlers.postgresql())) {
            SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().addTermPackage(terms).build();
            boolean mysql = terms.name().equals("json-mysql");
            for (String operator : List.of("json-path-in", "json-path-not-in")) {
                SqlFragment fragment = renderer.renderWhere(assertDoesNotThrow(
                        () -> JsonStructuredConditions.standard().compile(form, StructuredConditionInput.term(
                                "payload", operator, Map.of("path", "value", "value", members)))));
                boolean negated = operator.equals("json-path-not-in");
                String expected;
                if (mysql) {
                    String comparisons = String.join(" or ", java.util.Collections.nCopies(members.size(),
                            "json_extract(payload, ?) = cast(? as json)"));
                    expected = (negated ? "not (" : "(") + comparisons + ")";
                } else {
                    expected = "cast(payload #> cast(? as text[]) as jsonb) " + (negated ? "not in (" : "in (")
                            + String.join(", ", java.util.Collections.nCopies(members.size(), "cast(? as jsonb)")) + ")";
                }
                assertEquals("(" + expected + ")", fragment.sql());
                assertEquals(mysql ? members.size() * 2 : members.size() + 1, fragment.parameters().size());
                for (int index = 0; index < members.size(); index++) {
                    Object member = members.get(index);
                    assertEquals(member == null ? "null" : JsonValueCodec.writeLiteral(member),
                            fragment.parameters().get(mysql ? index * 2 + 1 : index + 1));
                    if (mysql) assertEquals("$.value", fragment.parameters().get(index * 2));
                }
                if (!mysql) assertArrayEquals(new String[]{"value"}, (String[]) fragment.parameters().getFirst());
            }
        }
    }

    @Test
    void emptyMembershipHasExplicitTruthInsteadOfDiscardingTheCondition() {
        DynamicForm form = DynamicForm.builder("documents", "documents")
                .addField(DynamicField.of("payload", "JSON")).build();
        for (SqlTermPackage terms : List.of(JsonTermHandlers.mysql(), JsonTermHandlers.postgresql())) {
            SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().addTermPackage(terms).build();
            for (String operator : List.of("json-path-in", "json-path-not-in")) {
                SqlFragment fragment = renderer.renderWhere(assertDoesNotThrow(
                        () -> JsonStructuredConditions.standard().compile(form, StructuredConditionInput.term(
                                "payload", operator, Map.of("path", "value", "value", List.of())))));
                assertEquals(operator.equals("json-path-in") ? "(1 = 0)" : "(1 = 1)", fragment.sql());
                assertTrue(fragment.parameters().isEmpty());
            }
        }
    }

    @Test
    void patternAndNullValuesFreezeOnlyTheRequiredInputs() {
        StringBuilder pattern = new StringBuilder(" name% ");
        java.util.ArrayList<String> path = new java.util.ArrayList<>(List.of("profile", "name"));
        JsonConditionValue like = JsonConditionValue.pathLike(path, pattern);
        JsonConditionValue nullable = JsonConditionValue.pathNull(path);
        pattern.append("changed");
        path.set(1, "changed");
        assertEquals(" name% ", like.value());
        assertEquals(List.of("profile", "name"), like.path());
        assertEquals(List.of("profile", "name"), nullable.path());
        assertEquals(null, nullable.value());
        assertThrows(IllegalArgumentException.class,
                () -> new JsonConditionValue(JsonConditionValue.Kind.PATH_PATTERN, List.of("name"), 42));
        assertThrows(IllegalArgumentException.class,
                () -> new JsonConditionValue(JsonConditionValue.Kind.PATH_NULL, List.of("name"), "null"));
    }

    @Test
    void textPatternsBindWithoutJsonQuotingAndNormalizeJsonNullBeforeUnquoting() {
        DynamicForm form = DynamicForm.builder("documents", "documents")
                .addField(DynamicField.of("payload", "JSON")).build();
        for (SqlTermPackage terms : List.of(JsonTermHandlers.mysql(), JsonTermHandlers.postgresql())) {
            SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().addTermPackage(terms).build();
            boolean mysql = terms.name().equals("json-mysql");
            String extraction = mysql
                    ? "json_unquote(nullif(json_extract(payload, ?), cast('null' as json)))"
                    : "payload #>> cast(? as text[])";
            for (String operator : List.of("json-path-like", "json-path-not-like")) {
                for (String pattern : List.of("", "%", "a_%", "a\\_%", "null", "张%", "x') OR TRUE --")) {
                    SqlFragment fragment = renderer.renderWhere(assertDoesNotThrow(
                            () -> JsonStructuredConditions.standard().compile(form, StructuredConditionInput.term(
                                    "payload", operator, Map.of("path", "profile.name", "value", pattern)))));
                    assertEquals("(" + extraction + (operator.equals("json-path-like") ? " like ?" : " not like ?")
                            + ")", fragment.sql());
                    assertEquals(2, fragment.parameters().size());
                    assertEquals(pattern, fragment.parameters().get(1));
                    if (mysql) {
                        assertEquals("$.profile.name", fragment.parameters().getFirst());
                    } else {
                        assertArrayEquals(new String[]{"profile", "name"}, (String[]) fragment.parameters().getFirst());
                    }
                }
            }
        }
    }

    @Test
    void pathNullChecksBindOnlyThePathAndDoNotConfuseTheStringNull() {
        DynamicForm form = DynamicForm.builder("documents", "documents")
                .addField(DynamicField.of("payload", "JSON")).build();
        for (SqlTermPackage terms : List.of(JsonTermHandlers.mysql(), JsonTermHandlers.postgresql())) {
            SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().addTermPackage(terms).build();
            boolean mysql = terms.name().equals("json-mysql");
            for (String operator : List.of("json-path-is-null", "json-path-not-null")) {
                boolean negated = operator.equals("json-path-not-null");
                String expected = mysql ? "coalesce(json_type(json_extract(payload, ?)), 'NULL') "
                        + (negated ? "<>" : "=") + " 'NULL'"
                        : "payload #>> cast(? as text[]) is " + (negated ? "not null" : "null");
                for (Object path : List.of("profile.name", Map.of("path", "profile.name"))) {
                    SqlFragment fragment = renderer.renderWhere(assertDoesNotThrow(
                            () -> JsonStructuredConditions.standard().compile(form,
                                    StructuredConditionInput.term("payload", operator, path))));
                    assertEquals("(" + expected + ")", fragment.sql());
                    assertEquals(1, fragment.parameters().size());
                    if (mysql) {
                        assertEquals("$.profile.name", fragment.parameters().getFirst());
                    } else {
                        assertArrayEquals(new String[]{"profile", "name"}, (String[]) fragment.parameters().getFirst());
                    }
                }
            }
        }
    }

    @Test
    void containmentVariantsPreserveRawCollectionAndStringBudgets() {
        DynamicForm form = DynamicForm.builder("documents", "documents")
                .addField(DynamicField.of("payload", "JSON")).build();
        for (String operator : List.of("json-contains", "json-not-contains", "json-contained-by", "json-not-contained-by")) {
            for (Object values : List.of(List.of("x", "y"), new String[]{"x", "y"}, Map.of("tags", List.of("x", "y")))) {
                StructuredConditionException failure = assertThrows(StructuredConditionException.class,
                        () -> JsonStructuredConditions.standard().compile(form,
                                StructuredConditionInput.term("payload", operator, values),
                                StructuredConditionPolicy.defaults().withMaxCollectionSize(1)));
                assertEquals(StructuredConditionErrorCode.VALUE_COLLECTION_TOO_LARGE, failure.code());
            }
            StructuredConditionException failure = assertThrows(StructuredConditionException.class,
                    () -> JsonStructuredConditions.standard().compile(form,
                            StructuredConditionInput.term("payload", operator, Map.of("status", "x".repeat(33))),
                            StructuredConditionPolicy.defaults().withMaxStringLength(32)));
            assertEquals(StructuredConditionErrorCode.VALUE_TOO_LONG, failure.code());
        }
    }

    @Test
    void containmentDirectionsAndNegationsUseBoundJsonOperands() {
        DynamicForm form = DynamicForm.builder("documents", "documents")
                .addField(DynamicField.of("payload", "JSON")).build();
        Map<String, Object> document = Map.of("status", "x') OR TRUE --");
        for (SqlTermPackage terms : List.of(JsonTermHandlers.mysql(), JsonTermHandlers.postgresql())) {
            SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().addTermPackage(terms).build();
            boolean mysql = terms.name().equals("json-mysql");
            Map<String, String> expected = mysql ? Map.of(
                    "json-not-contains", "not (json_contains(payload, cast(? as json)))",
                    "json-contained-by", "json_contains(cast(? as json), payload)",
                    "json-not-contained-by", "not (json_contains(cast(? as json), payload))") : Map.of(
                    "json-not-contains", "not (cast(payload as jsonb) @> cast(? as jsonb))",
                    "json-contained-by", "cast(payload as jsonb) <@ cast(? as jsonb)",
                    "json-not-contained-by", "not (cast(payload as jsonb) <@ cast(? as jsonb))");
            for (Map.Entry<String, String> predicate : expected.entrySet()) {
                SqlFragment fragment = renderer.renderWhere(assertDoesNotThrow(
                        () -> JsonStructuredConditions.standard().compile(form,
                                StructuredConditionInput.term("payload", predicate.getKey(), document))));
                assertEquals("(" + predicate.getValue() + ")", fragment.sql());
                assertEquals(List.of(JsonValueCodec.write(document)), fragment.parameters());
            }
        }
    }

    @Test
    void pathAndArrayNegationsPreserveExtractionAndBindings() {
        DynamicForm form = DynamicForm.builder("documents", "documents")
                .addField(DynamicField.of("payload", "JSON")).build();
        for (SqlTermPackage terms : List.of(JsonTermHandlers.mysql(), JsonTermHandlers.postgresql())) {
            SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().addTermPackage(terms).build();
            boolean mysql = terms.name().equals("json-mysql");
            SqlFragment exists = renderer.renderWhere(assertDoesNotThrow(
                    () -> JsonStructuredConditions.standard().compile(form,
                            StructuredConditionInput.term("payload", "json-not-exists", "tags"))));
            assertEquals(mysql ? "(not (json_contains_path(payload, 'one', ?)))"
                    : "(payload #> cast(? as text[]) is null)", exists.sql());
            assertEquals(1, exists.parameters().size());
            SqlFragment array = renderer.renderWhere(assertDoesNotThrow(
                    () -> JsonStructuredConditions.standard().compile(form,
                            StructuredConditionInput.term("payload", "json-array-not-contains",
                                    Map.of("path", "tags", "value", "")))));
            assertEquals(mysql ? "(not (json_contains(json_extract(payload, ?), cast(? as json))))"
                    : "(not (cast(payload #> cast(? as text[]) as jsonb) @> cast(? as jsonb)))", array.sql());
            assertEquals(2, array.parameters().size());
            assertEquals(mysql ? "\"\"" : "[\"\"]", array.parameters().get(1));
            if (mysql) {
                assertEquals("$.tags", exists.parameters().getFirst());
                assertEquals("$.tags", array.parameters().getFirst());
            } else {
                assertArrayEquals(new String[]{"tags"}, (String[]) exists.parameters().getFirst());
                assertArrayEquals(new String[]{"tags"}, (String[]) array.parameters().getFirst());
            }
        }
    }

    @Test
    void scalarPathComparisonsKeepJsonTypesAndBindPathsForBothDialects() {
        Map<String, String> operators = Map.of("json-path-ne", "<>", "json-path-gt", ">",
                "json-path-ge", ">=", "json-path-lt", "<", "json-path-le", "<=");
        DynamicForm form = DynamicForm.builder("documents", "documents")
                .addField(DynamicField.of("payload", "JSON")).build();
        for (SqlTermPackage terms : List.of(JsonTermHandlers.mysql(), JsonTermHandlers.postgresql())) {
            SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().addTermPackage(terms).build();
            boolean mysql = terms.name().equals("json-mysql");
            for (Map.Entry<String, String> operator : operators.entrySet()) {
                for (Object scalar : List.of("1", 1, true, "", "x') OR TRUE --",
                        new BigDecimal("9007199254740993.123456789"))) {
                    SqlFragment fragment = renderer.renderWhere(assertDoesNotThrow(
                            () -> JsonStructuredConditions.standard().compile(form,
                                    StructuredConditionInput.term("payload", operator.getKey(),
                                            Map.of("path", "details.value", "value", scalar)))));
                    assertEquals(mysql
                            ? "(json_extract(payload, ?) " + operator.getValue() + " cast(? as json))"
                            : "(cast(payload #> cast(? as text[]) as jsonb) " + operator.getValue() + " cast(? as jsonb))",
                            fragment.sql());
                    assertEquals(2, fragment.parameters().size());
                    assertEquals(JsonValueCodec.writeLiteral(scalar), fragment.parameters().get(1));
                    if (mysql) {
                        assertEquals("$.details.value", fragment.parameters().getFirst());
                    } else {
                        assertArrayEquals(new String[]{"details", "value"}, (String[]) fragment.parameters().getFirst());
                    }
                }
            }
        }
    }

    @Test
    void emptyStringsRemainValuesInDirectJsonConditions() {
        for (String operator : List.of(JsonStructuredConditions.JSON_PATH_EQUALS,
                JsonStructuredConditions.JSON_ARRAY_CONTAINS)) {
            StringBuilder source = new StringBuilder();
            JsonConditionValue condition = operator.equals(JsonStructuredConditions.JSON_PATH_EQUALS)
                    ? JsonConditionValue.pathEquals(List.of("value"), source)
                    : JsonConditionValue.arrayContains(List.of("value"), source);
            source.append("changed");
            assertEquals("", condition.value());
            for (SqlTermPackage terms : List.of(JsonTermHandlers.mysql(), JsonTermHandlers.postgresql())) {
                SqlFragment fragment = terms.handlers().stream().filter(handler -> handler.id().equals(operator))
                        .findFirst().orElseThrow().render(new TermCondition("payload", operator, condition),
                                name -> '"' + name + '"');
                String encoded = operator.equals(JsonStructuredConditions.JSON_ARRAY_CONTAINS)
                        && terms.name().equals("json-postgresql") ? "[\"\"]" : "\"\"";
                assertEquals(2, fragment.parameters().size());
                assertEquals(encoded, fragment.parameters().get(1));
                assertTrue(fragment.sql().contains("?"));
            }
        }
    }

    @Test
    void pathComparisonFreezesScalarsAndRejectsInvalidOperands() {
        AtomicInteger number = new AtomicInteger(18);
        StringBuilder text = new StringBuilder("active");
        JsonConditionValue numeric = JsonConditionValue.pathCompare(List.of("age"), number);
        JsonConditionValue textual = JsonConditionValue.pathCompare(List.of("status"), text);
        number.set(99);
        text.append("changed");
        assertEquals(18, numeric.value());
        assertEquals("active", textual.value());
        assertEquals(JsonConditionValue.Kind.PATH_COMPARE, numeric.kind());
        for (Object invalid : List.of(Double.NaN, Double.POSITIVE_INFINITY, Map.of("age", 18), List.of(18))) {
            assertThrows(IllegalArgumentException.class,
                    () -> JsonConditionValue.pathCompare(List.of("age"), invalid));
        }
        assertThrows(IllegalArgumentException.class,
                () -> JsonConditionValue.pathCompare(List.of("age[*]"), 18));
    }

    @Test
    void structuredComparisonsPreserveExplicitStringBudgets() {
        DynamicForm form = DynamicForm.builder("documents", "documents")
                .addField(DynamicField.of("payload", "JSON")).build();
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults().withMaxStringLength(32);
        for (String operator : List.of("json-path-ne", "json-path-gt", "json-path-ge", "json-path-lt", "json-path-le",
                "json-path-like", "json-path-not-like")) {
            StructuredConditionException failure = assertThrows(StructuredConditionException.class,
                    () -> JsonStructuredConditions.standard().compile(form, StructuredConditionInput.term(
                            "payload", operator, Map.of("path", "name", "value", "x".repeat(33))), policy));
            assertEquals(StructuredConditionErrorCode.VALUE_TOO_LONG, failure.code());
        }
    }

    @Test
    void emptyStringsRemainValuesInStructuredJsonConditions() {
        DynamicForm form = DynamicForm.builder("documents", "documents")
                .addField(DynamicField.of("payload", "JSON")).build();
        for (RdbDialect dialect : List.of(RdbDialect.mysql(), RdbDialect.postgresql())) {
            SqlTermPackage terms = dialect.name().equals("mysql")
                    ? JsonTermHandlers.mysql() : JsonTermHandlers.postgresql();
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().addTermPackage(terms).build(), dialect);
            for (String operator : List.of(JsonStructuredConditions.JSON_PATH_EQUALS,
                    JsonStructuredConditions.JSON_ARRAY_CONTAINS)) {
                SqlRequest request = renderer.select(form, JsonStructuredConditions.standard().compile(form,
                        StructuredConditionInput.term("payload", operator, Map.of("path", "value", "value", ""))));
                assertEquals(2, request.parameters().size());
                String encoded = operator.equals(JsonStructuredConditions.JSON_ARRAY_CONTAINS)
                        && dialect.name().equals("postgresql") ? "[\"\"]" : "\"\"";
                assertEquals(encoded, request.parameters().get(1));
                assertTrue(request.sql().contains(" where "));
            }
        }
    }

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
