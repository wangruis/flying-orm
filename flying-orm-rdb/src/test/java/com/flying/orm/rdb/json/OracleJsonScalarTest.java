package com.flying.orm.rdb.json;

import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.core.sql.render.SqlTermPackage;
import com.flying.orm.rdb.dialect.OracleVersion;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.internal.plan.SqlExecutionStatements;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class OracleJsonScalarTest {

    @Test
    void textPatternsBindWithoutScalarExtractionAndNegationRequiresText() {
        for (String pattern : List.of("张%", "_tail", "x') OR 1=1 --", "")) {
            for (boolean negated : List.of(false, true)) {
                String id = negated ? "json-path-not-like" : "json-path-like";
                SqlFragment fragment = render(id, JsonConditionValue.pathLike(List.of("name"), pattern));
                String operand = pattern.isEmpty() ? "\"\"" : "$v0";
                String predicate = "@.stringOnly() " + (pattern.isEmpty() ? "== " : "like ") + operand;
                if (negated) predicate = "exists(@.stringOnly()) && !(" + predicate + ")";
                assertEquals("json_exists(\"payload\", '$.\"name\"?(" + predicate + ")'"
                        + (pattern.isEmpty() ? "" : " passing ? as \"v0\"") + ")", fragment.sql());
                assertEquals(pattern.isEmpty() ? List.of() : List.of(pattern), fragment.parameters());
            }
        }
    }

    @Test
    void nullTestsUseJsonTypeAndExplicitSqlNullRatherThanScalarExtraction() {
        for (List<String> path : List.of(List.<String>of(), List.of("items", "[0]", "value"))) {
            String jsonPath = path.isEmpty() ? "$" : "$.\"items\"[0].\"value\"";
            String nonNull = "json_exists(\"payload\", '" + jsonPath + "?(@.type() != \"null\")')";
            SqlFragment isNull = render("json-path-is-null", JsonConditionValue.pathNull(path));
            SqlFragment notNull = render("json-path-not-null", JsonConditionValue.pathNull(path));
            assertEquals("(\"payload\" is null or not (" + nonNull + "))", isNull.sql());
            assertEquals("(\"payload\" is not null and " + nonNull + ")", notNull.sql());
            assertTrue(isNull.parameters().isEmpty());
            assertTrue(notNull.parameters().isEmpty());
        }
    }

    @Test
    void textAndNullStructuredQueriesKeepLiteralPathsAndTransportBindOrder() {
        DynamicForm form = DynamicForm.builder("documents", "documents")
                .addField(DynamicField.of("id", "INTEGER")).addField(DynamicField.of("payload", "JSON")).build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().addTermPackage(JsonTermHandlers.oracle()).build(),
                RdbDialect.oracle());
        for (String key : List.of("name", "x'? OR 1=1 --")) {
            String path = "$." + JsonValueCodec.writeLiteral(key);
            SqlRequest request = renderer.select(form, JsonStructuredConditions.standard().compile(form,
                    StructuredConditionInput.and(
                            StructuredConditionInput.term("id", "=", 7),
                            StructuredConditionInput.term("payload", "json-path-like",
                                    Map.of("path", path, "value", "%?value'%")),
                            StructuredConditionInput.term("payload", "json-path-not-null", path),
                            StructuredConditionInput.term("id", ">", 3))));
            assertTrue(request.sql().contains(path.replace("'", "''") + "?("), request.sql());
            assertEquals(List.of(7, "%?value'%", 3), request.parameters());
            assertEquals(request.sql(), SqlExecutionStatements.canonical(request, "Oracle Database"));
            assertEquals(request.sql(), SqlExecutionStatements.r2dbc(request, "Oracle Database"));
        }
    }

    @Test
    void comparisonsUseTypedFiltersAndBindBusinessValues() {
        Map<String, String> operators = Map.of("eq", "==", "ne", "!=", "gt", ">", "ge", ">=", "lt", "<", "le", "<=");
        for (Map.Entry<String, String> operator : operators.entrySet()) {
            for (Object value : List.of(18, new BigDecimal("0.000000000000000001"),
                    new BigInteger("123456789012345678901234567890"), "18", "x') OR 1=1 --")) {
                String id = "json-path-" + operator.getKey();
                JsonConditionValue condition = operator.getKey().equals("eq")
                        ? JsonConditionValue.pathEquals(List.of("profile", "value"), value)
                        : JsonConditionValue.pathCompare(List.of("profile", "value"), value);
                SqlFragment fragment = render(id, condition);
                String method = value instanceof Number ? "numberOnly" : "stringOnly";
                assertEquals("json_exists(\"payload\", '$.\"profile\".\"value\"?(@." + method + "() "
                        + operator.getValue() + " $v0)' passing ? as \"v0\")", fragment.sql());
                assertEquals(List.of(value), fragment.parameters());
                assertFalse(fragment.sql().contains(value.toString()));
            }
        }
    }

    @Test
    void booleansAndEmptyStringsDoNotBecomeSqlStringsOrNullBindings() {
        for (Object value : List.of(true, false, "")) {
            SqlFragment fragment = render("json-path-eq", JsonConditionValue.pathEquals(List.of("value"), value));
            String operand = value instanceof Boolean ? "@.booleanOnly() == " + value : "@.stringOnly() == \"\"";
            assertEquals("json_exists(\"payload\", '$.\"value\"?(" + operand + ")')", fragment.sql());
            assertTrue(fragment.parameters().isEmpty());
        }
    }

    @Test
    void membershipPreservesMixedTypesAndUsesOneJsonFilter() {
        List<Object> values = Arrays.asList(18, "18", "", true, false, null, "tail");
        JsonConditionValue condition = JsonConditionValue.pathIn(List.of("value"), values);
        SqlFragment fragment = render("json-path-in", condition);
        assertEquals("json_exists(\"payload\", '$.\"value\"?(@.numberOnly() == $v0 || "
                + "@.stringOnly() == $v1 || @.stringOnly() == \"\" || @.booleanOnly() == true || "
                + "@.booleanOnly() == false || @ == null || @.stringOnly() == $v2)' "
                + "passing ? as \"v0\", ? as \"v1\", ? as \"v2\")", fragment.sql());
        assertEquals(List.of(18, "18", "tail"), fragment.parameters());
        SqlFragment negated = render("json-path-not-in", condition);
        assertEquals("json_exists(\"payload\", '$.\"value\"') and not (" + fragment.sql() + ")", negated.sql());
        assertEquals(fragment.parameters(), negated.parameters());
    }

    @Test
    void emptyMembershipKeepsExplicitTruthWithoutBindings() {
        for (String id : List.of("json-path-in", "json-path-not-in")) {
            SqlFragment fragment = render(id, JsonConditionValue.pathIn(List.of("value"), List.of()));
            assertEquals(id.equals("json-path-in") ? "1 = 0" : "1 = 1", fragment.sql());
            assertTrue(fragment.parameters().isEmpty());
        }
    }

    @Test
    void structuredQueriesPreserveQuotesAndParameterOrderAcrossRepeatedShapes() {
        DynamicForm form = DynamicForm.builder("documents", "documents")
                .addField(DynamicField.of("id", "INTEGER")).addField(DynamicField.of("payload", "JSON")).build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().addTermPackage(JsonTermHandlers.oracle()).build(),
                RdbDialect.oracle());
        for (String key : List.of("first", "x'? OR 1=1 --", "second")) {
            String path = "$." + JsonValueCodec.writeLiteral(key);
            SqlRequest request = renderer.select(form, JsonStructuredConditions.standard().compile(form,
                    StructuredConditionInput.and(
                            StructuredConditionInput.term("id", "=", 7),
                            StructuredConditionInput.term("payload", "json-path-in",
                                    Map.of("path", path, "value", List.of(1, true, "", "x"))),
                            StructuredConditionInput.term("id", ">", 3))));
            assertTrue(request.sql().contains(path.replace("'", "''") + "?("), request.sql());
            assertEquals(List.of(7, 1, "x", 3), request.parameters());
            assertEquals(request.sql(), SqlExecutionStatements.canonical(request, "Oracle Database"));
            assertEquals(request.sql(), SqlExecutionStatements.r2dbc(request, "Oracle Database"));
        }
    }

    @Test
    void explicit12cDoesNotAdvertise19cTypedFilters() {
        SqlTermPackage terms = JsonTermHandlers.oracle(OracleVersion.V12C);
        assertEquals(List.of("json-exists", "json-not-exists"),
                terms.handlers().stream().map(SqlTermHandler::id).toList());
        for (OracleVersion version : List.of(OracleVersion.V19C, OracleVersion.V21C, OracleVersion.V23AI)) {
            assertEquals(JsonTermHandlers.oracle().handlers().stream().map(SqlTermHandler::id).toList(),
                    JsonTermHandlers.oracle(version).handlers().stream().map(SqlTermHandler::id).toList());
        }
    }

    private static SqlFragment render(String id, JsonConditionValue value) {
        SqlTermHandler handler = JsonTermHandlers.oracle().handlers().stream()
                .filter(candidate -> candidate.id().equals(id)).findFirst().orElse(null);
        assertNotNull(handler, id + " must be supplied by the Oracle 19c term package");
        return handler.render(new TermCondition("payload", id, value), field -> '"' + field + '"');
    }
}
