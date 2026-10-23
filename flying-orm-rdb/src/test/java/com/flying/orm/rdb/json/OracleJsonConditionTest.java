package com.flying.orm.rdb.json;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.core.sql.render.SqlTermPackage;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class OracleJsonConditionTest {

    private static final DynamicForm FORM = DynamicForm.builder("documents", "documents")
            .addField(DynamicField.of("id", "INTEGER"))
            .addField(DynamicField.of("payload", "JSON")).build();

    @Test
    void nativeExistenceUsesLiteralPathsForRootPropertiesAndArrays() {
        SqlTermPackage terms = oracleTerms();
        assertEquals("json-oracle", terms.name());
        Map<List<String>, String> paths = Map.of(
                List.of(), "$",
                List.of("items", "[0]", "_name"), "$.\"items\"[0].\"_name\"",
                List.of("[0]", "value"), "$[0].\"value\"");
        for (Map.Entry<List<String>, String> path : paths.entrySet()) {
            for (boolean negated : List.of(false, true)) {
                String id = negated ? "json-not-exists" : "json-exists";
                SqlFragment sql = handler(terms, id).render(
                        new TermCondition("payload", id, JsonConditionValue.exists(path.getKey())),
                        field -> '"' + field + '"');
                String exists = "json_exists(\"payload\", '" + path.getValue() + "')";
                assertEquals(negated ? "not (" + exists + ")" : exists, sql.sql());
                assertTrue(sql.parameters().isEmpty());
                assertFalse(sql.sql().contains("json_value"), "JSON null is present, not missing");
            }
        }
    }

    @Test
    void quotedNamesAreEscapedAsOnePathLiteral() {
        SqlTermPackage terms = oracleTerms();
        for (String key : List.of("user.name", "用户", "", "a\"b", "a\\b", "a\nb",
                "x') OR 1=1 --", "a?b", "'; delete from documents; --")) {
            String quoted = JsonValueCodec.writeLiteral(key);
            String path = "$." + quoted + "[0]";
            for (String id : List.of("json-exists", "json-not-exists")) {
                SqlFragment direct = handler(terms, id).render(
                        new TermCondition("payload", id, JsonConditionValue.exists(List.of(quoted, "[0]"))),
                        field -> '"' + field + '"');
                String exists = "json_exists(\"payload\", '" + path.replace("'", "''") + "')";
                assertEquals(id.equals("json-exists") ? exists : "not (" + exists + ")", direct.sql());
                SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().addTermPackage(terms).build()
                        .withIdentifierRenderer(field -> '"' + field + '"');
                ConditionGroup where = JsonStructuredConditions.standard().compile(FORM,
                        StructuredConditionInput.term("payload", id, Map.of("path", path)));
                assertTrue(renderer.renderWhere(where).sql().contains(direct.sql()));
            }
        }
    }

    @Test
    void sequentialFormQueriesDoNotReuseAnEarlierLiteralPath() {
        SqlTermPackage terms = oracleTerms();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().addTermPackage(terms).build(), RdbDialect.oracle());
        for (String key : List.of("first", "second", "first", "x'? OR 1=1 --")) {
            String path = "$." + JsonValueCodec.writeLiteral(key);
            ConditionGroup where = JsonStructuredConditions.standard().compile(FORM, StructuredConditionInput.and(
                    StructuredConditionInput.term("payload", "json-exists", path),
                    StructuredConditionInput.term("id", "=", 7)));
            SqlRequest request = renderer.select(FORM, where);
            assertTrue(request.sql().contains("'" + path.replace("'", "''") + "'"), request.sql());
            assertEquals(List.of(7), request.parameters());
        }
    }

    @Test
    void descriptorsRemainCompatibleWithTheStructuredConditionResolver() {
        SqlTermPackage terms = oracleTerms();
        assertEquals(List.of("json-exists", "json-not-exists", "json-path-eq", "json-path-ne",
                        "json-path-gt", "json-path-ge", "json-path-lt", "json-path-le", "json-path-in", "json-path-not-in",
                        "json-path-like", "json-path-not-like", "json-path-is-null", "json-path-not-null",
                        "json-contains", "json-not-contains"),
                terms.handlers().stream().map(SqlTermHandler::id).toList());
        for (SqlTermHandler handler : terms.handlers()) {
            assertEquals(JsonTermHandlers.mysql().terms().descriptor(handler.id()).orElseThrow().fingerprint(),
                    handler.descriptor().orElseThrow().fingerprint());
            Object wrongKind = handler.id().equals("json-contains") || handler.id().equals("json-not-contains")
                    ? JsonConditionValue.pathEquals(List.of("key"), "value")
                    : JsonConditionValue.contains(Map.of("key", "value"));
            assertThrows(IllegalArgumentException.class, () -> handler.render(new TermCondition(
                    "payload", handler.id(), wrongKind),
                    field -> field));
        }
    }

    private static SqlTermHandler handler(SqlTermPackage terms, String id) {
        return terms.handlers().stream().filter(handler -> handler.id().equals(id)).findFirst().orElseThrow();
    }

    private static SqlTermPackage oracleTerms() {
        return JsonTermHandlers.oracle();
    }
}
