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
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.dialect.SqlServerVersion;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.internal.plan.SqlExecutionStatements;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class JsonObjectContainmentTest {

    @Test
    void objectLeavesReuseTypedMembershipAndWholePredicateNegation() {
        for (SqlTermPackage terms : packages()) {
            Map<String, Object> leaves = new LinkedHashMap<>();
            leaves.put("number", new BigDecimal("123.000000000000000001"));
            leaves.put("string", "123");
            leaves.put("boolean", true);
            leaves.put("empty", "");
            leaves.put("null", null);
            StringBuilder expected = new StringBuilder("(");
            List<Object> parameters = new ArrayList<>();
            for (Map.Entry<String, Object> entry : leaves.entrySet()) {
                if (expected.length() > 1) expected.append(" and ");
                SqlFragment part = render(terms, "json-path-in", JsonConditionValue.pathIn(
                        List.of("\"profile\"", JsonValueCodec.writeLiteral(entry.getKey())),
                        java.util.Collections.singletonList(entry.getValue())));
                expected.append('(').append(part.sql()).append(')');
                parameters.addAll(part.parameters());
            }
            expected.append(')');
            JsonConditionValue value = JsonConditionValue.contains(Map.of("profile", leaves));
            SqlFragment positive = render(terms, "json-contains", value);
            assertEquals(expected.toString(), positive.sql());
            assertEquals(parameters, positive.parameters());
            SqlFragment negative = render(terms, "json-not-contains", value);
            assertEquals("(\"payload\" is not null and not (" + expected + "))", negative.sql());
            assertEquals(parameters, negative.parameters());
        }
    }

    @Test
    void literalKeysAndImmutableInputKeepPathsAndBindOrderIsolated() {
        DynamicForm form = DynamicForm.builder("documents", "documents")
                .addField(DynamicField.of("id", "INTEGER")).addField(DynamicField.of("payload", "JSON")).build();
        for (boolean oracle : List.of(false, true)) {
            SqlTermPackage terms = oracle ? JsonTermHandlers.oracle() : JsonTermHandlers.sqlServer();
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().addTermPackage(terms).build(),
                    oracle ? RdbDialect.oracle() : RdbDialect.sqlServer());
            for (String key : List.of("a.b", "", "用户", "x'?; drop table t;--", "[0]", "a\"b")) {
                Map<String, Object> source = new LinkedHashMap<>(Map.of(key, "bound'?-value"));
                JsonConditionValue snapshot = JsonConditionValue.contains(source);
                source.put(key, "changed");
                SqlRequest sql = renderer.select(form, JsonStructuredConditions.standard().compile(form,
                        StructuredConditionInput.and(StructuredConditionInput.term("id", "=", 7),
                                StructuredConditionInput.term("payload", "json-contains", snapshot),
                                StructuredConditionInput.term("id", ">", 2))));
                assertEquals(oracle ? List.of(7, "bound'?-value", 2)
                        : List.of(7, key, "bound'?-value", 2), sql.parameters());
                assertFalse(sql.sql().contains("bound'?-value"));
                if (oracle) assertTrue(sql.sql().contains(JsonValueCodec.writeLiteral(key).replace("'", "''")));
                else if (!key.isEmpty()) assertFalse(sql.sql().contains(key));
                String database = oracle ? "Oracle Database" : "Microsoft SQL Server";
                assertEquals(sql.sql(), SqlExecutionStatements.canonical(sql, database));
                assertDoesNotThrow(() -> SqlExecutionStatements.r2dbc(sql, database));
            }
        }
    }

    @Test
    void parserPreservesNumericPrecisionForJsonTextAndMapInputs() {
        BigDecimal decimal = new BigDecimal("0.123456789012345678901234567890123456789");
        BigInteger integer = new BigInteger("123456789012345678901234567890123456789");
        for (SqlTermPackage terms : packages()) {
            for (Object source : List.of(Map.of("n", decimal), "{\"n\":" + decimal + "}")) {
                assertTrue(render(terms, "json-contains", JsonConditionValue.contains(source)).parameters().contains(decimal));
            }
            List<Object> parameters = render(terms, "json-contains", JsonConditionValue.contains(Map.of("n", integer))).parameters();
            assertTrue(parameters.contains(terms.name().equals("json-oracle") ? integer : new BigDecimal(integer)));
        }
    }

    @Test
    void unsupportedStructuresAreNotSilentlyDropped() {
        for (SqlTermPackage terms : packages()) {
            for (Object value : List.of(Map.of(), Map.of("empty", Map.of()), Map.of("items", List.of(1)),
                    List.of(1), "null", "true")) {
                for (String id : List.of("json-contains", "json-not-contains")) {
                    assertThrows(IllegalArgumentException.class,
                            () -> render(terms, id, JsonConditionValue.contains(value)));
                }
            }
        }
    }

    @Test
    void multiLeafStructuredInputSharesDescriptor() {
        DynamicForm form = DynamicForm.builder("documents", "documents")
                .addField(DynamicField.of("payload", "JSON")).build();
        for (SqlTermPackage terms : packages()) {
            for (String id : List.of("json-contains", "json-not-contains")) {
                assertEquals(JsonTermHandlers.mysql().terms().descriptor(id).orElseThrow().fingerprint(),
                        terms.terms().descriptor(id).orElseThrow().fingerprint());
                SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().addTermPackage(terms).build();
                SqlFragment sql = renderer.renderWhere(JsonStructuredConditions.standard().compile(form,
                        StructuredConditionInput.term("payload", id, Map.of("a", 1, "b", 2, "c", 3))));
                assertTrue(sql.parameters().size() >= 3);
            }
        }
    }

    @Test
    void nestedObjectDoesNotLeakItsPathIntoFollowingSiblings() {
        Map<String, Object> source = new LinkedHashMap<>();
        source.put("parent", Map.of("child", Map.of("leaf", 1)));
        source.put("sibling", 2);
        source.put("other", Map.of("leaf", 3));
        for (SqlTermPackage terms : packages()) {
            List<SqlFragment> expected = List.of(
                    render(terms, "json-path-in", JsonConditionValue.pathIn(
                            List.of("\"parent\"", "\"child\"", "\"leaf\""), List.of(1))),
                    render(terms, "json-path-in", JsonConditionValue.pathIn(List.of("\"sibling\""), List.of(2))),
                    render(terms, "json-path-in", JsonConditionValue.pathIn(List.of("\"other\"", "\"leaf\""), List.of(3))));
            SqlFragment actual = render(terms, "json-contains", JsonConditionValue.contains(source));
            assertEquals("(" + expected.stream().map(part -> "(" + part.sql() + ")")
                    .collect(java.util.stream.Collectors.joining(" and ")) + ")", actual.sql());
            assertEquals(expected.stream().flatMap(part -> part.parameters().stream()).toList(), actual.parameters());
        }
    }

    private static List<SqlTermPackage> packages() {
        return List.of(JsonTermHandlers.oracle(), JsonTermHandlers.sqlServer(SqlServerVersion.V2016,
                DatabaseType.of("DECIMAL(38,18)")), JsonTermHandlers.sqlServer());
    }

    private static SqlFragment render(SqlTermPackage terms, String id, JsonConditionValue value) {
        SqlTermHandler handler = terms.handlers().stream().filter(item -> item.id().equals(id)).findFirst().orElseThrow();
        return handler.render(new TermCondition("payload", id, value), name -> '"' + name + '"');
    }
}
