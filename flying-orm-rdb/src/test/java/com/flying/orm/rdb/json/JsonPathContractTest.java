package com.flying.orm.rdb.json;

import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlTermPackage;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.type.DatabaseType;
import com.flying.orm.rdb.dialect.SqlServerVersion;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.internal.plan.SqlExecutionStatements;
import com.flying.orm.core.sql.render.SqlRequest;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JsonPathContractTest {

    private static final Map<String, String> SQL_SERVER_COMPARISONS = Map.of(
            "json-path-eq", "=", "json-path-ne", "<>", "json-path-gt", ">",
            "json-path-ge", ">=", "json-path-lt", "<", "json-path-le", "<=");

    private static final DynamicForm FORM = DynamicForm.builder("documents", "documents")
            .addField(DynamicField.of("payload", "JSON")).build();

    @Test
    void sqlServerRegistersOnlyFunctionsAvailableInTheSelectedVersion() {
        assertThrows(UnsupportedOperationException.class, () -> JsonTermHandlers.sqlServer(SqlServerVersion.V2012));
        for (SqlServerVersion version : List.of(SqlServerVersion.V2016, SqlServerVersion.V2019)) {
            List<String> ids = JsonTermHandlers.sqlServer(version).handlers().stream().map(handler -> handler.id()).toList();
            assertEquals(16, ids.size());
            assertTrue(ids.containsAll(List.of("json-path-in", "json-path-not-in")));
            assertTrue(ids.containsAll(List.of("json-exists", "json-not-exists")));
            assertTrue(ids.containsAll(SQL_SERVER_COMPARISONS.keySet()));
            assertTrue(ids.containsAll(List.of("json-path-like", "json-path-not-like", "json-path-is-null", "json-path-not-null")));
        }
        assertEquals(16, JsonTermHandlers.sqlServer().handlers().size());
    }

    @Test
    void sqlServerMembershipKeepsScalarTypesNullAndMissingPathsDistinct() {
        List<Object> members = java.util.Arrays.asList("true", true, new BigInteger("18446744073709551616"), null);
        for (SqlServerVersion version : List.of(SqlServerVersion.V2016, SqlServerVersion.V2019, SqlServerVersion.V2022)) {
            SqlTermPackage terms = JsonTermHandlers.sqlServer(version, DatabaseType.of("DECIMAL(38,18)"));
            for (String id : List.of("json-path-in", "json-path-not-in")) {
                com.flying.orm.core.sql.render.SqlTermHandler handler = assertDoesNotThrow(() -> terms.handlers()
                        .stream().filter(candidate -> candidate.id().equals(id)).findFirst().orElseThrow());
                SqlFragment sql = handler.render(new TermCondition("payload", id,
                        JsonConditionValue.pathIn(List.of("items", "[0]", "\"a.b\""), members)), name -> "[" + name + "]");
                assertEquals(List.of("a.b", "true", "true", new BigDecimal("18446744073709551616")), sql.parameters());
                assertTrue(sql.sql().contains("N'$.\"items\"[0]' as json"), sql.sql());
                assertTrue(sql.sql().contains("(j.[type] = 1 and j.[value] = ?)"), sql.sql());
                assertTrue(sql.sql().contains("(j.[type] = 3 and j.[value] = ?)"), sql.sql());
                assertTrue(sql.sql().contains("cast(case when j.[type] = 2 then j.[value] end as decimal(38,18)) = ?"), sql.sql());
                assertTrue(sql.sql().contains("j.[type] = 0"), sql.sql());
                assertEquals(id.equals("json-path-not-in"), sql.sql().contains("having max(case when ("));
                if (id.equals("json-path-not-in")) assertTrue(sql.sql().endsWith(") then 1 else 0 end) = 0)"), sql.sql());
                SqlFragment empty = handler.render(new TermCondition("payload", id,
                        JsonConditionValue.pathIn(List.of(), List.of())), name -> "[" + name + "]");
                assertEquals(id.equals("json-path-in") ? "1 = 0" : "1 = 1", empty.sql());
                assertTrue(empty.parameters().isEmpty());
                SqlFragment root = handler.render(new TermCondition("payload", id,
                        JsonConditionValue.pathIn(List.of(), java.util.Collections.singletonList(null))), name -> "[" + name + "]");
                assertTrue(root.sql().startsWith("exists (select 1 from openjson(N'[' + cast([payload] as nvarchar(max)) + N']') j "));
                assertTrue(root.parameters().isEmpty());
                assertTrue(root.sql().contains("j.[type] = 0"));
            }
        }
    }

    @Test
    void sqlServerStructuredMembershipBindsKeysAndValuesWithoutLeakingIntoSql() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms()
                .addTermPackage(JsonTermHandlers.sqlServer(SqlServerVersion.V2016)).build(),
                RdbDialect.sqlServer(SqlServerVersion.V2016));
        for (String id : List.of("json-path-in", "json-path-not-in")) {
            for (String key : List.of("first", "x'?; drop table documents;--", "a.b", "用户", "second")) {
                List<Object> values = java.util.Arrays.asList("value'?", false, null);
                SqlRequest sql = assertDoesNotThrow(() -> renderer.select(FORM, JsonStructuredConditions.standard().compile(FORM,
                        StructuredConditionInput.term("payload", id, Map.of("path", "$." + JsonValueCodec.writeLiteral(key), "value", values)))));
                assertEquals(List.of(key, "value'?", "false"), sql.parameters());
                assertEquals(sql.sql(), SqlExecutionStatements.canonical(sql, "Microsoft SQL Server"));
                String rendered = SqlExecutionStatements.r2dbc(sql, "Microsoft SQL Server");
                assertTrue(rendered.contains("cast(@P0 as nvarchar(max))"), rendered);
                assertTrue(rendered.contains("j.[value] = @P1"), rendered);
                assertTrue(rendered.contains("j.[value] = @P2"), rendered);
            }
        }
    }

    @Test
    void sqlServerScalarComparisonsKeepNumbersTypedAndTextUnbounded() {
        for (Map.Entry<String, String> operator : SQL_SERVER_COMPARISONS.entrySet()) {
            for (Object scalar : List.of(new java.math.BigDecimal("123.456"), "x".repeat(5_000) + "'?", true)) {
                JsonConditionValue value = operator.getKey().equals("json-path-eq")
                        ? JsonConditionValue.pathEquals(List.of("items", "[0]", "value"), scalar)
                        : JsonConditionValue.pathCompare(List.of("items", "[0]", "value"), scalar);
                SqlFragment sql = assertDoesNotThrow(() -> sqlServer(operator.getKey(), value));
                String type = scalar instanceof Number ? "DECIMAL(38,10)" : "nvarchar(max)";
                assertEquals("exists (select 1 from openjson(N'[' + cast([payload] as nvarchar(max)) + N']') "
                        + "with ([value] " + type + " N'$.\"items\"[0].\"value\"') where [value] "
                        + operator.getValue() + " ?)", sql.sql());
                assertEquals(List.of(scalar instanceof Boolean ? scalar.toString() : scalar), sql.parameters());
            }
        }
    }

    @Test
    void sqlServerNumericProjectionUsesTheConfiguredDatabaseType() {
        SqlTermPackage terms = JsonTermHandlers.sqlServer(SqlServerVersion.V2016, DatabaseType.of("DECIMAL(38,18)"));
        SqlFragment sql = terms.handlers().stream().filter(handler -> handler.id().equals("json-path-gt"))
                .findFirst().orElseThrow().render(new TermCondition("payload", "json-path-gt",
                        JsonConditionValue.pathCompare(List.of(), new java.math.BigDecimal("0.123456789012345678"))),
                        name -> "[" + name + "]");
        assertTrue(sql.sql().contains("[value] decimal(38,18) N'$'"), sql.sql());
        assertEquals(List.of(new java.math.BigDecimal("0.123456789012345678")), sql.parameters());
    }

    @Test
    void sqlServerBigIntegersBindAsLosslessDecimals() {
        for (String text : List.of("18446744073709551616", "-18446744073709551616")) {
            BigInteger value = new BigInteger(text);
            for (String operator : SQL_SERVER_COMPARISONS.keySet()) {
                JsonConditionValue condition = operator.equals("json-path-eq")
                        ? JsonConditionValue.pathEquals(List.of("id"), value)
                        : JsonConditionValue.pathCompare(List.of("id"), value);
                assertEquals(List.of(new BigDecimal(value)), sqlServer(operator, condition).parameters());
            }
        }
    }

    @Test
    void sqlServerNumericProjectionRejectsNonNumericOrArrayTypes() {
        for (String type : List.of("VARCHAR(100)", "DECIMAL[]")) {
            assertThrows(IllegalArgumentException.class,
                    () -> JsonTermHandlers.sqlServer(SqlServerVersion.V2016, DatabaseType.of(type)));
        }
    }

    @Test
    void sqlServerStructuredComparisonsKeepPathsValuesAndBindMarkersIsolated() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms()
                .addTermPackage(JsonTermHandlers.sqlServer()).build(), RdbDialect.sqlServer());
        for (Map.Entry<String, String> operator : SQL_SERVER_COMPARISONS.entrySet()) {
            for (String key : List.of("first", "x'?; drop table documents;--", "a.b", "second")) {
                String path = "$." + JsonValueCodec.writeLiteral(key);
                for (Object value : List.of(new BigDecimal("123.456"), "text?", false)) {
                    SqlRequest sql = renderer.select(FORM, JsonStructuredConditions.standard().compile(FORM,
                            StructuredConditionInput.term("payload", operator.getKey(), Map.of("path", path, "value", value))));
                    String type = value instanceof Number ? "DECIMAL(38,10)" : "nvarchar(max)";
                    assertTrue(sql.sql().contains("[value] " + type + " N'" + path.replace("'", "''") + "'"));
                    assertEquals(List.of(value instanceof Boolean ? value.toString() : value), sql.parameters());
                    assertEquals(sql.sql(), SqlExecutionStatements.canonical(sql, "Microsoft SQL Server"));
                    assertEquals(sql.sql().replace("[value] " + operator.getValue() + " ?",
                                    "[value] " + operator.getValue() + " @P0"),
                            SqlExecutionStatements.r2dbc(sql, "Microsoft SQL Server"));
                }
            }
        }
    }

    @Test
    void sqlServerTextPatternsUseUnboundedScalarExtractionAndBoundPatterns() {
        for (String id : List.of("json-path-like", "json-path-not-like")) {
            String pattern = "x".repeat(5_000) + "'?%";
            SqlFragment sql = sqlServer(id, JsonConditionValue.pathLike(List.of("items", "[0]", "name"), pattern));
            assertEquals("exists (select 1 from openjson(N'[' + cast([payload] as nvarchar(max)) + N']') "
                    + "with ([value] nvarchar(max) N'$.\"items\"[0].\"name\"') where [value] "
                    + (id.equals("json-path-like") ? "like ?" : "not like ?") + ")", sql.sql());
            assertEquals(List.of(pattern), sql.parameters());
        }
    }

    @Test
    void sqlServerNullTestsRetainObjectsAndArraysInsteadOfTreatingThemAsMissingScalars() {
        String present = "exists (select 1 from openjson(N'[' + cast([payload] as nvarchar(max)) + N']') "
                + "with ([value] nvarchar(max) N'$', [object] nvarchar(max) N'$' as json) "
                + "where [value] is not null or [object] is not null)";
        SqlFragment isNull = sqlServer("json-path-is-null", JsonConditionValue.pathNull(List.of()));
        SqlFragment notNull = sqlServer("json-path-not-null", JsonConditionValue.pathNull(List.of()));
        assertEquals("not (" + present + ")", isNull.sql());
        assertEquals(present, notNull.sql());
        assertTrue(isNull.parameters().isEmpty());
        assertTrue(notNull.parameters().isEmpty());
    }

    @Test
    void sqlServerExistenceBindsThePathWithoutExtractingOrLosingJsonNull() {
        for (String id : List.of("json-exists", "json-not-exists")) {
            SqlFragment sql = sqlServer(id, JsonConditionValue.exists(List.of("items", "[0]", "name")));
            assertEquals("json_path_exists([payload], ?) = " + (id.equals("json-exists") ? "1" : "0"), sql.sql());
            assertEquals(List.of("$.\"items\"[0].\"name\""), sql.parameters());
        }
    }

    @Test
    void olderSqlServerExistenceChecksMembersWithoutReadingTheirValues() {
        for (SqlServerVersion version : List.of(SqlServerVersion.V2016, SqlServerVersion.V2019)) {
            for (String id : List.of("json-exists", "json-not-exists")) {
                for (String segment : List.of("name", "\"name \"", "\"0\"", "[000]", "\"用户?\"", "\"\"",
                        "\"a\\u0000\"", "\"a\\u0000\\u0000\"", "\"😀\"", "\"x'?;--\"")) {
                    JsonConditionValue value = JsonConditionValue.exists(List.of("items", "[0]", segment));
                    SqlFragment sql = assertDoesNotThrow(() -> JsonTermHandlers.sqlServer(version).handlers().stream()
                            .filter(handler -> handler.id().equals(id)).findFirst().orElseThrow()
                            .render(new TermCondition("payload", id, value), name -> "[" + name + "]"));
                    assertTrue(sql.sql().contains("with ([parent] nvarchar(max) N'$.\"items\"[0]' as json)"), sql.sql());
                    assertTrue(sql.sql().contains("left(p.[parent], 1) = N'" + (segment.startsWith("[") ? "[" : "{") + "'"));
                    assertTrue(sql.sql().contains("cast(j.[key] as varbinary(max)) = cast(cast(? as nvarchar(max)) as varbinary(max))"));
                    assertTrue(sql.sql().startsWith("case when [payload] is null then null when exists ("));
                    assertTrue(sql.sql().endsWith(" then 1 else 0 end = " + (id.equals("json-exists") ? "1" : "0")));
                    Object key = segment.startsWith("[") ? "0" : segment.startsWith("\"") ? JsonValueCodec.read(segment) : segment;
                    assertEquals(List.of(key), sql.parameters());
                    assertTrue(sql.sql().contains("datalength(j.[key]) = " + 2L * ((String) key).length()),
                            "binary equality must also preserve the exact UTF-16 byte length");
                }
            }
        }
    }

    @Test
    void olderSqlServerRootAndStructuredPathsKeepNullAndBindingContracts() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms()
                .addTermPackage(JsonTermHandlers.sqlServer(SqlServerVersion.V2016)).build(),
                RdbDialect.sqlServer(SqlServerVersion.V2016));
        for (String id : List.of("json-exists", "json-not-exists")) {
            SqlRequest root = assertDoesNotThrow(() -> renderer.select(FORM, JsonStructuredConditions.standard().compile(FORM,
                    StructuredConditionInput.term("payload", id, Map.of("path", "$")))));
            assertTrue(root.sql().contains("case when [payload] is not null then 1 end = " + (id.equals("json-exists") ? "1" : "0")));
            assertTrue(root.parameters().isEmpty());
            for (String parent : List.of("first", "x'?; drop table documents;--", "a.b", "second")) {
                String path = "$." + JsonValueCodec.writeLiteral(parent) + ".\"a\\u002eb\"";
                SqlRequest sql = renderer.select(FORM, JsonStructuredConditions.standard().compile(FORM,
                        StructuredConditionInput.term("payload", id, Map.of("path", path))));
                assertEquals(List.of("a.b"), sql.parameters());
                assertTrue(sql.sql().contains("N'$." + JsonValueCodec.writeLiteral(parent).replace("'", "''") + "' as json"));
                assertEquals(sql.sql(), SqlExecutionStatements.canonical(sql, "Microsoft SQL Server"));
                assertEquals(sql.sql().replace("cast(? as nvarchar(max))", "cast(@P0 as nvarchar(max))"),
                        SqlExecutionStatements.r2dbc(sql, "Microsoft SQL Server"));
            }
        }
    }

    @Test
    void sqlServerStructuredQuotedPathsRemainLiteralSafeAndChangeBetweenRequests() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms()
                .addTermPackage(JsonTermHandlers.sqlServer()).build(), RdbDialect.sqlServer());
        for (String key : List.of("first", "x'?; drop table documents;--", "a.b", "用户", "second")) {
            String path = "$." + JsonValueCodec.writeLiteral(key);
            SqlRequest sql = renderer.select(FORM, JsonStructuredConditions.standard().compile(FORM,
                    StructuredConditionInput.term("payload", "json-path-like", Map.of("path", path, "value", "%?x'"))));
            assertTrue(sql.sql().contains("N'" + path.replace("'", "''") + "'"));
            assertEquals(List.of("%?x'"), sql.parameters());
            assertEquals(sql.sql(), SqlExecutionStatements.canonical(sql, "Microsoft SQL Server"));
            assertEquals(sql.sql().replace(" like ?", " like @P0"),
                    SqlExecutionStatements.r2dbc(sql, "Microsoft SQL Server"));
        }
        for (com.flying.orm.core.sql.render.SqlTermHandler handler : JsonTermHandlers.sqlServer().handlers()) {
            assertEquals(JsonTermHandlers.mysql().terms().descriptor(handler.id()).orElseThrow().fingerprint(),
                    handler.descriptor().orElseThrow().fingerprint());
        }
    }

    private static SqlFragment sqlServer(String id, JsonConditionValue value) {
        return JsonTermHandlers.sqlServer().handlers().stream().filter(handler -> handler.id().equals(id))
                .findFirst().orElseThrow().render(new TermCondition("payload", id, value), name -> "[" + name + "]");
    }

    @Test
    void structuredRootAndArrayPathsBecomeBoundDialectPaths() {
        Map<String, List<String>> paths = Map.of(
                "$", List.of(),
                "items[0].name", List.of("items", "[0]", "name"),
                "$.items[12][3].name", List.of("items", "[12]", "[3]", "name"),
                "[0].name", List.of("[0]", "name"),
                "$[0][1]", List.of("[0]", "[1]"));
        for (SqlTermPackage terms : List.of(JsonTermHandlers.mysql(), JsonTermHandlers.postgresql())) {
            SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().addTermPackage(terms).build();
            for (Map.Entry<String, List<String>> path : paths.entrySet()) {
                SqlFragment sql = renderer.renderWhere(assertDoesNotThrow(() -> JsonStructuredConditions.standard()
                        .compile(FORM, StructuredConditionInput.term("payload", "json-path-eq",
                                Map.of("path", path.getKey(), "value", "x') OR TRUE --")))));
                assertPath(terms.name().equals("json-mysql"), path.getValue(), sql.parameters().getFirst());
                assertEquals("\"x') OR TRUE --\"", sql.parameters().get(1));
                assertEquals(2, sql.parameters().size());
            }
        }
    }

    @Test
    void directRootAndArrayPathsWorkAcrossExistingConditionShapes() {
        for (List<String> path : List.of(List.<String>of(), List.of("items", "[0]", "name"))) {
            Map<String, JsonConditionValue> values = assertDoesNotThrow(() -> Map.of(
                    "json-path-eq", JsonConditionValue.pathEquals(path, "active"),
                    "json-path-gt", JsonConditionValue.pathCompare(path, 1),
                    "json-path-like", JsonConditionValue.pathLike(path, "A%"),
                    "json-path-is-null", JsonConditionValue.pathNull(path),
                    "json-path-in", JsonConditionValue.pathIn(path, List.of("active")),
                    "json-exists", JsonConditionValue.exists(path),
                    "json-array-contains", JsonConditionValue.arrayContains(path, "active")));
            for (SqlTermPackage terms : List.of(JsonTermHandlers.mysql(), JsonTermHandlers.postgresql())) {
                SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().addTermPackage(terms).build();
                for (Map.Entry<String, JsonConditionValue> condition : values.entrySet()) {
                    SqlFragment sql = renderer.renderWhere(JsonStructuredConditions.standard().compile(FORM,
                            StructuredConditionInput.term("payload", condition.getKey(), condition.getValue())));
                    assertPath(terms.name().equals("json-mysql"), path, sql.parameters().getFirst());
                }
            }
        }
    }

    @Test
    void structuredPathOnlyAndArrayConditionsUseTheSamePathContract() {
        for (String path : List.of("$", "$.items[0]")) {
            Map<String, Object> values = Map.of(
                    "json-exists", path,
                    "json-path-is-null", Map.of("path", path),
                    "json-array-contains", Map.of("path", path, "value", "active"));
            for (SqlTermPackage terms : List.of(JsonTermHandlers.mysql(), JsonTermHandlers.postgresql())) {
                SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().addTermPackage(terms).build();
                for (Map.Entry<String, Object> condition : values.entrySet()) {
                    SqlFragment sql = renderer.renderWhere(JsonStructuredConditions.standard().compile(FORM,
                            StructuredConditionInput.term("payload", condition.getKey(), condition.getValue())));
                    assertPath(terms.name().equals("json-mysql"),
                            path.equals("$") ? List.of() : List.of("items", "[0]"), sql.parameters().getFirst());
                }
            }
        }
    }

    @Test
    void quotedPropertyNamesRemainSingleBoundKeysAcrossConditionShapes() {
        for (String key : List.of("user.name", "display name", "用户", "[0]", "", "a\"b", "a\\b",
                "a\nb", "x') OR TRUE --")) {
            String quoted = JsonValueCodec.writeLiteral(key);
            List<String> segments = List.of(quoted, "items", "[0]");
            String path = "$." + quoted + ".items[0]";
            Map<String, JsonConditionValue> conditions = assertDoesNotThrow(() -> Map.of(
                    "json-path-eq", JsonConditionValue.pathEquals(segments, "active"),
                    "json-path-gt", JsonConditionValue.pathCompare(segments, 1),
                    "json-path-like", JsonConditionValue.pathLike(segments, "A%"),
                    "json-path-is-null", JsonConditionValue.pathNull(segments),
                    "json-path-in", JsonConditionValue.pathIn(segments, List.of("active")),
                    "json-exists", JsonConditionValue.exists(segments),
                    "json-array-contains", JsonConditionValue.arrayContains(segments, "active")));
            for (SqlTermPackage terms : List.of(JsonTermHandlers.mysql(), JsonTermHandlers.postgresql())) {
                SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().addTermPackage(terms).build();
                for (Map.Entry<String, JsonConditionValue> condition : conditions.entrySet()) {
                    Object raw = condition.getValue().value() == null ? Map.of("path", path)
                            : Map.of("path", path, "value", condition.getValue().value());
                    for (Object input : List.of(raw, condition.getValue())) {
                        SqlFragment sql = renderer.renderWhere(assertDoesNotThrow(() ->
                                JsonStructuredConditions.standard().compile(FORM,
                                        StructuredConditionInput.term("payload", condition.getKey(), input))));
                        if (terms.name().equals("json-mysql")) {
                            assertEquals(path, sql.parameters().getFirst());
                        } else {
                            assertArrayEquals(new String[]{key, "items", "0"},
                                    (String[]) sql.parameters().getFirst());
                        }
                        SqlFragment ordinary = renderer.renderWhere(JsonStructuredConditions.standard().compile(FORM,
                                StructuredConditionInput.term("payload", condition.getKey(),
                                        new JsonConditionValue(condition.getValue().kind(),
                                                List.of("ordinary", "items", "[0]"), condition.getValue().value()))));
                        assertEquals(ordinary.sql(), sql.sql(), "path contents must never enter SQL text");
                    }
                }
            }
        }
    }

    @Test
    void quotedEscapesAndLongNamesAreParsedWithoutSplittingOrTruncation() {
        assertEquals(List.of("\"a\\u002eb\"", "\"\""), JsonConditionValue.parsePath("$.\"a\\u002eb\".\"\""));
        JsonConditionValue value = JsonConditionValue.exists(List.of("\"a\\u002eb\""));
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().addTermPackage(JsonTermHandlers.postgresql()).build();
        SqlFragment sql = renderer.renderWhere(JsonStructuredConditions.standard().compile(FORM,
                StructuredConditionInput.term("payload", "json-exists", value)));
        assertArrayEquals(new String[]{"a.b"}, (String[]) sql.parameters().getFirst());
        String quoted = "\"" + "x".repeat(20_000) + "\"";
        assertEquals(List.of(quoted), JsonConditionValue.parsePath("$." + quoted));
        assertDoesNotThrow(() -> JsonConditionValue.exists(List.of(quoted)));
    }

    @Test
    void malformedQuotedNamesDoNotBroadenThePathGrammar() {
        for (String path : List.of("$.\"unterminated", "$.\"a\\q\"", "$.\"a\\u123\"", "$.\"a\nb\"",
                "$.\"a\"name", "$.\"a\"\"b\"", "$.\"a\".", "$.\"a\".[0]", "$[\"name\"]")) {
            assertThrows(IllegalArgumentException.class, () -> JsonConditionValue.parsePath(path), path);
        }
        for (String segment : List.of("\"a\\q\"", "\"a\nb\"", "\"a\".name", "\"a\" trailing")) {
            assertThrows(IllegalArgumentException.class, () -> JsonConditionValue.exists(List.of(segment)), segment);
        }
    }

    @Test
    void incompleteOrExpressionPathsAreNotSilentlyReinterpreted() {
        for (String path : List.of("", "$.", "$..name", "$.[0]", "$name", "items.", "items..name", "items.[0]",
                "items[0].", "items[0]..name",
                "items[0]name", "items[]", "items[-1]", "items[*]", "items[0:2]", "items[?(@.x)]",
                "items[0", "items[0]]", "items['name']", "items;drop table users")) {
            assertThrows(IllegalArgumentException.class, () -> JsonStructuredConditions.standard().compile(FORM,
                    StructuredConditionInput.term("payload", "json-path-eq", Map.of("path", path, "value", 1))), path);
        }
        for (String segment : List.of("items[0]", "[0].name", "[*]", "[-1]", "[0:2]", "[x]")) {
            assertThrows(IllegalArgumentException.class,
                    () -> JsonConditionValue.pathEquals(List.of(segment), 1), segment);
        }
    }

    private static void assertPath(boolean mysql, List<String> path, Object parameter) {
        if (mysql) {
            StringBuilder expected = new StringBuilder("$");
            for (String segment : path) {
                if (!segment.startsWith("[")) expected.append('.');
                expected.append(segment);
            }
            assertEquals(expected.toString(), parameter);
        } else {
            String[] expected = path.stream().map(segment -> segment.startsWith("[")
                    ? segment.substring(1, segment.length() - 1) : segment).toArray(String[]::new);
            assertArrayEquals(expected, (String[]) parameter);
        }
    }
}
