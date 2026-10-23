package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionException;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.array.ArrayStructuredConditions;
import com.flying.orm.rdb.array.ArrayTermHandlers;
import com.flying.orm.rdb.array.ArrayConditionValue;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.json.JsonStructuredConditions;
import com.flying.orm.rdb.json.JsonConditionValue;
import com.flying.orm.rdb.vector.VectorStructuredConditions;
import com.flying.orm.rdb.vector.VectorTermHandlers;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class StructuredConditionResolverFacadeTest {

    @Test
    void rangeEndpointsRemainDatabaseValuesAcrossReadAndWriteConditions() {
        DynamicForm form = DynamicForm.builder("users", "users")
                .addField(DynamicField.of("name", "VARCHAR")).build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        List<String> endpoints = List.of("a", "Z");
        for (String operator : List.of("between", "not-between")) {
            ConditionGroup direct = ConditionGroup.and().where("name", operator, endpoints).build();
            ConditionGroup structured = StructuredConditionResolver.defaults().compile(form,
                    StructuredConditionInput.term("name", operator, endpoints), StructuredConditionPolicy.defaults());
            for (ConditionGroup condition : List.of(direct, structured)) {
                for (SqlRequest request : List.of(renderer.select(form, condition), renderer.count(form, condition),
                        renderer.update(form, Map.of("name", "new"), condition), renderer.delete(form, condition))) {
                    assertTrue(request.sql().toLowerCase(java.util.Locale.ROOT).contains(
                            operator.equals("between") ? "between ? and ?" : "not between ? and ?"), request.sql());
                    assertEquals(endpoints, request.parameters().subList(request.parameters().size() - 2,
                            request.parameters().size()));
                }
            }
        }
    }

    @Test
    void relatedJsonUsesResolvedMetadataInStandaloneAndCompositeResolvers() {
        DynamicForm root = DynamicForm.builder("users", "users")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        StructuredConditionInput input = StructuredConditionInput.term("department.payload",
                JsonStructuredConditions.JSON_PATH_EQUALS, Map.of("path", "status", "value", "x' OR 1=1 --"));
        for (StructuredConditionResolver resolver : List.of(JsonStructuredConditions.standard(), extensions())) {
            ConditionGroup compiled = resolver.compileFields(root, path -> DynamicField.of("payload", "JSON"),
                    input, StructuredConditionPolicy.defaults());
            TermCondition term = (TermCondition) compiled.children().getFirst();
            assertEquals("department.payload", term.field());
            assertEquals("x' OR 1=1 --", assertInstanceOf(JsonConditionValue.class, term.value()).value());
            assertThrows(IllegalArgumentException.class, () -> resolver.compileFields(root,
                    path -> DynamicField.of("payload", "TEXT"), input, StructuredConditionPolicy.defaults()));
        }
    }

    @Test
    void relatedArrayUsesElementTypeAndRetainsCollectionBudget() {
        DynamicForm root = DynamicForm.builder("users", "users")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        StructuredConditionInput input = StructuredConditionInput.term("department.tags",
                ArrayStructuredConditions.CONTAINS, List.of("1", "2"));
        for (StructuredConditionResolver resolver : List.of(ArrayStructuredConditions.postgresql(), extensions())) {
            ConditionGroup compiled = resolver.compileFields(root, path -> DynamicField.of("tags", "INTEGER[]"),
                    input, StructuredConditionPolicy.defaults());
            TermCondition term = (TermCondition) compiled.children().getFirst();
            assertEquals("department.tags", term.field());
            assertInstanceOf(ArrayConditionValue.class, term.value());
            ConditionGroup scalar = resolver.compileFields(root, path -> DynamicField.of("tags", "INTEGER[]"),
                    StructuredConditionInput.term("department.tags", ArrayStructuredConditions.ANY_EQUALS, "2"),
                    StructuredConditionPolicy.defaults());
            assertEquals(2, ((TermCondition) scalar.children().getFirst()).value());
            assertThrows(StructuredConditionException.class, () -> resolver.compileFields(root,
                    path -> DynamicField.of("tags", "INTEGER[]"), input,
                    StructuredConditionPolicy.defaults().withMaxCollectionSize(1)));
        }
    }

    @Test
    void relatedVectorUsesDeclaredDimensions() {
        DynamicForm root = DynamicForm.builder("users", "users")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        StructuredConditionInput input = StructuredConditionInput.term("department.embedding",
                VectorStructuredConditions.L2_LESS_THAN, Map.of("vector", List.of(1, 2), "distance", 0.5));
        for (StructuredConditionResolver resolver : List.of(VectorStructuredConditions.postgresql(), extensions())) {
            ConditionGroup compiled = resolver.compileFields(root,
                    path -> DynamicField.of("embedding", "VECTOR").withLength(2), input,
                    StructuredConditionPolicy.defaults());
            TermCondition term = (TermCondition) compiled.children().getFirst();
            assertEquals("department.embedding", term.field());
            assertTrue(SqlRenderer.builder().addDefaultTerms().addTermPackage(VectorTermHandlers.postgresql())
                    .build().renderWhere(compiled).sql().contains("cast(? as vector)"));
            assertThrows(IllegalArgumentException.class, () -> resolver.compileFields(root,
                    path -> DynamicField.of("embedding", "VECTOR").withLength(3), input,
                    StructuredConditionPolicy.defaults()));
        }
    }

    private static StructuredConditionResolver extensions() {
        return StructuredConditionResolver.composite(JsonStructuredConditions.standard(),
                ArrayStructuredConditions.postgresql(), VectorStructuredConditions.postgresql());
    }

    @Test
    void defaultAndCustomizedResolversUseBoundFieldsWithoutLosingOperatorAliases() {
        DynamicForm root = DynamicForm.builder("users", "users")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        StructuredConditionInput input = StructuredConditionInput.term("department.id", "eq", "12");
        for (StructuredConditionResolver resolver : List.of(StructuredConditionResolver.defaults(),
                StructuredConditionResolver.composite(StructuredConditionCustomizer.allowOperator("same-as", "=")))) {
            ConditionGroup group = resolver.compileFields(root, name -> DynamicField.of("id", "BIGINT"),
                    input, StructuredConditionPolicy.defaults());
            var term = (com.flying.orm.core.condition.TermCondition) group.children().getFirst();
            assertEquals("department.id", term.field());
            assertEquals(12L, term.value());
        }
        ConditionGroup custom = StructuredConditionResolver.composite(
                StructuredConditionCustomizer.allowOperator("same-as", "=")).compileFields(root,
                name -> DynamicField.of("id", "BIGINT"),
                StructuredConditionInput.term("department.id", "same-as", "13"),
                StructuredConditionPolicy.defaults());
        assertEquals(13L, ((com.flying.orm.core.condition.TermCondition) custom.children().getFirst()).value());
    }

    @Test
    void fieldAwareCompilationKeepsThirdPartyResolverAndValidationBoundary() {
        AtomicInteger invocations = new AtomicInteger();
        StructuredConditionResolver custom = StructuredConditionResolvers.validating((form, fields, input, policy) -> {
            invocations.incrementAndGet();
            return ConditionGroup.and().where(fields.apply("id").name(), "=", 7).build();
        });
        DynamicForm root = DynamicForm.builder("users", "users")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        StructuredConditionInput input = StructuredConditionInput.and(
                StructuredConditionInput.term("id", "eq", 1));
        assertThrows(StructuredConditionException.class, () -> custom.compileFields(root, root::field,
                input, StructuredConditionPolicy.defaults().withMaxNodes(1)));
        assertEquals(0, invocations.get());
        assertEquals(1, custom.compileFields(root, root::field, input,
                StructuredConditionPolicy.defaults()).children().size());
        assertEquals(1, invocations.get());
        ConditionGroup related = custom.compileFields(root, path -> DynamicField.of("department_id", "BIGINT"),
                input, StructuredConditionPolicy.defaults());
        assertEquals("department_id", ((TermCondition) related.children().getFirst()).field());
        ConditionGroup direct = custom.compile(root, input, StructuredConditionPolicy.defaults());
        assertEquals("id", ((TermCondition) direct.children().getFirst()).field());
        assertEquals(3, invocations.get());
    }

    @Test
    void resolverFactoryKeepsCustomOperatorsAndStandardFieldCodecsTogether() {
        DynamicForm form = DynamicForm.builder("users", "users")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        ConditionGroup compiled = StructuredConditionResolver.allowOperators("user-in-org").compile(
                form, StructuredConditionInput.and(
                        StructuredConditionInput.term("id", "eq", "12"),
                        StructuredConditionInput.term("id", "user-in-org", "org-a")),
                StructuredConditionPolicy.defaults());
        assertEquals(12L, ((TermCondition) compiled.children().get(0)).value());
        assertEquals("org-a", ((TermCondition) compiled.children().get(1)).value());
    }

    @Test
    void explicitlyAllowedPlainHandlerBindsCustomValuesAcrossReadAndWriteSql() {
        DynamicForm form = DynamicForm.builder("users", "users")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR")).build();
        SqlRenderer sql = SqlRenderer.builder().addDefaultTerms().addTerm(
                SqlTermHandler.of("user-in-org", (term, context) -> new SqlFragment(
                        context.identifier(term.field())
                                + " in (select user_id from org_users where org_id = ?)",
                        List.of(context.parameter(term.value()))))).build();
        String value = "org-a' OR 1=1 --";
        ConditionGroup condition = StructuredConditionResolver.allowOperators("user-in-org").compile(
                form, StructuredConditionInput.term("id", "user-in-org", value),
                StructuredConditionPolicy.defaults().withTerms(sql.terms()));
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(sql, RdbDialect.postgresql());
        for (SqlRequest request : List.of(renderer.select(form, condition), renderer.count(form, condition),
                renderer.update(form, Map.of("name", "new"), condition), renderer.delete(form, condition))) {
            assertTrue(request.sql().contains("org_id = ?"), request.sql());
            assertTrue(!request.sql().contains(value), request.sql());
            assertEquals(value, request.parameters().getLast());
        }
    }

    @Test
    void structuredOrAtMaximumDepthDoesNotGainAnEmptyServerGroup() {
        StructuredConditionInput input = StructuredConditionInput.term("id", "=", 1);
        for (int depth = 1; depth < 64; depth++) {
            input = StructuredConditionInput.or(input);
        }
        assertStructuredBoundary(input, StructuredConditionPolicy.defaults().withMaxDepth(64));
    }

    @Test
    void structuredOrAtMaximumNodeCountDoesNotGainAnEmptyServerGroup() {
        var nodes = new StructuredConditionInput[9999];
        java.util.Arrays.fill(nodes, StructuredConditionInput.term("id", "=", 1));
        assertStructuredBoundary(StructuredConditionInput.or(nodes),
                StructuredConditionPolicy.defaults().withMaxNodes(10_000));
    }

    private static void assertStructuredBoundary(StructuredConditionInput input, StructuredConditionPolicy policy) {
        var form = DynamicForm.builder("events", "events")
                .addField(DynamicField.primaryKey("id", "INTEGER")).build();
        var reads = new FormAggregateReadSupport(FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2()),
                new FormScopeSupport(FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2()), StructuredConditionResolver.defaults(), DataScope.none()));
        var spec = QuerySpec.structured(form, input).withStructuredPolicy(policy);
        var read = assertDoesNotThrow(() -> reads.prepare(spec));
        assertEquals(com.flying.orm.core.condition.LogicalOperator.OR, read.where().operator());
        assertDoesNotThrow(() -> reads.prepareGoverned(spec));
    }

    @Test
    void boxedBinaryStructuredConditionKeepsItsCodecTypeThroughSqlRendering() {
        DynamicForm form = DynamicForm.builder("binary_events", "binary_events")
                .addField(DynamicField.of("payload", "BINARY"))
                .build();
        ConditionGroup condition = StructuredConditionResolver.defaults().compile(
                form,
                StructuredConditionInput.term("payload", "eq", new Byte[]{1, 2}),
                StructuredConditionPolicy.defaults());
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());

        Object parameter = renderer.select(form, condition).parameters().getFirst();

        assertArrayEquals(new byte[]{1, 2}, assertInstanceOf(byte[].class, parameter));
    }

    @Test
    void standaloneArrayResolverCompilesItsBuiltInOperatorWithDefaultPolicy() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("tags", "TEXT[]"))
                                      .build();
        StructuredConditionInput input = StructuredConditionInput.term(
                "tags", ArrayStructuredConditions.CONTAINS, List.of("a"));

        ConditionGroup condition = assertDoesNotThrow(
                () -> ArrayStructuredConditions.postgresql().compile(form, input));
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms()
                                          .addTermPackage(ArrayTermHandlers.postgresql()).build();
        assertEquals("(tags @> cast(? as text[]))", renderer.renderWhere(condition).sql());
    }

    @Test
    void standaloneVectorResolverCompilesItsBuiltInOperatorWithDefaultPolicy() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("embedding", "VECTOR").withLength(2))
                                      .build();
        StructuredConditionInput input = StructuredConditionInput.term(
                "embedding", VectorStructuredConditions.L2_LESS_THAN,
                Map.of("vector", List.of(1, 2), "distance", 0.5));

        ConditionGroup condition = assertDoesNotThrow(
                () -> VectorStructuredConditions.postgresql().compile(form, input));
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms()
                                          .addTermPackage(VectorTermHandlers.postgresql()).build();
        assertEquals("((embedding <-> cast(? as vector)) < ?)", renderer.renderWhere(condition).sql());
    }

    @Test
    void jsonPathSchemaDoesNotConsumeTheCustomValueCollectionBudget() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("payload", "JSON"))
                                      .build();
        StructuredConditionInput input = StructuredConditionInput.term(
                "payload", JsonStructuredConditions.JSON_PATH_EQUALS, Map.of("path", "status", "value", "active"));

        assertDoesNotThrow(() -> JsonStructuredConditions.standard().compile(
                form, input, StructuredConditionPolicy.defaults().withMaxCollectionSize(1)));
    }

    @Test
    void arrayResolverRetainsTheCustomValueCollectionBudget() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("tags", "TEXT[]"))
                                      .build();
        StructuredConditionInput input = StructuredConditionInput.term(
                "tags", ArrayStructuredConditions.CONTAINS, List.of("a", "b"));

        assertThrows(StructuredConditionException.class,
                     () -> ArrayStructuredConditions.postgresql().compile(
                             form, input, StructuredConditionPolicy.defaults().withMaxCollectionSize(1)));
    }

    @Test
    void jsonContainsRetainsTheRootCollectionBudget() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("payload", "JSON"))
                                      .build();
        StructuredConditionInput input = StructuredConditionInput.term(
                "payload", JsonStructuredConditions.JSON_CONTAINS, List.of("a", "b"));

        assertThrows(StructuredConditionException.class,
                     () -> JsonStructuredConditions.standard().compile(
                             form, input, StructuredConditionPolicy.defaults().withMaxCollectionSize(1)));
    }

    @Test
    void jsonContainsRetainsTheNestedCollectionBudget() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("payload", "JSON"))
                                      .build();
        StructuredConditionInput input = StructuredConditionInput.term(
                "payload", JsonStructuredConditions.JSON_CONTAINS, Map.of("tags", List.of("a", "b")));

        assertThrows(StructuredConditionException.class,
                     () -> JsonStructuredConditions.standard().compile(
                             form, input, StructuredConditionPolicy.defaults().withMaxCollectionSize(1)));
    }

    @Test
    void guardStopsAnInvalidStructureBeforeInvokingAThirdPartyResolver() {
        AtomicInteger invocations = new AtomicInteger();
        StructuredConditionResolver thirdParty = (form, fields, input, policy) -> {
            invocations.incrementAndGet();
            return ConditionGroup.and().build();
        };
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        FormScopeSupport guard = new FormScopeSupport(renderer, thirdParty, DataScope.none());
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("id", "INTEGER"))
                                      .build();
        StructuredConditionInput deepInput = StructuredConditionInput.and(
                StructuredConditionInput.and(StructuredConditionInput.term("id", "=", 1)));

        assertThrows(StructuredConditionException.class,
                     () -> guard.scopedRead(com.flying.orm.rdb.form.spec.QuerySpec.structured(form, deepInput)
                             .withStructuredPolicy(StructuredConditionPolicy.defaults().withMaxDepth(1))));
        assertEquals(0, invocations.get());
    }
}
