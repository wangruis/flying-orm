package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.NullOrder;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.lock.ReadLock;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ComputedQueryProjectionTest {
    @Test
    void wildcardSelectionUsesTheDefaultProjectionAcrossReadTerminals() {
        QuerySpec query = QuerySpec.builder(FORM).select("*").build();
        FormReadPlanner reads = planner(RdbDialect.postgresql()).reads;
        List<String> fields = List.of("id", "amount", "quantity");
        assertAll(
                () -> assertEquals(fields, reads.select(query).outputFields()),
                () -> assertEquals(fields, reads.first(query).outputFields()),
                () -> assertEquals(fields, reads.page(query, PageQuery.of(1, 2)).outputFields()),
                () -> assertEquals(fields, reads.keysetPage(query, KeysetPageQuery.first(2, KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST))).outputFields()),
                () -> assertEquals(fields, reads.keysetPage(query, KeysetPageQuery.first(2,
                        KeysetSort.asc("id", NullOrder.LAST))).outputFields()));
    }

    @Test
    void wildcardSelectionHonorsExclusionsAndReadableFieldsBeforeRendering() {
        QuerySpec query = QuerySpec.builder(FORM).select("*").exclude("quantity")
                .scope(DataScope.none().withFields(FieldScope.readable("id", "quantity"))).build();
        FormReadPlanner.PlannedQuery plan = planner(RdbDialect.postgresql()).reads.select(query);
        assertEquals(List.of("id"), plan.outputFields());
        assertEquals("select \"id\" from \"sales\"", plan.request().sql());
    }

    @Test
    void wildcardSelectionKeepsSortOnlyExpressionOutOfTheResult() {
        QuerySpec query = QuerySpec.builder(FORM).select("*")
                .orderBy(ValueExpression.field("quantity").add(ValueExpression.value(1)), PageSort.Direction.ASC).build();
        FormReadPlanner.PlannedQuery plan = planner(RdbDialect.postgresql()).reads.select(query);
        assertEquals(List.of("id", "amount", "quantity"), plan.outputFields());
        assertEquals(List.of(1), plan.request().parameters());
        assertTrue(plan.request().sql().contains("order by"));
    }

    @Test
    void wildcardAndComputedColumnsKeepAllReadableFieldsThroughSpecComposition() {
        QuerySpec.Builder builder = QuerySpec.builder(FORM).select("*");
        QuerySpec published = builder.build();
        QuerySpec built = builder.expression("label", ValueExpression.value("value")).build();
        QuerySpec composed = published.withExpression("label", ValueExpression.value("value"));
        for (QuerySpec query : List.of(built, composed)) {
            FormReadPlanner.PlannedQuery plan = planner(RdbDialect.postgresql()).reads.select(query);
            assertEquals(List.of("id", "amount", "quantity", "label"), plan.outputFields());
            assertEquals(List.of("value"), plan.request().parameters());
            assertEquals(List.of("id", "label"), planner(RdbDialect.postgresql()).reads.select(
                    query.withScope(DataScope.none().withFields(FieldScope.readable("id", "quantity")))
                            .withExcludedFields(List.of("quantity"))).outputFields());
        }
        assertEquals(List.of("id", "amount", "quantity"),
                planner(RdbDialect.postgresql()).reads.select(published).outputFields());
    }

    @Test
    void relatedWildcardAndComputedColumnsUseTheSameProjectionExpansion() {
        DynamicForm form = DynamicForm.builder("roots", "roots")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("hidden", "VARCHAR"))
                .relation("sales", com.flying.orm.core.join.QueryRelation.of(FORM, "id", "id")).build();
        QuerySpec spec = QuerySpec.builder(form).select("*")
                .expression("next_amount", ValueExpression.field("sales.amount").add(ValueExpression.value(1)))
                .exclude("hidden").build();
        FormReadPlanner.PlannedQuery plan = planner(RdbDialect.postgresql()).reads.select(spec);
        assertEquals(List.of("id", "next_amount"), plan.form().fields().stream().map(DynamicField::name).toList());
        assertEquals(List.of(1), plan.request().parameters());
        assertTrue(plan.request().sql().contains("join"));
    }

    @Test
    void readPlannerOwnsReadPlanningWithoutAStatelessForwardingClass() {
        assertThrows(ClassNotFoundException.class,
                () -> Class.forName("com.flying.orm.rdb.form.FormReadPlanSupport"));
    }

    @Test
    void ordinaryAndComputedReadsShareTheExistingProjectionOwner() {
        assertTrue(java.util.Arrays.stream(FormReadPlanner.class.getDeclaredClasses())
                .noneMatch(type -> type.getSimpleName().equals("ExpressionRead")),
                "computed reads must not retain a second intermediate plan beside Projection");
    }

    @Test
    void groupedProjectionIsValidatedForEveryReadTerminal() {
        QuerySpec query = QuerySpec.of(FORM, ConditionGroup.and().build())
                .withProjection(List.of("id", "amount"), List.of("id"));
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormOperationPlanner planner = planner(dialect);
            assertAll(dialect.toString(),
                    () -> assertThrows(IllegalArgumentException.class, () -> planner.reads.select(query)),
                    () -> assertThrows(IllegalArgumentException.class, () -> planner.reads.first(query)),
                    () -> assertThrows(IllegalArgumentException.class, () -> planner.reads.selectGoverned(
                            query, FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults())),
                    () -> assertThrows(IllegalArgumentException.class, () -> planner.reads.page(query, PageQuery.of(1, 10))));
        }
    }

    @Test
    void sharedPriorityValuesAreBoundPerFieldWithoutRunningCodecsInTheSpec() {
        DynamicField text = DynamicField.of("text", "VARCHAR");
        DynamicForm form = DynamicForm.builder("coded", "coded").addField(text).build();
        java.util.concurrent.atomic.AtomicInteger writes = new java.util.concurrent.atomic.AtomicInteger();
        com.flying.orm.core.codec.ValueCodec codec = new com.flying.orm.core.codec.ValueCodec() {
            public boolean supports(Class<?> type) { return type == String.class; }
            public Object write(Object value) { writes.incrementAndGet(); return "coded:" + value; }
            public Object read(Object value, Class<?> type) { return value; }
        };
        com.flying.orm.rdb.mapping.EntityTypeMappingRegistry registry = com.flying.orm.rdb.mapping.EntityTypeMappingRegistry.builder()
                .registerNamed("coded", String.class, text.databaseType(), codec).build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql())
                .withEntityFieldCodecs(Map.of(text, registry.resolve("coded", String.class)));
        FormOperationPlanner planner = new FormOperationPlanner(renderer, new FormScopeSupport(renderer,
                StructuredConditionResolver.defaults(), DataScope.none()), SqlExecutionOptions.unlimited());
        ValueExpression rank = ValueExpression.field("text").priority("A").add(ValueExpression.value(0));
        QuerySpec selected = QuerySpec.of(form, ConditionGroup.and().build()).withExpression("rank_value", rank);
        assertEquals(0, writes.get());
        assertEquals(List.of("coded:A", 0), planner.reads.select(selected).request().parameters());
        assertEquals(1, writes.get());
        assertEquals(List.of("coded:A", 0), planner.reads.select(QuerySpec.of(form, ConditionGroup.and().build())
                .withOrderBy(rank, PageSort.Direction.ASC)).request().parameters());
        assertEquals(2, writes.get());
    }

    @Test
    void sharedLiteralsUseEachPrioritySourceTypeAndDeepCompositionIsIterative() {
        DynamicForm form = DynamicForm.builder("typed", "typed")
                .addField(DynamicField.of("tags", "TEXT[]"))
                .addField(DynamicField.of("document", "JSON")).build();
        ValueExpression value = ValueExpression.value(List.of("A", "B"));
        ValueExpression arrayRank = ValueExpression.call("priority", ValueExpression.field("tags"), value);
        ValueExpression jsonRank = ValueExpression.call("priority", ValueExpression.field("document"), value);
        QuerySpec spec = QuerySpec.of(form, ConditionGroup.and().build())
                .withExpression("rank_value", arrayRank.add(jsonRank));
        List<Object> parameters = planner(RdbDialect.postgresql()).reads.select(spec).request().parameters();
        assertArrayEquals(new String[] { "A", "B" }, (String[]) parameters.getFirst());
        assertEquals("[\"A\",\"B\"]", parameters.get(1));
        List<Object> mixed = planner(RdbDialect.postgresql()).reads.select(QuerySpec.of(form, ConditionGroup.and().build())
                .withOrderBy(ValueExpression.firstMatch(arrayRank, ValueExpression.lastMatch(jsonRank)), PageSort.Direction.ASC))
                .request().parameters();
        assertArrayEquals(new String[] { "A", "B" }, (String[]) mixed.getFirst());
        assertEquals(List.of("[\"A\",\"B\"]"), mixed.subList(1, mixed.size()));
        for (ValueExpression rank : List.of(ValueExpression.firstMatch(arrayRank, jsonRank),
                ValueExpression.lastMatch(arrayRank, jsonRank))) {
            List<Object> matched = planner(RdbDialect.postgresql()).reads.select(
                    QuerySpec.of(form, ConditionGroup.and().build()).withOrderBy(rank, PageSort.Direction.ASC))
                    .request().parameters();
            assertArrayEquals(new String[] { "A", "B" }, (String[]) matched.getFirst());
            assertEquals("[\"A\",\"B\"]", matched.get(1));
        }
        ValueExpression deep = arrayRank;
        for (int index = 0; index < 2000; index++) deep = deep.add(ValueExpression.value(0));
        QuerySpec composed = QuerySpec.of(form, ConditionGroup.and().build()).withOrderBy(deep, PageSort.Direction.ASC);
        assertEquals(2001, planner(RdbDialect.postgresql()).reads.select(composed).request().parameters().size());
        ValueExpression plain = ValueExpression.field("tags").coalesce(ValueExpression.field("tags"));
        assertSame(plain, QuerySpec.of(form, ConditionGroup.and().build()).withExpression("plain", plain).expressions().get("plain"));
    }

    @Test
    void computedOrderingDoesNotRequireReturningItsValue() {
        ValueExpression priority = ValueExpression.call("priority", ValueExpression.field("quantity"),
                ValueExpression.value(5), ValueExpression.value(3));
        QuerySpec spec = QuerySpec.of(FORM, ConditionGroup.and().where("id", ">", 1L).build())
                .withExpression("rank_value", priority).withSorts(List.of(PageSort.asc("rank_value")))
                .withProjection(List.of("id"), List.of());
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormReadPlanner.PlannedQuery plan = assertDoesNotThrow(() -> planner(dialect).reads.select(spec));
            assertEquals(List.of("id"), plan.outputFields());
            assertEquals(List.of(1L, 5, 3), plan.request().parameters());
            assertTrue(plan.request().sql().contains(" order by case "));
            assertFalse(plan.request().sql().contains("rank_value"));
            FormReadPlanner.PlannedQuery suppliedSort = assertDoesNotThrow(() -> planner(dialect).reads.page(spec.withSorts(List.of()), new PageQuery(1, 10, spec.sorts())));
            assertEquals(List.of(1L, 5, 3), suppliedSort.request().parameters().subList(0, 3));
            assertEquals(List.of("id"), suppliedSort.outputFields());
            assertEquals(List.of(1L), planner(dialect).reads.count(spec, FieldUsePolicy.unrestricted(),
                    QueryShapeLimits.defaults(), false).request().parameters());
        }
    }

    @Test
    void sortOnlyExpressionsPreserveDefaultProjectionExclusionsAndParameterOrder() {
        ValueExpression priority = ValueExpression.call("priority", ValueExpression.field("quantity"),
                ValueExpression.value(5), ValueExpression.value(3));
        QuerySpec original = QuerySpec.of(FORM, ConditionGroup.and().where("id", ">", 1L).build());
        QuerySpec sorted = original.withOrderBy(priority, PageSort.Direction.ASC);
        QuerySpec excluded = sorted.withExcludedFields(List.of("amount", "quantity"));
        assertTrue(sorted.projections().isEmpty());
        assertTrue(original.sorts().isEmpty());
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormOperationPlanner planner = planner(dialect);
            assertEquals(List.of("id", "amount", "quantity"), planner.reads.select(sorted).outputFields());
            FormReadPlanner.PlannedQuery selected = planner.reads.select(excluded);
            assertEquals(List.of("id"), selected.outputFields());
            assertEquals(List.of(1L, 5, 3), selected.request().parameters());
            FormReadPlanner.PlannedQuery page = planner.reads.page(excluded, PageQuery.of(2, 1));
            assertEquals(List.of(1L), page.countRequest().parameters());
            assertEquals(List.of(1L, 5, 3), page.request().parameters().subList(0, 3));
            assertFalse(page.request().sql().contains("__fo_order_"));
            assertEquals(List.of(1L, 5, 3), planner.reads.lockingRead(
                    excluded.withLock(ReadLock.update())).request().parameters());
        }
    }

    @Test
    void sortOnlyExpressionsKeepSourcePermissionsAndGroupingRules() {
        QuerySpec sorted = QuerySpec.of(FORM, ConditionGroup.and().build())
                .withProjection(List.of("id"), List.of())
                .withOrderBy(ValueExpression.field("quantity").add(ValueExpression.value(2)), PageSort.Direction.DESC);
        FormOperationPlanner planner = planner(RdbDialect.postgresql());
        FieldUsePolicy allowed = FieldUsePolicy.builder().visibility("id", FieldVisibility.FULL)
                .allow("quantity", FieldUse.SORT).build();
        assertDoesNotThrow(() -> planner.reads.selectGoverned(sorted, allowed, QueryShapeLimits.defaults()));
        FieldUsePolicy denied = FieldUsePolicy.builder().visibility("id", FieldVisibility.FULL).build();
        assertThrows(ScopeAccessException.class,
                () -> planner.reads.selectGoverned(sorted, denied, QueryShapeLimits.defaults()));
        assertThrows(IllegalArgumentException.class,
                () -> planner.reads.select(sorted.withProjection(List.of("id"), List.of("id"))));
        assertEquals(List.of("id"), planner.reads.select(sorted.withSorts(List.of(PageSort.asc("id")))).outputFields());
    }

    @Test
    void internalSortNamesDoNotShadowRealFields() {
        DynamicForm form = DynamicForm.builder("collision", "collision")
                .addField(DynamicField.of("__fo_order_0", "INTEGER"))
                .addField(DynamicField.of("id", "BIGINT")).build();
        QuerySpec spec = QuerySpec.of(form, ConditionGroup.and().build())
                .withOrderBy(ValueExpression.field("id"), PageSort.Direction.ASC)
                .withOrderBy(ValueExpression.field("__fo_order_0"), PageSort.Direction.DESC);
        FormReadPlanner.PlannedQuery plan = planner(RdbDialect.postgresql()).reads.select(spec);
        assertEquals(List.of("__fo_order_0", "id"), plan.outputFields());
        assertTrue(plan.request().sql().endsWith(" order by \"id\" asc, \"__fo_order_0\" desc"));
        assertTrue(spec.expressions().isEmpty(), "plain field sorts need no generated expression aliases");
        QuerySpec computed = QuerySpec.of(form, ConditionGroup.and().build())
                .withOrderBy(ValueExpression.field("id").priority(2L), PageSort.Direction.ASC)
                .withOrderBy(ValueExpression.field("__fo_order_0").priority(7), PageSort.Direction.DESC);
        assertEquals(2, computed.expressions().size());
        assertFalse(computed.expressions().containsKey("__fo_order_0"));
        FormReadPlanner.PlannedQuery computedPlan = planner(RdbDialect.postgresql()).reads.select(computed);
        assertEquals(plan.outputFields(), computedPlan.outputFields());
        assertEquals(List.of(2L, 7), computedPlan.request().parameters());
        assertTrue(computedPlan.request().sql().contains("case \"__fo_order_0\" when ?"));
    }

    private static final DynamicForm FORM = DynamicForm.builder("sales", "sales")
            .addField(DynamicField.primaryKey("id", "BIGINT"))
            .addField(DynamicField.of("amount", "DECIMAL"))
            .addField(DynamicField.of("quantity", "INTEGER")).build();

    @Test
    void bulkExpressionsPreserveOrderBindingsAndOtherQueryOptions() {
        Map<String, ValueExpression> expressions = new LinkedHashMap<>();
        expressions.put("Units", ValueExpression.field("quantity").coalesce(ValueExpression.value(3)));
        expressions.put("Adjusted", ValueExpression.field("amount").add(ValueExpression.value(7)));
        QuerySpec original = query().withSorts(List.of(PageSort.desc("subtotal")))
                .withScope(DataScope.where(ConditionGroup.and().where("id", ">", 0L).build()))
                .withExecutionOptions(SqlExecutionOptions.unlimited()).showSensitive();
        QuerySpec bulk = original.withExpressions(expressions);
        QuerySpec sequential = original;
        for (Map.Entry<String, ValueExpression> entry : expressions.entrySet()) {
            sequential = sequential.withExpression(entry.getKey(), entry.getValue());
        }
        assertEquals(List.of("id", "subtotal", "Units", "Adjusted"), bulk.projections());
        assertEquals(original.groups(), bulk.groups());
        assertSame(original.where(), bulk.where());
        assertSame(original.scope(), bulk.scope());
        assertEquals(original.sorts(), bulk.sorts());
        assertEquals(original.executionOptions(), bulk.executionOptions());
        assertEquals(original.sensitiveDisplayMode(), bulk.sensitiveDisplayMode());
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            SqlRequest expected = planner(dialect).reads.select(sequential).request();
            SqlRequest actual = planner(dialect).reads.select(bulk).request();
            assertEquals(expected.sql(), actual.sql());
            assertEquals(List.of(2, 3, 7, 1L, 0L), actual.parameters());
            assertEquals(expected.parameters(), actual.parameters());
        }
        expressions.clear();
        assertEquals(3, bulk.expressions().size());
        assertEquals(1, original.expressions().size());
        assertThrows(UnsupportedOperationException.class, () -> bulk.expressions().clear());
        assertSame(bulk, bulk.withExpressions(Map.of()));
    }

    @Test
    void bulkExpressionsRejectCollisionsWithoutChangingTheOriginal() {
        QuerySpec original = query();
        Map<String, ValueExpression> duplicate = new LinkedHashMap<>();
        duplicate.put("extra", ValueExpression.value(1));
        duplicate.put("EXTRA", ValueExpression.value(2));
        assertThrows(IllegalArgumentException.class, () -> original.withExpressions(duplicate));
        assertThrows(IllegalArgumentException.class,
                () -> original.withExpressions(Map.of("SUBTOTAL", ValueExpression.value(1))));
        assertThrows(IllegalArgumentException.class,
                () -> original.withExpressions(Map.of("AMOUNT", ValueExpression.value(1))));
        assertThrows(IllegalArgumentException.class,
                () -> original.withExpressions(Map.of("t.value", ValueExpression.value(1))));
        assertThrows(IllegalArgumentException.class,
                () -> original.withExpressions(Map.of("x;drop table sales", ValueExpression.value(1))));
        duplicate.clear();
        duplicate.put("missing", null);
        assertThrows(NullPointerException.class, () -> original.withExpressions(duplicate));
        assertEquals(List.of("id", "subtotal"), original.projections());
        assertEquals(1, original.expressions().size());
    }

    @Test
    void bulkExpressionsKeepStructuredFiltersAndTheirPolicy() {
        StructuredConditionInput input = StructuredConditionInput.term("quantity", ">", 4);
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults().allowOnlyFields(List.of("quantity"));
        QuerySpec original = QuerySpec.structured(FORM, input).withStructuredPolicy(policy)
                .withWhere(ConditionGroup.and().where("id", "=", 2L).build());
        QuerySpec bulk = original.withExpressions(Map.of("extra", ValueExpression.value(9)));
        assertSame(input, bulk.structuredInput().orElseThrow());
        assertSame(policy, bulk.structuredPolicy().orElseThrow());
        SqlRequest expected = planner(RdbDialect.postgresql()).reads.select(original.withExpression("extra", ValueExpression.value(9))).request();
        SqlRequest actual = planner(RdbDialect.postgresql()).reads.select(bulk).request();
        assertEquals(expected.sql(), actual.sql());
        assertEquals(expected.parameters(), actual.parameters());
        assertEquals(3, actual.parameters().size());
    }

    @Test
    void selectFirstAndPageKeepProjectionBindsBeforeWhereAndCountDoesNotBindThem() {
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormOperationPlanner planner = planner(dialect);
            QuerySpec spec = query().withSorts(List.of(PageSort.asc("id")));
            SqlRequest rows = planner.reads.select(spec).request();
            assertTrue(rows.sql().contains("coalesce("));
            assertTrue(rows.sql().contains("subtotal"));
            assertEquals(List.of(2, 1L), rows.parameters());
            SqlRequest first = planner.reads.first(spec).request();
            if (dialect.name().equals("sqlserver")) {
                assertEquals(List.of(1, 2, 1L), first.parameters());
            } else if (dialect.name().equals("oracle")) {
                assertEquals(List.of(2, 1L, 0L, 1), first.parameters());
            } else {
                assertEquals(List.of(2, 1L, 1, 0L), first.parameters());
            }
            FormReadPlanner.PlannedQuery page = planner.reads.page(spec, PageQuery.of(2, 3));
            assertEquals(List.of(1L), page.countRequest().parameters());
            assertFalse(page.countRequest().sql().contains("coalesce("));
            assertTrue(page.request().sql().contains("coalesce("));
        }
    }

    @Test
    void computedAliasesCannotBypassSourcePermissions() {
        FormOperationPlanner planner = planner(RdbDialect.postgresql());
        FieldUsePolicy denied = FieldUsePolicy.builder().allow("id", FieldUse.FILTER)
                .visibility("id", FieldVisibility.FULL)
                .visibility("subtotal", FieldVisibility.FULL).build();
        assertThrows(ScopeAccessException.class,
                () -> planner.reads.selectGoverned(query(), denied, QueryShapeLimits.defaults()));
        FieldUsePolicy allowed = FieldUsePolicy.builder().allow("id", FieldUse.FILTER)
                .visibility("id", FieldVisibility.FULL).visibility("amount", FieldVisibility.FULL)
                .visibility("quantity", FieldVisibility.FULL).build();
        assertEquals(FieldVisibility.FULL, planner.reads.selectGoverned(query(), allowed,
                QueryShapeLimits.defaults()).fieldUse().visibility("subtotal"));
    }

    @Test
    void fullValueUseWithoutProjectionPermissionCannotPublishAnAlias() {
        FieldUsePolicy internalValueOnly = FieldUsePolicy.builder().allow("id", FieldUse.FILTER)
                .visibility("id", FieldVisibility.FULL)
                .allow("amount", FieldUse.FULL_VALUE).allow("quantity", FieldUse.FULL_VALUE).build();
        FormOperationPlanner planner = planner(RdbDialect.postgresql());
        assertThrows(ScopeAccessException.class,
                () -> planner.reads.selectGoverned(query(), internalValueOnly, QueryShapeLimits.defaults()));
        assertThrows(ScopeAccessException.class,
                () -> planner.reads.pageGoverned(query(), PageQuery.of(1, 2), internalValueOnly, QueryShapeLimits.defaults()));
    }

    @Test
    void specsKeepExpressionsWhenReplacingOtherOptionsAndRejectAliasCollisions() {
        QuerySpec spec = query().withScope(DataScope.none()).showSensitive()
                .withExecutionOptions(SqlExecutionOptions.unlimited())
                .withWhere(ConditionGroup.and().where("id", "=", 2L).build());
        assertEquals(List.of(2, 2L), planner(RdbDialect.postgresql()).reads.select(spec).request().parameters());
        assertThrows(IllegalArgumentException.class,
                () -> query().withExpression("AMOUNT", ValueExpression.value(1)));
        assertThrows(IllegalArgumentException.class,
                () -> query().withExpression("x;drop table sales", ValueExpression.value(1)));
    }

    @Test
    void replacingProjectionDropsUnusedExpressionsAndKeepsSelectedAliasCasing() {
        QuerySpec selected = query().withProjection(List.of("SUBTOTAL", "id"), List.of());
        FormReadPlanner.PlannedQuery plan = planner(RdbDialect.postgresql()).reads.select(selected);
        assertEquals(List.of("SUBTOTAL", "id"), plan.outputFields());
        assertEquals(List.of(2, 1L), plan.request().parameters());
        QuerySpec plain = selected.withProjection(List.of("id"), List.of());
        assertTrue(plain.expressions().isEmpty());
        assertEquals(List.of(1L), planner(RdbDialect.postgresql()).reads.select(plain).request().parameters());
    }

    @Test
    void maskedSourcesCannotBePublishedUnderANewAlias() {
        DynamicForm form = DynamicForm.builder("private", "private_values")
                .addField(DynamicField.of("name", "VARCHAR"))
                .masked("name", MaskedFieldDefinition.builder("partial").build()).build();
        QuerySpec spec = QuerySpec.of(form, ConditionGroup.and().build()).withExpression("display_name",
                ValueExpression.field("name").coalesce(ValueExpression.value("anonymous")));
        FormOperationPlanner planner = planner(RdbDialect.postgresql());
        assertThrows(IllegalArgumentException.class, () -> planner.reads.select(spec));
        assertDoesNotThrow(() -> planner.reads.select(spec.showSensitive()));
        FieldUsePolicy masked = FieldUsePolicy.builder().visibility("name", FieldVisibility.MASKED).build();
        assertThrows(ScopeAccessException.class,
                () -> planner.reads.selectGoverned(spec.showSensitive(), masked, QueryShapeLimits.defaults()));
    }

    @Test
    void aliasSortsResolveSelectedCasingAndKeepProjectionParametersSingle() {
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormOperationPlanner planner = planner(dialect);
            QuerySpec spec = query().withProjection(List.of("id", "SUBTOTAL"), List.of())
                    .withSorts(List.of(PageSort.desc("subtotal"), PageSort.asc("id")));
            SqlRequest request = planner.reads.select(spec).request();
            assertTrue(request.sql().contains("SUBTOTAL"));
            assertTrue(request.sql().substring(request.sql().indexOf("order by")).contains("SUBTOTAL"));
            assertEquals(List.of(2, 1L), request.parameters());
            assertDoesNotThrow(() -> planner.reads.first(spec));
            FormReadPlanner.PlannedQuery page = planner.reads.page(spec.withSorts(List.of()),
                    new PageQuery(2, 3, List.of(PageSort.desc("subtotal"))));
            assertTrue(page.request().sql().contains("order by"));
            assertEquals(List.of(1L), page.countRequest().parameters());
        }
    }

    @Test
    void computedSortMetadataDoesNotExposeUnreadableOrdinaryFieldsOrUnknownAliases() {
        FormOperationPlanner planner = planner(RdbDialect.postgresql());
        QuerySpec spec = QuerySpec.of(FORM, ConditionGroup.and().build())
                .withExpression("literal", ValueExpression.value(1))
                .withScope(DataScope.none().withFields(FieldScope.readable("id")));
        assertDoesNotThrow(() -> planner.reads.select(spec.withSorts(List.of(PageSort.asc("literal")))));
        assertThrows(IllegalArgumentException.class,
                () -> planner.reads.select(spec.withSorts(List.of(PageSort.asc("amount")))));
        assertThrows(IllegalArgumentException.class,
                () -> planner.reads.select(spec.withSorts(List.of(PageSort.asc("not_selected")))));
    }

    @Test
    void aliasOrderingRequiresSortPermissionOnEverySourceNotOnAlias() {
        QuerySpec spec = query().withSorts(List.of(PageSort.desc("subtotal")));
        FormOperationPlanner planner = planner(RdbDialect.postgresql());
        FieldUsePolicy allowed = FieldUsePolicy.builder().allow("id", FieldUse.FILTER)
                .visibility("id", FieldVisibility.FULL).visibility("amount", FieldVisibility.FULL)
                .visibility("quantity", FieldVisibility.FULL)
                .allow("amount", FieldUse.SORT).allow("quantity", FieldUse.SORT).build();
        assertDoesNotThrow(() -> planner.reads.selectGoverned(spec, allowed, QueryShapeLimits.defaults()));
        assertDoesNotThrow(() -> planner.reads.pageGoverned(spec, PageQuery.of(1, 2), allowed, QueryShapeLimits.defaults()));
        assertDoesNotThrow(() -> planner.reads.lockingReadGoverned(spec.withLock(ReadLock.update()),
                allowed, QueryShapeLimits.defaults()));
        FieldUsePolicy denied = FieldUsePolicy.builder().allow("id", FieldUse.FILTER)
                .visibility("id", FieldVisibility.FULL).visibility("amount", FieldVisibility.FULL)
                .visibility("quantity", FieldVisibility.FULL)
                .allow("amount", FieldUse.SORT).allow("subtotal", FieldUse.SORT).build();
        assertThrows(ScopeAccessException.class,
                () -> planner.reads.selectGoverned(spec, denied, QueryShapeLimits.defaults()));
        assertThrows(ScopeAccessException.class, () -> planner.reads.pageGoverned(spec.withSorts(List.of()),
                new PageQuery(1, 2, spec.sorts()), denied, QueryShapeLimits.defaults()));
        assertThrows(ScopeAccessException.class, () -> planner.reads.lockingReadGoverned(
                spec.withLock(ReadLock.update()), denied, QueryShapeLimits.defaults()));
    }

    @Test
    void groupedOffsetPageCountsGroupsWithoutProjectionParameters() {
        QuerySpec grouped = QuerySpec.of(FORM, ConditionGroup.and().where("id", ">", 5L).build())
                .withProjection(List.of("quantity"), List.of("quantity"))
                .withExpression("shifted", ValueExpression.field("quantity").add(ValueExpression.value(3)))
                .withSorts(List.of(PageSort.asc("shifted")))
                .withScope(DataScope.where(ConditionGroup.and().where("id", "<", 100L).build()));
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormOperationPlanner planner = planner(dialect);
            FormReadPlanner.PlannedQuery plan = planner.reads.page(grouped, PageQuery.of(3, 9));
            assertTrue(plan.countRequest().sql().contains("from (select 1 as group_marker from"));
            assertTrue(plan.countRequest().sql().contains("group by"));
            assertFalse(plan.countRequest().sql().contains("shifted"));
            assertFalse(plan.countRequest().sql().contains("order by"));
            assertEquals(List.of(5L, 100L), plan.countRequest().parameters());
            assertEquals(List.of(3, 5L, 100L), plan.request().parameters().subList(0, 3));
            assertTrue(plan.request().sql().contains("group by"));
            assertTrue(plan.request().sql().contains("order by"));
            assertThrows(IllegalArgumentException.class, () -> planner.reads.page(
                    grouped.withProjection(List.of("shifted"), List.of("amount")), PageQuery.of(1, 2)));
        }
    }

    @Test
    void groupedOffsetPageKeepsGroupPermissionsBudgetsAndProjectionRules() {
        FormOperationPlanner planner = planner(RdbDialect.postgresql());
        QuerySpec ordinary = QuerySpec.of(FORM, ConditionGroup.and().build())
                .withProjection(List.of("quantity"), List.of("quantity"))
                .withSorts(List.of(PageSort.asc("quantity")));
        QuerySpec computed = ordinary.withExpression("units", ValueExpression.field("quantity"));
        FieldUsePolicy denied = FieldUsePolicy.builder().visibility("quantity", FieldVisibility.FULL)
                .allow("quantity", FieldUse.SORT).build();
        for (QuerySpec spec : List.of(ordinary, computed)) {
            assertDoesNotThrow(() -> planner.reads.pageGoverned(spec, PageQuery.of(1, 2),
                    FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults().withMaxGroupCount(1)));
            assertThrows(ScopeAccessException.class, () -> planner.reads.pageGoverned(spec, PageQuery.of(1, 2),
                    denied, QueryShapeLimits.defaults()));
            IllegalArgumentException budget = assertThrows(IllegalArgumentException.class,
                    () -> planner.reads.pageGoverned(spec, PageQuery.of(1, 2), FieldUsePolicy.unrestricted(),
                            QueryShapeLimits.defaults().withMaxGroupCount(0)));
            assertTrue(budget.getMessage().contains("group"));
        }
        assertThrows(IllegalArgumentException.class, () -> planner.reads.page(
                ordinary.withProjection(List.of("id"), List.of("quantity")), PageQuery.of(1, 2)));
        assertThrows(IllegalArgumentException.class, () -> planner.reads.page(
                ordinary.withSorts(List.of(PageSort.asc("id"))), PageQuery.of(1, 2)));
        assertThrows(IllegalArgumentException.class, () -> planner.reads.page(
                ordinary.withProjection(List.of(), List.of("quantity")), PageQuery.of(1, 2)));
    }

    @Test
    void groupedPageExecutesCountAndOffsetAgainstTheSameGroups() throws Exception {
        QuerySpec grouped = QuerySpec.of(FORM, ConditionGroup.and().build())
                .withExpression("units", ValueExpression.field("quantity").coalesce(ValueExpression.value(0)))
                .withProjection(List.of("units"), List.of("quantity"))
                .withSorts(List.of(PageSort.asc("units")));
        FormReadPlanner.PlannedQuery plan = planner(RdbDialect.postgresql()).reads.page(grouped, PageQuery.of(2, 1));
        try (java.sql.Connection connection = java.sql.DriverManager.getConnection(
                "jdbc:h2:mem:grouped_page_" + java.util.UUID.randomUUID() + ";MODE=PostgreSQL");
             java.sql.Statement setup = connection.createStatement()) {
            setup.execute("create table \"sales\" (\"id\" bigint primary key, \"amount\" decimal, \"quantity\" integer)");
            setup.execute("insert into \"sales\" values (1, 0, null), (2, 0, null), (3, 0, 4), (4, 0, 4), (5, 0, 8)");
            try (java.sql.PreparedStatement count = connection.prepareStatement(plan.countRequest().sql());
                 java.sql.ResultSet result = count.executeQuery()) {
                assertTrue(result.next());
                assertEquals(3L, result.getLong(1));
                assertFalse(result.next());
            }
            try (java.sql.PreparedStatement data = connection.prepareStatement(plan.request().sql())) {
                for (int index = 0; index < plan.request().parameters().size(); index++) {
                    data.setObject(index + 1, plan.request().parameters().get(index));
                }
                try (java.sql.ResultSet result = data.executeQuery()) {
                    assertTrue(result.next());
                    assertEquals(4, result.getInt("units"));
                    assertFalse(result.next());
                }
            }
        }
    }

    @Test
    void groupedOffsetPageChecksEachStatementBudgetIndependently() {
        FormOperationPlanner planner = planner(RdbDialect.postgresql());
        QuerySpec grouped = QuerySpec.of(FORM, ConditionGroup.and().build())
                .withProjection(List.of("quantity"), List.of("quantity"));
        QuerySpec computed = grouped.withExpression("units", ValueExpression.value(0))
                .withProjection(List.of("units"), List.of("quantity"));
        for (QuerySpec spec : List.of(grouped, computed)) {
            FormReadPlanner.PlannedQuery plan = planner.reads.page(spec, PageQuery.of(1, 2));
            int countLength = plan.countRequest().sql().length();
            int dataLength = plan.request().sql().length();
            assertTrue(countLength > dataLength);
            QueryShapeLimits tooShort = QueryShapeLimits.defaults().withMaxSqlLength(dataLength);
            IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                    () -> planner.reads.pageGoverned(spec, PageQuery.of(1, 2), FieldUsePolicy.unrestricted(), tooShort));
            assertTrue(failure.getMessage().contains("SQL length"));
            assertDoesNotThrow(() -> planner.reads.pageGoverned(spec, PageQuery.of(1, 2), FieldUsePolicy.unrestricted(),
                    QueryShapeLimits.defaults().withMaxSqlLength(countLength)
                            .withMaxBindCount(plan.request().parameters().size())));
        }
    }

    @Test
    void computedGroupedProjectionMustUseGroupedSources() {
        FormOperationPlanner planner = planner(RdbDialect.postgresql());
        QuerySpec grouped = QuerySpec.of(FORM, ConditionGroup.and().build())
                .withExpression("units", ValueExpression.field("quantity").coalesce(ValueExpression.value(0)))
                .withProjection(List.of("units"), List.of("quantity"));
        assertTrue(planner.reads.select(grouped.withSorts(List.of(PageSort.desc("units"))))
                .request().sql().contains("group by"));
        assertThrows(IllegalArgumentException.class, () -> planner.reads.select(
                grouped.withProjection(List.of("units"), List.of("amount"))));
    }

    @Test
    void lockingComputedProjectionPreservesDialectPlacementAndParameters() {
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormOperationPlanner planner = planner(dialect);
            QuerySpec spec = query().withSorts(List.of(PageSort.desc("subtotal")));
            for (ReadLock lock : List.of(ReadLock.update(), ReadLock.updateNowait(), ReadLock.updateSkipLocked())) {
                SqlRequest request = planner.reads.lockingRead(spec.withLock(lock)).request();
                assertEquals(List.of(2, 1L), request.parameters());
                assertTrue(request.sql().contains("order by"));
                if (dialect.name().equals("sqlserver")) {
                    assertTrue(request.sql().contains(" WITH (UPDLOCK, ROWLOCK"));
                    assertTrue(request.sql().indexOf(" WITH (") < request.sql().indexOf(" where "));
                    assertFalse(request.sql().contains("FOR UPDATE"));
                } else {
                    String expected = switch (lock.waitMode()) {
                        case WAIT -> " FOR UPDATE";
                        case NOWAIT -> " FOR UPDATE NOWAIT";
                        case SKIP_LOCKED -> " FOR UPDATE SKIP LOCKED";
                    };
                    assertTrue(request.sql().endsWith(expected), request.sql());
                }
            }
            QuerySpec grouped = QuerySpec.of(FORM, ConditionGroup.and().build())
                    .withExpression("units", ValueExpression.field("quantity"))
                    .withProjection(List.of("units"), List.of("quantity"));
            assertThrows(UnsupportedOperationException.class,
                    () -> planner.reads.lockingRead(grouped.withLock(ReadLock.update())));
        }
    }

    @Test
    void cursorComputedProjectionPreservesBindingsAndSourcePermissions() {
        KeysetPageQuery cursor = KeysetPageQuery.after(2, com.flying.orm.core.page.CursorPosition.of(List.of(1L)), KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST));
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormOperationPlanner planner = planner(dialect);
            SqlRequest actual = planner.reads.keysetPage(query(), cursor).request();
            QuerySpec plain = QuerySpec.of(FORM, query().where()).withProjection(List.of("id"), List.of());
            SqlRequest expected = planner.reads.keysetPage(plain, cursor).request();
            assertEquals(2, actual.parameters().getFirst());
            assertEquals(expected.parameters(), actual.parameters().subList(1, actual.parameters().size()));
            assertTrue(actual.sql().contains("coalesce("));
            assertTrue(actual.sql().contains("order by"));
            assertTrue(planner.reads.keysetPage(
                    query().withProjection(List.of("subtotal"), List.of()), cursor).layout().hasHiddenSelections());
        }
        FieldUsePolicy denied = FieldUsePolicy.builder().visibility("id", FieldVisibility.FULL)
                .allow("id", FieldUse.SORT).allow("id", FieldUse.FILTER).build();
        assertThrows(ScopeAccessException.class, () -> planner(RdbDialect.postgresql()).reads.keysetPageGoverned(query(), cursor, denied, QueryShapeLimits.defaults()));
    }

    @Test
    void keysetComputedProjectionPreservesHiddenFieldsBindingsAndSourcePermissions() {
        KeysetPageQuery page = KeysetPageQuery.after(2,
                com.flying.orm.core.page.CursorPosition.of(List.of(1L)), KeysetSort.asc("id", NullOrder.LAST));
        QuerySpec computed = query().withProjection(List.of("subtotal"), List.of());
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormOperationPlanner planner = planner(dialect);
            FormReadPlanner.PlannedKeysetPage plan = planner.reads.keysetPage(computed, page);
            SqlRequest actual = plan.request();
            SqlRequest plain = planner.reads.keysetPage(QuerySpec.of(FORM, computed.where())
                    .withProjection(List.of("amount"), List.of()), page).request();
            assertEquals(2, actual.parameters().getFirst());
            assertEquals(plain.parameters(), actual.parameters().subList(1, actual.parameters().size()));
            assertTrue(actual.sql().contains("coalesce("));
            assertTrue(actual.sql().contains("__fo_ks_0"));
            assertTrue(plan.layout().hasHiddenSelections());
            assertEquals(List.of("subtotal"), plan.outputFields());
        }
        FieldUsePolicy denied = FieldUsePolicy.builder().visibility("id", FieldVisibility.FULL)
                .allow("id", FieldUse.SORT).allow("id", FieldUse.FILTER).build();
        assertThrows(ScopeAccessException.class, () -> planner(RdbDialect.postgresql()).reads.keysetPageGoverned(computed, page, denied, QueryShapeLimits.defaults()));
        assertThrows(IllegalArgumentException.class, () -> planner(RdbDialect.postgresql()).reads.keysetPage(computed, KeysetPageQuery.first(2, KeysetSort.asc("subtotal", NullOrder.LAST))));
    }

    private static QuerySpec query() {
        return QuerySpec.of(FORM, ConditionGroup.and().where("id", "=", 1L).build())
                .withProjection(List.of("id"), List.of())
                .withExpression("subtotal", ValueExpression.field("amount")
                        .multiply(ValueExpression.field("quantity").coalesce(ValueExpression.value(2))));
    }

    private static FormOperationPlanner planner(RdbDialect dialect) {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), dialect);
        return new FormOperationPlanner(renderer, new FormScopeSupport(renderer,
                StructuredConditionResolver.defaults(), DataScope.none()), SqlExecutionOptions.unlimited());
    }
}
