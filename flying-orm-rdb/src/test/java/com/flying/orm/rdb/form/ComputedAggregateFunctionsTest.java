package com.flying.orm.rdb.form;

import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateFunction;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.aggregate.AggregateRow;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class ComputedAggregateFunctionsTest {
    private static final DynamicForm FORM = DynamicForm.builder("items", "items")
            .addField(DynamicField.of("amount", "DECIMAL"))
            .addField(DynamicField.of("name", "VARCHAR"))
            .build();

    @Test
    void groupingDoesNotRequireAnUnwantedAggregateColumn() {
        AggregateSpec spec = assertDoesNotThrow(() -> AggregateSpec.builder(
                QuerySpec.of(FORM, ConditionGroup.and().build())).group("name", "bucket").build());
        FormAggregatePlanner.Plan plan = planner(RdbDialect.postgresql(), FieldUsePolicy.unrestricted()).plan(spec);
        assertEquals("select \"name\" as \"bucket\" from \"items\" group by \"name\"", plan.request().sql());
        assertEquals(1, plan.layout().size());
        assertEquals("alpha", new AggregateResultDecoder(plan).decode(row(List.of("bucket"), List.of("alpha")))
                .get("bucket", String.class));
    }

    @Test
    void computedGroupsBindOnceAcrossSelectionHavingSortingAndPageCount() {
        ValueExpression bucket = ValueExpression.call("lower",
                ValueExpression.field("name").coalesce(ValueExpression.value("empty")));
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(FORM,
                        ConditionGroup.and().where("amount", ">", 0).build()))
                .group(bucket, "bucket")
                .aggregate(AggregateExpression.sum(ValueExpression.field("amount").add(ValueExpression.value(1)), "total"))
                .having(having -> having.where("total", ">", 2))
                .orderByFirst("bucket", "alpha").build();
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(), RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormAggregatePlanner.Plan plan = planner(dialect, FieldUsePolicy.unrestricted()).planPage(spec, PageQuery.of(1, 5), false);
            assertEquals(List.of(1, "empty", 0, 2), plan.countRequest().orElseThrow().parameters());
            assertEquals(1, plan.request().sql().split("coalesce", -1).length - 1, plan.request().sql());
            assertTrue(plan.request().sql().contains("group by"), plan.request().sql());
            AggregateRow result = new AggregateResultDecoder(plan).decode(row(List.of("bucket", "total"), List.of("alpha", 8)));
            assertEquals("alpha", result.get("bucket", String.class));
            assertEquals(new BigDecimal("8"), result.get("total", BigDecimal.class));
        }
    }

    @Test
    void computedGroupingExecutesWithNoRepeatedParameterIdentity() throws Exception {
        ValueExpression bucket = ValueExpression.call("lower",
                ValueExpression.field("name").coalesce(ValueExpression.value("empty")));
        AggregateSpec.Builder builder = AggregateSpec.builder(QuerySpec.of(FORM, ConditionGroup.and().build()))
                .group(bucket, "bucket");
        try (java.sql.Connection connection = java.sql.DriverManager.getConnection("jdbc:h2:mem:computed_group;DB_CLOSE_DELAY=0")) {
            try (java.sql.Statement statement = connection.createStatement()) {
                statement.execute("create table \"items\" (\"name\" varchar, \"amount\" decimal)");
                statement.execute("insert into \"items\" values ('Alpha',2),('alpha',4),(null,3)");
            }
            SqlRequest request = planner(RdbDialect.postgresql(), FieldUsePolicy.unrestricted()).plan(builder.build()).request();
            try (java.sql.PreparedStatement statement = connection.prepareStatement(request.sql())) {
                for (int i = 0; i < request.parameters().size(); i++) statement.setObject(i + 1, request.parameters().get(i));
                try (java.sql.ResultSet result = statement.executeQuery()) {
                    java.util.Set<String> buckets = new java.util.HashSet<>();
                    while (result.next()) buckets.add(result.getString(1));
                    assertEquals(java.util.Set.of("alpha", "empty"), buckets);
                }
            }
            AggregateSpec aggregate = builder.aggregate(AggregateExpression.sum(
                    ValueExpression.field("amount").add(ValueExpression.value(1)), "total"))
                    .having(having -> having.where("total", ">", 5)).orderByFirst("bucket", "alpha").build();
            FormAggregatePlanner.Plan page = planner(RdbDialect.postgresql(), FieldUsePolicy.unrestricted())
                    .planPage(aggregate, PageQuery.of(1, 5), false);
            for (SqlRequest sql : List.of(page.countRequest().orElseThrow(), page.request())) {
                try (java.sql.PreparedStatement statement = connection.prepareStatement(sql.sql())) {
                    for (int i = 0; i < sql.parameters().size(); i++) statement.setObject(i + 1, sql.parameters().get(i));
                    try (java.sql.ResultSet result = statement.executeQuery()) {
                        assertTrue(result.next());
                        if (sql == page.request()) {
                            assertEquals("alpha", result.getString("bucket"));
                            assertEquals(0, new BigDecimal("8").compareTo(result.getBigDecimal("total")));
                        } else assertEquals(1L, result.getLong(1));
                        assertFalse(result.next());
                    }
                }
            }
        }
    }

    @Test
    void computedGroupVisibilityUsesEverySourceAndDoesNotInheritTextMasking() {
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(FORM, ConditionGroup.and().build()))
                .group(ValueExpression.call("length", ValueExpression.field("name")), "length").build();
        FieldUsePolicy hidden = FieldUsePolicy.builder().allow("name", FieldUse.GROUP, FieldUse.PROJECT)
                .visibility("name", FieldVisibility.HIDDEN).build();
        assertThrows(com.flying.orm.core.scope.ScopeAccessException.class,
                () -> planner(RdbDialect.postgresql(), hidden).plan(spec));
        FieldUsePolicy masked = FieldUsePolicy.builder().allow("name", FieldUse.GROUP, FieldUse.PROJECT)
                .visibility("name", FieldVisibility.MASKED).build();
        assertThrows(IllegalArgumentException.class, () -> planner(RdbDialect.postgresql(), masked).plan(spec));
    }

    @Test
    void joinedComputedGroupsKeepSourceIdentityAndRejectForeignSources() {
        com.flying.orm.core.join.JoinQuerySpec.Builder relation = com.flying.orm.core.join.JoinQuerySpec.builder(FORM);
        com.flying.orm.core.join.JoinSource other = relation.join(com.flying.orm.core.join.JoinType.LEFT,
                FORM, relation.root(), "name", "name");
        ValueExpression key = ValueExpression.field(other.field("name")).coalesce(ValueExpression.value("empty"));
        AggregateSpec spec = AggregateSpec.builder(relation.buildRelation()).group(key, "bucket").countRows("rows").build();
        FormAggregatePlanner planner = planner(RdbDialect.postgresql(), FieldUsePolicy.unrestricted());
        FormAggregatePlanner.Plan plan = planner.plan(spec);
        assertTrue(plan.request().sql().contains("coalesce(\"t1\".\"name\", ?)"));
        assertTrue(plan.request().sql().contains("count(*)"));
        assertEquals(List.of("empty"), plan.request().parameters());
        AggregateSpec single = AggregateSpec.builder(QuerySpec.of(FORM, ConditionGroup.and().build()))
                .group(key, "bucket").build();
        assertThrows(IllegalArgumentException.class, () -> planner.plan(single));
    }

    @Test
    void groupedFunctionsCannotBypassExplicitEncryptionOrMasks() {
        DynamicForm masked = DynamicForm.builder("masked", "items").addField(DynamicField.of("name", "VARCHAR"))
                .masked("name", com.flying.orm.core.protection.MaskedFieldDefinition.builder("partial").build()).build();
        ValueExpression key = ValueExpression.call("length", ValueExpression.field("name"));
        FormAggregatePlanner planner = planner(RdbDialect.postgresql(), FieldUsePolicy.unrestricted());
        QuerySpec query = QuerySpec.of(masked, ConditionGroup.and().build());
        assertThrows(IllegalArgumentException.class, () -> planner.plan(AggregateSpec.builder(query).group(key, "bucket").build()));
        assertDoesNotThrow(() -> planner.plan(AggregateSpec.builder(query.showSensitive()).group(key, "bucket").build()));
        DynamicForm encrypted = DynamicForm.builder("encrypted", "items").addField(DynamicField.of("name", "VARCHAR"))
                .encrypted("name", com.flying.orm.core.protection.EncryptedFieldDefinition.builder().build()).build();
        assertThrows(IllegalArgumentException.class, () -> planner.plan(AggregateSpec.builder(
                QuerySpec.of(encrypted, ConditionGroup.and().build())).group(key, "bucket").build()));
    }

    @Test
    void countDistinctMinAndMaxPreserveTheirOwnResultTypes() {
        AggregateExpression<Long> count = computed(AggregateFunction.COUNT, "counted", LogicalType.BIG_INTEGER,
                Long.class, ValueExpression.field("name").coalesce(ValueExpression.value("fallback")));
        AggregateExpression<Long> distinct = computed(AggregateFunction.COUNT_DISTINCT, "distinct_names", LogicalType.BIG_INTEGER,
                Long.class, ValueExpression.call("lower", ValueExpression.field("name")));
        AggregateExpression<String> minimum = computed(AggregateFunction.MIN, "minimum", LogicalType.TEXT,
                String.class, ValueExpression.call("lower", ValueExpression.field("name")));
        AggregateExpression<BigDecimal> maximum = computed(AggregateFunction.MAX, "maximum", LogicalType.DECIMAL,
                BigDecimal.class, ValueExpression.field("amount").add(ValueExpression.value(1)));
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(FORM, ConditionGroup.and().build()))
                .aggregate(count).aggregate(distinct).aggregate(minimum).aggregate(maximum).build();
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(), RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormAggregatePlanner.Plan plan = planner(dialect, FieldUsePolicy.unrestricted()).plan(spec);
            assertEquals(List.of("fallback", 1), plan.request().parameters());
            assertTrue(plan.request().sql().contains("(distinct lower("), plan.request().sql());
            AggregateRow row = new AggregateResultDecoder(plan).decode(row(
                    List.of("counted", "distinct_names", "minimum", "maximum"), List.of(3, 2, "alpha", 21)));
            assertEquals(3L, row.get(count));
            assertEquals(2L, row.get(distinct));
            assertEquals("alpha", row.get(minimum));
            assertEquals(new BigDecimal("21"), row.get(maximum));
        }
    }

    @Test
    void computedTextHavingAndPagingDoNotDuplicateProjectionBindings() {
        AggregateExpression<String> minimum = computed(AggregateFunction.MIN, "minimum", LogicalType.TEXT,
                String.class, ValueExpression.field("name").coalesce(ValueExpression.value("fallback")));
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(FORM,
                ConditionGroup.and().where("amount", ">", 2).build())).aggregate(minimum)
                .having(ConditionGroup.and().where("minimum", "=", "alpha").build()).build();
        FormAggregatePlanner.Plan plan = planner(RdbDialect.postgresql(), FieldUsePolicy.unrestricted())
                .planPage(spec, PageQuery.of(1, 5), false);
        assertEquals(List.of("fallback", 2, "alpha"), plan.countRequest().orElseThrow().parameters());
        assertTrue(plan.request().sql().contains("flying_aggregate_result"));
    }

    @Test
    void computedResultCannotBypassHiddenOrMaskedSources() {
        AggregateExpression<String> minimum = computed(AggregateFunction.MIN, "minimum", LogicalType.TEXT,
                String.class, ValueExpression.call("lower", ValueExpression.field("name")));
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(FORM, ConditionGroup.and().build())).aggregate(minimum).build();
        FieldUsePolicy hidden = FieldUsePolicy.builder().allow("name", FieldUse.AGGREGATE)
                .visibility("name", FieldVisibility.HIDDEN).build();
        FormAggregatePlanner.Plan plan = planner(RdbDialect.postgresql(), hidden).plan(spec);
        assertNull(new AggregateResultDecoder(plan).decode(row(List.of("minimum"), List.of("alpha"))).get(minimum));
        FieldUsePolicy masked = FieldUsePolicy.builder().allow("name", FieldUse.AGGREGATE)
                .visibility("name", FieldVisibility.MASKED).build();
        assertThrows(IllegalArgumentException.class, () -> planner(RdbDialect.postgresql(), masked).plan(spec));
    }

    @Test
    void computedMinStillChecksTheDeclaredJavaAndLogicalTypes() {
        for (AggregateExpression<?> expression : List.of(
                computed(AggregateFunction.MIN, "minimum", LogicalType.INTEGER, Integer.class,
                        ValueExpression.call("lower", ValueExpression.field("name"))),
                computed(AggregateFunction.MIN, "minimum", LogicalType.TEXT, Integer.class,
                        ValueExpression.call("lower", ValueExpression.field("name"))))) {
            AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(FORM, ConditionGroup.and().build())).aggregate(expression).build();
            assertThrows(IllegalArgumentException.class, () -> planner(RdbDialect.postgresql(), FieldUsePolicy.unrestricted()).plan(spec));
        }
    }

    private static <T> AggregateExpression<T> computed(AggregateFunction function, String alias,
                                                       LogicalType type, Class<T> javaType, ValueExpression operand) {
        return assertDoesNotThrow(() -> new AggregateExpression<>(function, operand, alias, type, javaType));
    }

    @Test
    void declaredMaskCannotBeRemovedByWrappingTheFieldInAFunction() {
        DynamicForm form = DynamicForm.builder("private_items", "items")
                .addField(DynamicField.of("name", "VARCHAR"))
                .masked("name", com.flying.orm.core.protection.MaskedFieldDefinition.builder("partial").build()).build();
        AggregateExpression<String> minimum = AggregateExpression.min(ValueExpression.call("lower",
                ValueExpression.field("name")), "minimum", LogicalType.TEXT, String.class);
        QuerySpec query = QuerySpec.of(form, ConditionGroup.and().build());
        FormAggregatePlanner planner = planner(RdbDialect.postgresql(), FieldUsePolicy.unrestricted());
        assertThrows(IllegalArgumentException.class,
                () -> planner.plan(AggregateSpec.builder(query).aggregate(minimum).build()));
        assertThrows(IllegalArgumentException.class,
                () -> planner.plan(AggregateSpec.builder(query.masked()).aggregate(minimum).build()));
        assertDoesNotThrow(() -> planner.plan(AggregateSpec.builder(query.showSensitive()).aggregate(minimum).build()));
        AggregateExpression<String> direct = AggregateExpression.min(ValueExpression.field("name"), "minimum",
                LogicalType.TEXT, String.class);
        FormAggregatePlanner.Plan directPlan = planner.plan(AggregateSpec.builder(query).aggregate(direct).build());
        assertNotEquals("alpha", new AggregateResultDecoder(directPlan)
                .decode(row(List.of("minimum"), List.of("alpha"))).get(direct));
    }

    @Test
    void coalesceDoesNotTurnALobIntoAComparableScalar() {
        DynamicForm form = DynamicForm.builder("documents", "documents")
                .addField(DynamicField.of("body", "CLOB")).build();
        ValueExpression input = ValueExpression.field("body").coalesce(ValueExpression.value(null));
        for (AggregateExpression<?> expression : List.of(
                AggregateExpression.countDistinct("body", "result"),
                AggregateExpression.min("body", "result", LogicalType.TEXT, String.class),
                AggregateExpression.max("body", "result", LogicalType.TEXT, String.class),
                AggregateExpression.countDistinct(input, "result"),
                AggregateExpression.min(input, "result", LogicalType.TEXT, String.class),
                AggregateExpression.max(input, "result", LogicalType.TEXT, String.class))) {
            AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(form, ConditionGroup.and().build()))
                    .aggregate(expression).build();
            assertThrows(IllegalArgumentException.class,
                    () -> planner(RdbDialect.oracle(), FieldUsePolicy.unrestricted()).plan(spec), expression.toString());
        }
        AggregateSpec length = AggregateSpec.builder(QuerySpec.of(form, ConditionGroup.and().build()))
                .aggregate(AggregateExpression.sum(ValueExpression.call("length", input), "length")).build();
        assertDoesNotThrow(() -> planner(RdbDialect.oracle(), FieldUsePolicy.unrestricted()).plan(length));
    }

    private static FormAggregatePlanner planner(RdbDialect dialect, FieldUsePolicy policy) {
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().expressionFunctions(
                com.flying.orm.core.sql.expression.SqlExpressionFunctions.builder().register("lower", call -> {
                    call.requireArity(1);
                    call.sql("lower(").argument(0).sql(")");
                    return LogicalType.TEXT;
                }).register("length", call -> {
                    call.requireArity(1);
                    call.sql("length(").argument(0).sql(")");
                    return LogicalType.INTEGER;
                }).build()).build();
        return new FormAggregatePlanner(FormDataSqlRenderer.create(renderer, dialect),
                new FormScopeSupport(FormDataSqlRenderer.create(renderer, dialect), StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                policy,
                QueryShapeLimits.defaults());
    }

    private static DynamicRow row(List<String> names, List<?> values) {
        Map<String, Object> row = new LinkedHashMap<>();
        for (int index = 0; index < names.size(); index++) row.put(names.get(index), values.get(index));
        return DynamicRow.copyOf(row);
    }
}
