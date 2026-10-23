package com.flying.orm.rdb.form;

import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateSpec;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.sql.expression.SqlExpressionFunctions;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class RegisteredAggregateContractTest {
    private static final DynamicForm FORM = DynamicForm.builder("sales", "sales")
            .addField(DynamicField.of("amount", "DECIMAL"))
            .addField(DynamicField.of("weight", "INTEGER"))
            .addField(DynamicField.of("category", "VARCHAR")).build();

    @Test
    void registeredAggregateExecutesGroupedHavingAndPageSql() throws Exception {
        AggregateExpression<BigDecimal> average = weighted(ValueExpression.field("amount"),
                ValueExpression.field("weight").coalesce(ValueExpression.value(2)));
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(FORM,
                ConditionGroup.and().where("amount", ">", 5).build()))
                .group("category", "category").aggregate(average)
                .having(ConditionGroup.and().where("average", ">", 20).build())
                .orderBy(PageSort.desc("average")).build();
        FormAggregatePlanner.Plan plan = planner(RdbDialect.postgresql(), FieldUsePolicy.unrestricted())
                .planPage(spec, PageQuery.of(1, 1), false);
        try (Connection connection = DriverManager.getConnection(
                "jdbc:h2:mem:registered_aggregate;DATABASE_TO_LOWER=TRUE")) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("create table sales(amount decimal(10,2), weight integer, category varchar(16))");
                statement.execute("insert into sales values (10, 2, 'a'), (20, 2, 'a'), (100, null, 'b')");
            }
            for (com.flying.orm.core.sql.render.SqlRequest request : List.of(
                    plan.countRequest().orElseThrow(), plan.request())) {
                try (PreparedStatement statement = connection.prepareStatement(request.sql())) {
                    List<Object> parameters = request.parameters();
                    for (int i = 0; i < parameters.size(); i++) statement.setObject(i + 1, parameters.get(i));
                    try (ResultSet rows = statement.executeQuery()) {
                        assertTrue(rows.next());
                        if (request == plan.request()) {
                            assertEquals("b", rows.getString("category"));
                            assertEquals(0, new BigDecimal("100").compareTo(rows.getBigDecimal("average")));
                        } else {
                            assertEquals(1L, rows.getLong("total"));
                        }
                        assertFalse(rows.next());
                    }
                }
            }
        }
    }

    @Test
    void registeredFunctionSharesGroupingHavingPagingAndTypedDecoding() {
        AggregateExpression<BigDecimal> average = weighted(ValueExpression.field("amount"),
                ValueExpression.field("weight").coalesce(ValueExpression.value(2)));
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(FORM,
                ConditionGroup.and().where("amount", ">", 1).build()))
                .group("category", "category").aggregate(average)
                .having(ConditionGroup.and().where("average", ">", 3).build())
                .orderBy(PageSort.desc("average")).build();
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormAggregatePlanner.Plan plan = planner(dialect, FieldUsePolicy.unrestricted())
                    .planPage(spec, PageQuery.of(1, 5), false);
            assertEquals(List.of(2, 2, 1, 3), plan.countRequest().orElseThrow().parameters());
            String sql = plan.request().sql();
            assertTrue(sql.contains("sum("), sql);
            assertFalse(sql.contains("sum(sum("), sql);
            assertTrue(sql.contains("group by"), sql);
            assertTrue(sql.contains("flying_aggregate_result"), sql);
            assertTrue(sql.contains("order by"), sql);
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("category", "books");
            values.put("average", new BigDecimal("12.50"));
            assertEquals(new BigDecimal("12.50"), new AggregateResultDecoder(plan)
                    .decode(DynamicRow.copyOf(values)).get(average));
        }
    }

    @Test
    void registrationAndResultTypesRemainExplicit() {
        FormAggregatePlanner planner = planner(RdbDialect.postgresql(), FieldUsePolicy.unrestricted());
        AggregateExpression<String> wrongType = AggregateExpression.function("total", "result",
                LogicalType.TEXT, String.class, ValueExpression.field("amount"));
        AggregateExpression<BigDecimal> unknown = AggregateExpression.function("not-registered", "result",
                LogicalType.DECIMAL, BigDecimal.class, ValueExpression.field("amount"));
        for (AggregateExpression<?> expression : List.of(wrongType, unknown)) {
            assertThrows(IllegalArgumentException.class, () -> planner.plan(spec(expression)));
        }
        AggregateExpression<BigDecimal> total = AggregateExpression.function("total", "result",
                LogicalType.DECIMAL, BigDecimal.class, ValueExpression.field("amount"));
        FormAggregatePlanner.Plan plan = planner.plan(spec(total));
        assertTrue(plan.request().sql().contains("sum(\"amount\")"));
        assertEquals(new BigDecimal("42"), new AggregateResultDecoder(plan)
                .decode(DynamicRow.copyOf(Map.of("result", 42))).get(total));
        Map<String, Object> empty = new LinkedHashMap<>();
        empty.put("result", null);
        assertNull(new AggregateResultDecoder(plan).decode(DynamicRow.copyOf(empty)).get(total));
    }

    @Test
    void everyInputRetainsFieldUseAndVisibility() {
        AggregateExpression<BigDecimal> average = weighted(ValueExpression.field("amount"), ValueExpression.field("weight"));
        AggregateSpec spec = spec(average);
        FieldUsePolicy missing = FieldUsePolicy.builder().allow("amount", FieldUse.AGGREGATE).build();
        assertThrows(ScopeAccessException.class, () -> planner(RdbDialect.postgresql(), missing).plan(spec));
        FieldUsePolicy hidden = FieldUsePolicy.builder().allow("amount", FieldUse.AGGREGATE)
                .allow("weight", FieldUse.AGGREGATE).visibility("weight", FieldVisibility.HIDDEN).build();
        FormAggregatePlanner.Plan plan = planner(RdbDialect.postgresql(), hidden).plan(spec);
        assertNull(new AggregateResultDecoder(plan).decode(DynamicRow.copyOf(Map.of("average", 42))).get(average));
        FieldUsePolicy masked = FieldUsePolicy.builder().allow("amount", FieldUse.AGGREGATE)
                .allow("weight", FieldUse.AGGREGATE).visibility("weight", FieldVisibility.MASKED).build();
        assertThrows(IllegalArgumentException.class, () -> planner(RdbDialect.postgresql(), masked).plan(spec));
    }

    @Test
    void registeredFunctionPreservesSelfJoinSourceIdentity() {
        JoinQuerySpec.Builder relation = JoinQuerySpec.builder(FORM);
        JoinSource left = relation.root();
        JoinSource right = relation.join(JoinType.LEFT, FORM, left, "category", "category");
        AggregateExpression<BigDecimal> average = weighted(ValueExpression.field(left.field("amount")),
                ValueExpression.field(right.field("weight")));
        AggregateSpec spec = AggregateSpec.builder(relation.buildRelation()).aggregate(average).build();
        String sql = planner(RdbDialect.postgresql(), FieldUsePolicy.unrestricted()).plan(spec).request().sql();
        assertTrue(sql.contains("sum(\"t0\".\"amount\" * \"t1\".\"weight\")"), sql);
        assertTrue(sql.contains("nullif(sum(\"t1\".\"weight\"), 0)"), sql);
    }

    @Test
    void zeroArgumentAggregateAndBooleanResultsUseTheExistingCodec() {
        AggregateExpression<Boolean> result = AggregateExpression.function("any-row", "result",
                LogicalType.BOOLEAN, Boolean.class);
        FormAggregatePlanner.Plan plan = planner(RdbDialect.postgresql(), FieldUsePolicy.unrestricted()).plan(spec(result));
        assertTrue(plan.request().sql().contains("count(*) > 0"));
        assertTrue(plan.request().parameters().isEmpty());
        assertEquals(Boolean.TRUE, new AggregateResultDecoder(plan)
                .decode(DynamicRow.copyOf(Map.of("result", true))).get(result));
    }

    @Test
    void registeredFunctionCannotReadEncryptedSourceValues() {
        DynamicForm secrets = DynamicForm.builder("secrets", "secrets")
                .addField(DynamicField.of("amount", "VARCHAR"))
                .encrypted("amount", com.flying.orm.core.protection.EncryptedFieldDefinition.builder().build()).build();
        AggregateExpression<BigDecimal> total = AggregateExpression.function("total", "result",
                LogicalType.DECIMAL, BigDecimal.class, ValueExpression.field("amount"));
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(secrets, ConditionGroup.and().build())).aggregate(total).build();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("test", new byte[32]);
             ProtectedFieldRuntime protection = ProtectedFieldRuntime.create(keys)) {
            SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms().expressionFunctions(
                    SqlExpressionFunctions.builder().register("total", "sum", LogicalType.DECIMAL,
                            LogicalType.DECIMAL).build()).build();
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, RdbDialect.postgresql())
                    .withProtectedFields(protection);
            FormAggregatePlanner planner = new FormAggregatePlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults());
            IllegalArgumentException error = assertThrows(IllegalArgumentException.class, () -> planner.plan(spec));
            assertEquals("encrypted field must not be used as an aggregate source", error.getMessage());
        }
    }

    private static AggregateExpression<BigDecimal> weighted(ValueExpression amount, ValueExpression weight) {
        return AggregateExpression.function("weighted-mean", "average", LogicalType.DECIMAL,
                BigDecimal.class, amount, weight);
    }

    private static AggregateSpec spec(AggregateExpression<?> expression) {
        return AggregateSpec.builder(QuerySpec.of(FORM, ConditionGroup.and().build())).aggregate(expression).build();
    }

    private static FormAggregatePlanner planner(RdbDialect dialect, FieldUsePolicy policy) {
        SqlExpressionFunctions functions = SqlExpressionFunctions.builder()
                .register("any-row", call -> {
                    call.requireArity(0);
                    call.sql("(count(*) > 0)");
                    return LogicalType.BOOLEAN;
                })
                .register("total", "sum", LogicalType.DECIMAL, LogicalType.DECIMAL)
                .register("weighted-mean", call -> {
                    call.requireArity(2);
                    call.requireNumericArguments();
                    call.sql("(sum(").argument(0).sql(" * ").argument(1)
                            .sql(") / nullif(sum(").argument(1).sql("), 0))");
                    return LogicalType.DECIMAL;
                }).build();
        SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms().expressionFunctions(functions).build();
        return new FormAggregatePlanner(FormDataSqlRenderer.create(conditions, dialect),
                new FormScopeSupport(FormDataSqlRenderer.create(conditions, dialect), StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                policy,
                QueryShapeLimits.defaults());
    }
}
