package com.flying.orm.rdb.form;

import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.aggregate.GroupSelection;
import com.flying.orm.rdb.aggregate.AggregateRow;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class AggregateRowCountContractTest {
    @Test
    void unusedCorrelatedExtensionDoesNotChangeOrdinaryHavingSql() {
        SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms().addTerm(SqlTermHandler.relationExists(
                "allowed-size", "allowed_counts", "a", "row_count", "org_id")).build();
        AggregateSpec spec = AggregateSpec.builder(query()).aggregate(countRows("rows"))
                .having(ConditionGroup.and().where("rows", ">", 2L).build()).build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, RdbDialect.postgresql());
        FormAggregatePlanner.Plan plan = new FormAggregatePlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(renderer.valueCodecs()), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults()).plan(spec);
        assertEquals("select count(*) as \"rows\" from \"orders\" having count(*) > ?", plan.request().sql());
    }

    @Test
    void correlatedHavingBindsToMaterializedAggregateResultNotInnerRelationRows() {
        SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms().addTerm(SqlTermHandler.relationExists(
                "allowed-size", "allowed_counts", "flying_aggregate_result", "row_count", "org_id")).build();
        AggregateSpec spec = AggregateSpec.builder(query().withSorts(List.of(PageSort.desc("rows"))))
                .group(GroupSelection.of("status", "status"))
                .aggregate(countRows("rows"))
                .aggregate(AggregateExpression.sum("amount", "gross"))
                .having(ConditionGroup.or(conditions.terms())
                        .where("rows", "allowed-size", 7L).where("gross", ">", BigDecimal.TEN).build()).build();
        for (RdbDialect dialect : dialects()) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, dialect);
            FormAggregatePlanner.Plan plan = new FormAggregatePlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(renderer.valueCodecs()), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults())
                    .planPage(spec, PageQuery.of(1, 10), false);
            String result = dialect.schema().identifier("flying_aggregate_result");
            String relation = dialect.schema().identifier("flying_aggregate_result_relation");
            String sql = plan.request().sql();
            assertTrue(sql.startsWith("select * from (select "), sql);
            assertTrue(sql.contains(") " + result + " where "), sql);
            assertTrue(sql.contains("exists (select 1 from "), sql);
            assertTrue(sql.contains(relation + "." + dialect.schema().identifier("row_count") + " = "
                    + result + "." + dialect.schema().identifier("rows")), sql);
            assertTrue(sql.contains(" or " + result + "." + dialect.schema().identifier("gross") + " > ?"), sql);
            assertEquals(List.of(7L, BigDecimal.TEN), plan.countRequest().orElseThrow().parameters());
            assertFalse(sql.substring(sql.indexOf("exists (select")).contains("count(*)"), sql);
        }
    }

    @Test
    void rendersRowCountWithoutSubstitutingANullableColumnAcrossDialects() {
        AggregateExpression<Long> rows = countRows("rows");
        AggregateExpression<Long> values = AggregateExpression.count("amount", "values");
        AggregateSpec spec = AggregateSpec.builder(query())
                .aggregate(rows).aggregate(values).aggregate(AggregateExpression.sum("amount", "sum")).build();
        for (RdbDialect dialect : dialects()) {
            FormAggregatePlanner.Plan plan = planner(dialect, FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults())
                    .plan(spec);
            String count = dialect.name().equals("sqlserver") ? "count_big" : "count";
            assertEquals("select " + count + "(*) as " + dialect.schema().identifier("rows") + ", "
                    + count + "(" + dialect.schema().identifier("amount") + ") as "
                    + dialect.schema().identifier("values") + ", sum(" + dialect.schema().identifier("amount")
                    + ") as " + dialect.schema().identifier("sum")
                    + " from " + dialect.schema().identifier("orders"), plan.request().sql());
            assertTrue(plan.request().parameters().isEmpty());
        }
    }

    @Test
    void rowCountHasNoSourceAndRetainsAnExactLongContract() {
        AggregateExpression<Long> rows = countRows("rows");
        assertNull(rows.operand());
        assertEquals(Long.class, rows.javaType());
        assertEquals(LogicalType.BIG_INTEGER, rows.resultLogicalType());
        assertThrows(IllegalArgumentException.class, () -> new AggregateExpression<>(
                rows.function(), com.flying.orm.core.expression.ValueExpression.field("amount"), "rows", LogicalType.BIG_INTEGER, Long.class));
        assertThrows(IllegalArgumentException.class, () -> new AggregateExpression<>(
                rows.function(), null, "rows", LogicalType.DECIMAL, BigDecimal.class));
        assertThrows(IllegalArgumentException.class, () -> countRows(" "));
        assertThrows(IllegalArgumentException.class, () -> planner(RdbDialect.postgresql(),
                FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults())
                .plan(AggregateSpec.builder(query()).aggregate(countRows("rows;delete")).build()));
        assertThrows(IllegalArgumentException.class, () -> AggregateExpression.count((String) null, "values"));
    }

    @Test
    void havingOrderingAndPaginationUseRowExpressionAndFinalGroupCount() {
        AggregateExpression<Long> rows = countRows("rows");
        AggregateSpec spec = AggregateSpec.builder(query().withSorts(List.of(PageSort.desc("rows"))))
                .group(GroupSelection.of("status", "status"))
                .aggregate(rows)
                .having(ConditionGroup.and().where("rows", ">", 2L).build()).build();
        for (RdbDialect dialect : dialects()) {
            FormAggregatePlanner.Plan plan = planner(dialect, FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults())
                    .planPage(spec, PageQuery.of(2, 10), false);
            String count = dialect.name().equals("sqlserver") ? "count_big" : "count";
            assertTrue(plan.request().sql().contains(" having " + count + "(*) > ?"));
            assertTrue(plan.request().sql().contains(" order by " + dialect.schema().identifier("rows") + " desc"));
            assertEquals(List.of(2L), plan.countRequest().orElseThrow().parameters());
            assertFalse(plan.countRequest().orElseThrow().sql().contains(" order by "));
            assertTrue(plan.countRequest().orElseThrow().sql().contains("group by"));
            assertTrue(plan.countRequest().orElseThrow().sql().contains("having " + count + "(*) > ?"));
        }
    }

    @Test
    void countKeepsTenantLogicDeleteAndBusinessFilters() {
        DynamicForm form = DynamicForm.builder("orders", "orders")
                .addField(DynamicField.of("tenant", "BIGINT"))
                .addField(DynamicField.of("deleted", "INTEGER"))
                .addField(DynamicField.of("amount", "DECIMAL"))
                .tenant("tenant", TenantStrategy.AUTO).logicDelete("deleted", 0, 1).build();
        QuerySpec query = QuerySpec.of(form, ConditionGroup.and().where("amount", ">", BigDecimal.ZERO).build())
                .withScope(DataScope.tenant("tenant", 7L));
        FormAggregatePlanner.Plan plan = planner(RdbDialect.postgresql(), FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults()).plan(AggregateSpec.builder(query).aggregate(countRows("rows")).build());
        assertTrue(plan.request().sql().startsWith("select count(*) as \"rows\""));
        assertTrue(plan.request().sql().contains("\"tenant\" = ?"));
        assertTrue(plan.request().sql().contains("\"deleted\" = ?"));
        assertEquals(List.of(BigDecimal.ZERO, 7L, 0), plan.request().parameters());
    }

    @Test
    void resultAliasDoesNotBecomeASourceFieldForApprovalOrMasking() {
        DynamicForm form = DynamicForm.builder("orders", "orders")
                .addField(DynamicField.of("secret", "VARCHAR"))
                .masked("secret", MaskedFieldDefinition.builder("full").build()).build();
        AggregateExpression<Long> rows = countRows("secret");
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(form, ConditionGroup.and().build())
                        .withSorts(List.of(PageSort.desc("secret"))))
                .aggregate(rows)
                .having(ConditionGroup.and().where("secret", ">", 0L).build()).build();
        FieldUsePolicy policy = FieldUsePolicy.builder().visibility("secret", FieldVisibility.MASKED).build();
        FormAggregatePlanner.Plan plan = planner(RdbDialect.postgresql(), policy, QueryShapeLimits.defaults()).plan(spec);
        assertEquals(3L, new AggregateResultDecoder(plan).decode(DynamicRow.copyOf(Map.of("secret", 3L))).get(rows));
        AggregateSpec filtered = AggregateSpec.builder(QuerySpec.of(form,
                ConditionGroup.and().where("secret", "=", "value").build())).aggregate(rows).build();
        assertThrows(ScopeAccessException.class,
                () -> planner(RdbDialect.postgresql(), policy, QueryShapeLimits.defaults()).plan(filtered));
    }

    @Test
    void noFieldDependencyDoesNotBypassCallerShapeBudgets() {
        AggregateSpec spec = AggregateSpec.builder(query()).aggregate(countRows("rows"))
                .aggregate(countRows("again")).build();
        assertThrows(IllegalArgumentException.class, () -> planner(RdbDialect.postgresql(),
                FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults().withMaxAggregateCount(1)).plan(spec));
    }

    @Test
    void exactLongDecodingIsSharedWithFieldCounts() {
        AggregateExpression<Long> rows = countRows("rows");
        AggregateExpression<Long> values = AggregateExpression.count("amount", "values");
        FormAggregatePlanner.Plan plan = planner(RdbDialect.postgresql(), FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults()).plan(AggregateSpec.builder(query()).aggregate(rows).aggregate(values).build());
        AggregateResultDecoder decoder = new AggregateResultDecoder(plan);
        Map<String, Object> raw = new LinkedHashMap<>();
        raw.put("rows", new BigDecimal("3.0"));
        raw.put("values", BigInteger.ONE);
        AggregateRow decoded = decoder.decode(DynamicRow.copyOf(raw));
        assertEquals(3L, decoded.get(rows));
        assertEquals(1L, decoded.get(values));
        raw.put("rows", new BigDecimal("0.5"));
        assertThrows(IllegalArgumentException.class, () -> decoder.decode(DynamicRow.copyOf(raw)));
        raw.put("rows", BigInteger.ONE.shiftLeft(63));
        assertThrows(IllegalArgumentException.class, () -> decoder.decode(DynamicRow.copyOf(raw)));
        raw.put("rows", 0L);
        assertEquals(0L, decoder.decode(DynamicRow.copyOf(raw)).get(rows));
    }

    private static AggregateExpression<Long> countRows(String alias) {
        return AggregateExpression.countRows(alias);
    }

    private static QuerySpec query() {
        DynamicForm form = DynamicForm.builder("orders", "orders")
                .addField(DynamicField.of("status", "VARCHAR"))
                .addField(DynamicField.of("amount", "DECIMAL")).build();
        return QuerySpec.of(form, ConditionGroup.and().build());
    }

    private static List<RdbDialect> dialects() {
        return List.of(RdbDialect.postgresql(), RdbDialect.mysql(), RdbDialect.oracle(), RdbDialect.sqlServer());
    }

    private static FormAggregatePlanner planner(RdbDialect dialect, FieldUsePolicy policy, QueryShapeLimits limits) {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), dialect);
        return new FormAggregatePlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(renderer.valueCodecs()), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                policy,
                limits);
    }
}
