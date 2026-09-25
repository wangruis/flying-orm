package com.flying.orm.rdb.aggregate;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class AggregateFiveDialectSqlTest {

    @Test
    void postgresqlUuidExtremaUseStableOrderingAndKeepUuidHavingAndResults() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        DynamicForm form = DynamicForm.builder("events", "events")
                .addField(DynamicField.of("id", "UUID")).build();
        AggregateExpression<UUID> minimum = AggregateExpression.min("id", "first", LogicalType.UUID, UUID.class);
        AggregateExpression<UUID> maximum = AggregateExpression.max("id", "last", LogicalType.UUID, UUID.class);
        UUID boundary = UUID.fromString("80000000-0000-0000-0000-000000000001");
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(form, ConditionGroup.and().build()))
                .aggregate(minimum).aggregate(maximum)
                .having(AggregateHaving.of(ConditionGroup.and().where("last", ">", boundary).build())).build();
        FormAggregatePlanner.Plan plan = new FormAggregatePlanner(renderer,
                StructuredConditionResolver.defaults(renderer.valueCodecs()), DataScope.none(),
                SqlExecutionOptions.safeDefaults(), FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults())
                .plan(spec);

        assertEquals("select cast(min(cast(\"id\" as text) collate \"C\") as uuid) as \"first\", "
                + "cast(max(cast(\"id\" as text) collate \"C\") as uuid) as \"last\" from \"events\" "
                + "having cast(max(cast(\"id\" as text) collate \"C\") as uuid) > ?", plan.request().sql());
        assertEquals(List.of(boundary), plan.request().parameters());
        UUID first = UUID.fromString("00000000-0000-0000-0000-000000000001");
        UUID last = UUID.fromString("ffffffff-ffff-ffff-ffff-ffffffffffff");
        Map<String, Object> columns = new LinkedHashMap<>();
        columns.put("first", first);
        columns.put("last", last);
        AggregateRow row = new AggregateResultDecoder(plan).decode(DynamicRow.copyOf(columns));
        assertEquals(first, row.get(minimum));
        assertEquals(last, row.get(maximum));
        Map<String, Object> nulls = new LinkedHashMap<>();
        nulls.put("first", null);
        nulls.put("last", null);
        AggregateRow empty = new AggregateResultDecoder(plan).decode(DynamicRow.copyOf(nulls));
        assertNull(empty.get(minimum));
        assertNull(empty.get(maximum));
    }

    @Test
    void rendersTheSameTypedShapeWithDialectIdentifiers() {
        for (RdbDialect dialect : List.of(
                RdbDialect.h2(),
                RdbDialect.mysql(),
                RdbDialect.postgresql(),
                RdbDialect.oracle(),
                RdbDialect.sqlServer())) {
            assertDialectSql(dialect);
        }
    }

    @Test
    void sqlServerPromotesIntegerBeforeSumToAvoidIntOverflow() {
        assertEquals(
                "select sum(cast([amount] as decimal(38,10))) as [amount_sum] from [orders]",
                sqlServerIntegerAggregate(AggregateExpression.sum("amount", "amount_sum")));
    }

    @Test
    void sqlServerPromotesIntegerBeforeAverageToPreserveFraction() {
        assertEquals(
                "select avg(cast([amount] as decimal(38,10))) as [amount_avg] from [orders]",
                sqlServerIntegerAggregate(AggregateExpression.avg("amount", "amount_avg")));
    }

    @Test
    void sqlServerCountAndDistinctCountKeepLongResultsInSelectAndHaving() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.sqlServer());
        DynamicForm form = DynamicForm.builder("orders", "orders")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        AggregateExpression<Long> count = AggregateExpression.count("id", "rows");
        AggregateExpression<Long> distinct = AggregateExpression.countDistinct("id", "distinct_rows");
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(form, ConditionGroup.and().build()))
                .aggregate(count).aggregate(distinct)
                .having(AggregateHaving.of(ConditionGroup.and()
                        .where("rows", ">", 2_147_483_647L)
                        .where("distinct_rows", ">", 2_147_483_647L).build())).build();
        FormAggregatePlanner.Plan plan = new FormAggregatePlanner(renderer,
                StructuredConditionResolver.defaults(renderer.valueCodecs()), DataScope.none(),
                SqlExecutionOptions.safeDefaults(), FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults())
                .plan(spec);

        assertEquals("select count_big([id]) as [rows], count_big(distinct [id]) as [distinct_rows]"
                + " from [orders] having count_big([id]) > ? and count_big(distinct [id]) > ?",
                plan.request().sql());
        assertEquals(List.of(2_147_483_647L, 2_147_483_647L), plan.request().parameters());
        Map<String, Object> columns = new LinkedHashMap<>();
        columns.put("rows", 2_147_483_648L);
        columns.put("distinct_rows", 2_147_483_648L);
        AggregateRow result = new AggregateResultDecoder(plan).decode(DynamicRow.copyOf(columns));
        assertEquals(2_147_483_648L, result.get(count));
        assertEquals(2_147_483_648L, result.get(distinct));
    }

    private static String sqlServerIntegerAggregate(AggregateExpression<BigDecimal> expression) {
        RdbDialect dialect = RdbDialect.sqlServer();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), dialect);
        DynamicForm form = DynamicForm.builder("orders", "orders")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("amount", "INTEGER"))
                .build();
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(
                        form, ConditionGroup.and().build()))
                .aggregate(expression)
                .build();

        return new FormAggregatePlanner(
                renderer,
                StructuredConditionResolver.defaults(renderer.valueCodecs()),
                DataScope.none(),
                SqlExecutionOptions.safeDefaults(),
                FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults()).plan(spec).request().sql();
    }

    private static void assertDialectSql(RdbDialect dialect) {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), dialect);
        DynamicForm form = DynamicForm.builder("orders", "orders")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("tenant_id", "BIGINT"))
                .addField(DynamicField.of("status", "VARCHAR"))
                .addField(DynamicField.of("amount", "DECIMAL"))
                .build();
        QuerySpec query = QuerySpec.of(form, ConditionGroup.and().build())
                .withSorts(List.of(PageSort.desc("gross")));
        AggregateSpec spec = AggregateSpec.builder(query)
                .group(GroupSelection.of("status", "status_group"))
                .aggregate(AggregateExpression.countDistinct("tenant_id", "tenant_count"))
                .aggregate(AggregateExpression.sum("amount", "gross"))
                .aggregate(AggregateExpression.avg("amount", "average"))
                .having(AggregateHaving.of(
                        ConditionGroup.and().where("gross", ">", BigDecimal.TEN).build()))
                .build();

        FormAggregatePlanner.Plan plan = new FormAggregatePlanner(
                renderer,
                StructuredConditionResolver.defaults(renderer.valueCodecs()),
                DataScope.none(),
                SqlExecutionOptions.safeDefaults(),
                FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults()).plan(spec);

        String status = renderer.conditionRenderer().identifier("status");
        String statusGroup = renderer.conditionRenderer().identifier("status_group");
        String tenant = renderer.conditionRenderer().identifier("tenant_id");
        String tenantCount = renderer.conditionRenderer().identifier("tenant_count");
        String amount = renderer.conditionRenderer().identifier("amount");
        String gross = renderer.conditionRenderer().identifier("gross");
        String average = renderer.conditionRenderer().identifier("average");
        String orders = renderer.conditionRenderer().identifier("orders");
        assertEquals("select " + status + " as " + statusGroup
                             + ", " + ("sqlserver".equals(dialect.name()) ? "count_big" : "count")
                             + "(distinct " + tenant + ") as " + tenantCount
                             + ", sum(" + amount + ") as " + gross
                             + ", avg(" + amount + ") as " + average
                             + " from " + orders
                             + " group by " + status
                             + " having sum(" + amount + ") > ?"
                             + " order by " + gross + " desc",
                     plan.request().sql());
        assertEquals(List.of(BigDecimal.TEN), plan.request().parameters());
    }

}
