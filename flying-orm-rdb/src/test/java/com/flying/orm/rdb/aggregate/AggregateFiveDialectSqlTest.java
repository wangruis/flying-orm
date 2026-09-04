package com.flying.orm.rdb.aggregate;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AggregateFiveDialectSqlTest {

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
                             + ", count(distinct " + tenant + ") as " + tenantCount
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
