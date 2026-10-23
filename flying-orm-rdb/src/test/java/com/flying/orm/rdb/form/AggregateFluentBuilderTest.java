package com.flying.orm.rdb.form;

import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.aggregate.GroupSelection;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

class AggregateFluentBuilderTest {
    @Test
    void fieldShorthandAndExpressionShareOneSelectionAndHavingPlan() {
        AggregateExpression<BigDecimal> shorthand = AggregateExpression.sum("amount", "total");
        AggregateExpression<BigDecimal> explicit = AggregateExpression.sum(ValueExpression.field("amount"), "total");
        assertEquals(shorthand, explicit);
        AggregateSpec left = builder().aggregate(shorthand)
                .having(condition -> condition.where("total", ">", BigDecimal.TEN)).build();
        AggregateSpec right = builder().aggregate(explicit)
                .having(condition -> condition.where("total", ">", BigDecimal.TEN)).build();
        FormAggregatePlanner planner = planner(RdbDialect.postgresql());
        sameRequest(planner.plan(left).request(), planner.plan(right).request());
        assertFalse(planner.plan(right).request().sql().contains("flying_aggregate_result"));
    }

    @Test
    void aliasValidationReturnsTheNamesForOrderAllocation() throws Exception {
        java.lang.reflect.Method validation = AggregateSpec.class.getDeclaredMethod("validateAliases", List.class, List.class);
        validation.setAccessible(true);
        assertEquals(java.util.Set.of("bucket", "total"), validation.invoke(null,
                List.of(GroupSelection.of("status", "BUCKET")), List.of(AggregateExpression.sum("amount", "Total"))));
    }

    @Test
    void preferredResultOrderingUsesBoundValuesWithoutChangingCount() {
        AggregateSpec.Builder report = builder().group("status", "bucket").sum("amount", "total")
                .orderByFirst("bucket", "paid", "pending");
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormAggregatePlanner.Plan plan = planner(dialect).planPage(report.build(), PageQuery.of(2, 7), false);
            assertEquals(List.of("paid"), plan.countRequest().orElseThrow().parameters());
            assertFalse(plan.countRequest().orElseThrow().sql().contains("order by"));
            SqlRequest expected = dialect.pagination().paginate(
                    planner(dialect).plan(report.build()).request().sql(), List.of("paid", "paid", "pending"), PageQuery.of(2, 7));
            sameRequest(expected, plan.request());
            assertTrue(plan.request().sql().contains("case " + dialect.schema().identifier("status") + " when ?"));
            assertEquals(2, plan.layout().size());
        }
    }

    @Test
    void computedAndPlainOrdersKeepDeclarationOrderAndPageOverride() {
        AggregateSpec spec = builder().group("status", "__fo_order_0").sum("amount", "total")
                .orderByFirst("__fo_order_0", "paid").orderBy(PageSort.desc("total"))
                .orderByLast("total", BigDecimal.TEN).build();
        FormAggregatePlanner planner = planner(RdbDialect.postgresql());
        SqlRequest request = planner.plan(spec).request();
        assertTrue(request.sql().endsWith("case \"status\" when ? then 0 else 1 end asc, \"total\" desc, "
                + "case when sum(\"amount\") = ? then 1 else 0 end asc"), request.sql());
        assertEquals(List.of("paid", "paid", BigDecimal.TEN), request.parameters());
        assertEquals(3, spec.sorts().size());
        assertFalse(spec.sortExpressions().containsKey("__fo_order_0"));
        FormAggregatePlanner.Plan page = planner.planPage(spec, PageQuery.of(1, 10, PageSort.asc("__fo_order_0")), false);
        assertTrue(page.request().sql().endsWith("order by \"__fo_order_0\" asc limit ? offset ?"));
        assertEquals(List.of("paid", 10, 0L), page.request().parameters());
    }

    @Test
    void unknownResultReferencesAreRejectedEvenInsideExpressions() {
        AggregateSpec spec = builder().countRows("rows")
                .orderBy(ValueExpression.field("status").priority("paid"), PageSort.Direction.ASC).build();
        assertThrows(IllegalArgumentException.class, () -> planner(RdbDialect.postgresql()).plan(spec));
        AggregateSpec collision = builder().countRows("rows").orderByFirst("rows", 1L)
                .orderBy(PageSort.asc("__fo_order_0")).build();
        assertThrows(IllegalArgumentException.class, () -> planner(RdbDialect.postgresql()).plan(collision));
    }

    @Test
    void fluentSelectionsProduceTheExistingTypedPlanAcrossDialects() {
        AggregateSpec.Builder shorthand = builder().group("status", "bucket")
                .countRows("rows").count("amount", "present")
                .countDistinct("amount", "distinct_amounts")
                .sum("amount", "total").avg("amount", "average")
                .having(condition -> condition.where("total", ">", BigDecimal.TEN));
        AggregateSpec expected = builder().group(GroupSelection.of("status", "bucket"))
                .aggregate(AggregateExpression.countRows("rows"))
                .aggregate(AggregateExpression.count("amount", "present"))
                .aggregate(AggregateExpression.countDistinct("amount", "distinct_amounts"))
                .aggregate(AggregateExpression.sum("amount", "total"))
                .aggregate(AggregateExpression.avg("amount", "average"))
                .having(ConditionGroup.and().where("total", ">", BigDecimal.TEN).build())
                .build();
        AggregateSpec actual = shorthand.build();
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormAggregatePlanner planner = planner(dialect);
            PageQuery page = PageQuery.of(2, 7, PageSort.asc("bucket"));
            FormAggregatePlanner.Plan left = planner.planPage(actual, page, false);
            FormAggregatePlanner.Plan right = planner.planPage(expected, page, false);
            sameRequest(right.request(), left.request());
            sameRequest(right.countRequest().orElseThrow(), left.countRequest().orElseThrow());
        }
    }

    @Test
    void havingCallbackReplacesTheSpecificationAndSnapshotsNestedConditions() {
        AtomicReference<ConditionGroup.Builder> captured = new AtomicReference<>();
        AggregateSpec.Builder spec = builder().aggregate(AggregateExpression.countRows("rows"))
                .having(ConditionGroup.and().where("rows", ">", 999L).build());
        spec.having(condition -> {
            captured.set(condition);
            condition.or(or -> or.where("rows", "=", 1L).where("rows", "=", 2L))
                    .where("rows", "<", 10L);
        });
        AggregateSpec published = spec.build();
        captured.get().where("rows", "=", 1000L);
        SqlRequest request = planner(RdbDialect.postgresql()).plan(published).request();
        assertEquals(List.of("paid", 1L, 2L, 10L), request.parameters());
        assertTrue(request.sql().contains(" or "), request.sql());
    }

    @Test
    void shorthandStillUsesBoundValuesAndRejectsUnknownHavingAliases() {
        String input = "x' OR 1=1 --";
        AggregateSpec.Builder spec = builder().group("status", "bucket").countRows("rows")
                .having(condition -> condition.where("bucket", "=", input));
        SqlRequest request = planner(RdbDialect.postgresql()).plan(spec.build()).request();
        assertEquals(List.of("paid", input), request.parameters());
        assertFalse(request.sql().contains(input));
        spec.having(condition -> condition.where("hidden", "=", 1));
        assertThrows(IllegalArgumentException.class, () -> planner(RdbDialect.postgresql()).plan(spec.build()));
    }

    @Test
    void shorthandDoesNotBypassAliasUniqueness() {
        AggregateSpec.Builder spec = builder().group("status", "total").sum("amount", "total");
        assertThrows(IllegalArgumentException.class, spec::build);
    }

    private static AggregateSpec.Builder builder() {
        DynamicForm form = DynamicForm.builder("orders", "orders")
                .addField(DynamicField.of("status", "VARCHAR"))
                .addField(DynamicField.of("amount", "DECIMAL")).build();
        return AggregateSpec.builder(QuerySpec.of(form, ConditionGroup.and().where("status", "=", "paid").build()));
    }

    private static FormAggregatePlanner planner(RdbDialect dialect) {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), dialect);
        return new FormAggregatePlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(renderer.valueCodecs()), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults());
    }

    private static void sameRequest(SqlRequest expected, SqlRequest actual) {
        assertEquals(expected.sql(), actual.sql());
        assertEquals(expected.parameters(), actual.parameters());
    }
}
