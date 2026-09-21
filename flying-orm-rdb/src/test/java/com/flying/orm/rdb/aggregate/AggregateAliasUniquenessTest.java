package com.flying.orm.rdb.aggregate;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AggregateAliasUniquenessTest {

    @Test
    void relationHavingQualifiesGroupAndAggregateSources() {
        DynamicForm form = DynamicForm.builder("orders", "orders")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms()
                .addTerm(com.flying.orm.core.sql.render.SqlTermHandler.relationExists(
                        "member-of", "membership", "orders", "owner_id", "group_id")).build();
        var having = ConditionGroup.and(conditions.terms())
                .where("group_id", "member-of", 1L).where("maximum", "member-of", 2L).build();
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(form, ConditionGroup.and().build()))
                .group(GroupSelection.of("id", "group_id"))
                .aggregate(AggregateExpression.max(
                        "id", "maximum", com.flying.orm.core.type.LogicalType.BIG_INTEGER, Long.class))
                .having(AggregateHaving.of(having)).build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, RdbDialect.postgresql());
        String sql = new FormAggregatePlanner(renderer, StructuredConditionResolver.defaults(), DataScope.none(),
                SqlExecutionOptions.safeDefaults(), FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults())
                .plan(spec).request().sql();
        assertTrue(sql.contains("\"orders_relation\".\"owner_id\" = \"orders\".\"id\""), sql);
        assertTrue(sql.contains("\"orders_relation\".\"owner_id\" = max(\"orders\".\"id\")"), sql);
    }

    @Test
    void orderingUsesTheDeclaredAliasAfterCaseInsensitiveResolution() {
        DynamicForm form = DynamicForm.builder("orders", "orders")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .build();
        QuerySpec query = QuerySpec.of(form, ConditionGroup.and().build())
                .withSorts(List.of(PageSort.asc("total")));
        AggregateSpec spec = AggregateSpec.builder(query)
                .aggregate(AggregateExpression.count("id", "Total")).build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        FormAggregatePlanner planner = new FormAggregatePlanner(
                renderer, StructuredConditionResolver.defaults(), DataScope.none(),
                SqlExecutionOptions.safeDefaults(), FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults());

        String sql = planner.plan(spec).request().sql();

        assertTrue(sql.endsWith(" order by \"Total\" asc"), sql);
    }

    @Test
    void aliasesAreUniqueAcrossGroupsAndAggregateExpressions() {
        QuerySpec query = QuerySpec.of(
                DynamicForm.builder("orders", "orders")
                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                        .addField(DynamicField.of("status", "VARCHAR"))
                        .build(),
                ConditionGroup.and().build());

        assertThrows(IllegalArgumentException.class,
                     () -> AggregateSpec.builder(query)
                             .group(GroupSelection.of("status", "result"))
                             .aggregate(AggregateExpression.count("id", "RESULT"))
                             .build());

        AggregateSpec spec = AggregateSpec.builder(query)
                .group(GroupSelection.of("status", "status_group"))
                .aggregate(AggregateExpression.count("id", "total"))
                .build();
        assertEquals(List.of("status_group"),
                     spec.groups().stream().map(GroupSelection::alias).toList());
        assertEquals(List.of("total"),
                     spec.aggregates().stream().map(AggregateExpression::alias).toList());
    }
}
