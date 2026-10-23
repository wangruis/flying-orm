package com.flying.orm.rdb.form;

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
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ComputedJoinProjectionTest {
    @Test
    void sortOnlyJoinExpressionsBindAfterRelationsWithoutChangingOutput() {
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(FORM);
        JoinSource root = builder.root();
        JoinSource other = builder.join(JoinType.LEFT, FORM, root, "quantity", "id");
        ValueExpression order = ValueExpression.firstMatch(ValueExpression.field(other.field("quantity")).priority(5),
                ValueExpression.field(root.field("amount")).priority(10));
        builder.orderBy(order, PageSort.Direction.ASC);
        JoinQuerySpec spec = builder.selectAs(root, "id", "id")
                .where(root, ConditionGroup.and().where("id", ">", 1L).build()).build();
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormReadPlanner.PlannedQuery plan = planner(dialect).page(spec, PageQuery.of(1, 2), null);
            assertEquals(List.of(1L), plan.countRequest().parameters());
            assertEquals(List.of(1L, 5, 10), plan.request().parameters().subList(0, 3));
            assertEquals(List.of("id"), plan.form().fields().stream().map(DynamicField::name).toList());
            assertTrue(plan.request().sql().contains("order by case when"));
        }
        FieldUsePolicy.Builder allowed = FieldUsePolicy.builder().joinVisibility(root.field("id"), FieldVisibility.FULL)
                .allowJoin(root.field("id"), FieldUse.FILTER).allowJoin(root.field("quantity"), FieldUse.JOIN)
                .allowJoin(other.field("id"), FieldUse.JOIN).allowJoin(root.field("amount"), FieldUse.SORT);
        assertThrows(ScopeAccessException.class, () -> planner(RdbDialect.postgresql())
                .planGoverned(spec, null, allowed.build(), QueryShapeLimits.defaults()));
        allowed.allowJoin(other.field("quantity"), FieldUse.SORT);
        assertDoesNotThrow(() -> planner(RdbDialect.postgresql())
                .planGoverned(spec, null, allowed.build(), QueryShapeLimits.defaults()));
    }

    @Test
    void joinedPriorityRetainsSourceSpecificArrayAndJsonBindings() {
        DynamicForm typed = DynamicForm.builder("typed", "typed").addField(DynamicField.of("id", "BIGINT"))
                .addField(DynamicField.of("tags", "TEXT[]")).addField(DynamicField.of("document", "JSON")).build();
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(typed);
        JoinSource root = builder.root();
        ValueExpression same = ValueExpression.value(List.of("A"));
        ValueExpression order = ValueExpression.firstMatch(
                ValueExpression.call("priority", ValueExpression.field(root.field("tags")), same),
                ValueExpression.call("priority", ValueExpression.field(root.field("document")), same));
        JoinQuerySpec spec = builder.selectAs(root, "id", "id").orderBy(order, PageSort.Direction.ASC).build();
        List<Object> values = planner(RdbDialect.postgresql()).plan(spec, null).request().parameters();
        assertArrayEquals(new String[] { "A" }, (String[]) values.getFirst());
        assertEquals("[\"A\"]", values.get(1));
        assertThrows(IllegalArgumentException.class,
                () -> builder.orderBy(ValueExpression.field("tags"), PageSort.Direction.ASC));
        assertThrows(IllegalArgumentException.class,
                () -> builder.orderBy(ValueExpression.field(new JoinSource(3, typed).field("tags")), PageSort.Direction.ASC));
    }

    private static final DynamicForm FORM = DynamicForm.builder("sales", "sales")
            .addField(DynamicField.primaryKey("id", "BIGINT"))
            .addField(DynamicField.of("amount", "DECIMAL"))
            .addField(DynamicField.of("quantity", "INTEGER")).build();

    @Test
    void joinedCalculationPrecedesRelationBindingsAndDoesNotPolluteCount() {
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(FORM);
        JoinSource root = builder.root();
        JoinSource other = builder.join(JoinType.LEFT, FORM, root, "quantity", "id");
        JoinQuerySpec spec = builder.selectAs(root, "id", "id")
                .select(ValueExpression.field(root.field("amount")).multiply(
                        ValueExpression.field(other.field("quantity")).coalesce(ValueExpression.value(2))), "subtotal")
                .scope(other, DataScope.where(ConditionGroup.and().where("id", ">", 0L).build()))
                .where(root, ConditionGroup.and().where("id", ">", 1L).build())
                .orderBy(root, "id", PageSort.Direction.ASC).build();
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            JoinQueryPlanner planner = planner(dialect);
            FormReadPlanner.PlannedQuery plan = planner.plan(spec, null);
            assertEquals(List.of(2, 0L, 1L), plan.request().parameters());
            assertTrue(plan.request().sql().contains("coalesce("));
            assertNotNull(plan.form().field("subtotal"));
            FormReadPlanner.PlannedQuery page = planner.page(spec, PageQuery.of(2, 3), null);
            assertEquals(List.of(0L, 1L), page.countRequest().parameters());
            assertFalse(page.countRequest().sql().contains("coalesce("));
            assertEquals(List.of(2, 0L, 1L), page.request().parameters().subList(0, 3));
        }
        assertThrows(ScopeAccessException.class, () -> planner(RdbDialect.postgresql())
                .planGoverned(spec, null, FieldUsePolicy.builder().build(), QueryShapeLimits.defaults()));
    }

    @Test
    void expressionSourcesAndAliasesRemainBoundToTheQuery() {
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(FORM);
        JoinSource foreign = new JoinSource(2, FORM);
        assertThrows(IllegalArgumentException.class,
                () -> builder.select(ValueExpression.field(foreign.field("amount")), "subtotal"));
        assertThrows(IllegalArgumentException.class,
                () -> builder.select(ValueExpression.field("amount"), "subtotal"));
        builder.select(ValueExpression.value(1), "subtotal");
        assertThrows(IllegalArgumentException.class,
                () -> builder.selectAs(builder.root(), "id", "SUBTOTAL"));
        assertEquals(1, builder.build().projections().size());
    }

    @Test
    void computationRequiresFullSourceProjectionAndDoesNotBypassDeclaredMasking() {
        DynamicForm masked = DynamicForm.builder("masked_sales", "sales").addField(DynamicField.of("amount", "VARCHAR"))
                .masked("amount", MaskedFieldDefinition.builder("full").build()).build();
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(masked);
        JoinSource root = builder.root();
        builder.select(ValueExpression.field(root.field("amount")).coalesce(ValueExpression.value("fallback")), "subtotal");
        JoinQueryPlanner planner = planner(RdbDialect.postgresql());
        assertThrows(IllegalArgumentException.class, () -> planner.plan(builder.build(), null));
        JoinQuerySpec visible = builder.showSensitive().build();
        assertDoesNotThrow(() -> planner.plan(visible, null));
        FieldUsePolicy fullOnly = FieldUsePolicy.builder().allowJoin(root.field("amount"), FieldUse.FULL_VALUE).build();
        assertThrows(ScopeAccessException.class,
                () -> planner.planGoverned(visible, null, fullOnly, QueryShapeLimits.defaults()));
        FieldUsePolicy allowed = FieldUsePolicy.builder().joinVisibility(root.field("amount"), FieldVisibility.FULL).build();
        assertDoesNotThrow(() -> planner.planGoverned(visible, null, allowed, QueryShapeLimits.defaults()));
    }

    @Test
    void joinedGroupsUseOneSourceSetForProjectionOrderingAndPageCount() {
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(FORM);
        JoinSource root = builder.root();
        JoinSource other = builder.join(JoinType.LEFT, FORM, root, "quantity", "id");
        JoinQuerySpec query = builder.selectAs(other, "id", "related_id")
                .select(ValueExpression.field(other.field("id")).add(ValueExpression.value(2L)), "offset_id")
                .where(root, ConditionGroup.and().where("id", ">", 3L).build())
                .groupBy(other, "id")
                .orderBy(other, "id", PageSort.Direction.ASC).build();
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormReadPlanner.PlannedQuery page = planner(dialect).page(query, PageQuery.of(2, 3), null);
            assertTrue(page.request().sql().contains("group by"));
            assertEquals(List.of(2L, 3L), page.request().parameters().subList(0, 2));
            assertTrue(page.countRequest().sql().contains("from (select 1 as grouped_value from"),
                    "portable derived tables name their columns, as required by SQL Server");
            assertTrue(page.countRequest().sql().contains("group by"));
            assertFalse(page.countRequest().sql().contains("order by"));
            assertEquals(List.of(3L), page.countRequest().parameters());
        }
    }

    private static JoinQueryPlanner planner(RdbDialect dialect) {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), dialect);
        return new JoinQueryPlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited());
    }
}
