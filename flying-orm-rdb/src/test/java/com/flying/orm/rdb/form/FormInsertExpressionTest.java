package com.flying.orm.rdb.form;

import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.binding.SqlNullParameter;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FormInsertExpressionTest {
    private static final DynamicForm FORM = DynamicForm.builder("items", "items")
            .addField(DynamicField.primaryKey("id", "BIGINT"))
            .addField(DynamicField.of("quantity", "INTEGER"))
            .addField(DynamicField.of("note", "VARCHAR")).build();
    private final FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
            SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());

    @Test
    void insertSharesExpressionCompilerAndKeepsValuesBound() {
        SqlRequest request = renderer.insert(FORM, row(1L, sum(2, 3), "x'); delete from items; --"));
        assertTrue(request.sql().contains("(?, (? + ?), ?)"), request.sql());
        assertEquals(List.of(1L, 2, 3, "x'); delete from items; --"), request.parameters());
    }

    @Test
    void boundNullUsesTheTargetFieldType() {
        SqlRequest request = renderer.insert(FORM, row(1L, ValueExpression.value(null), "note"));
        assertEquals(new SqlNullParameter(Integer.class), request.parameters().get(1));
    }

    @Test
    void batchBindsReorderedRowsAndVariableParameterCounts() {
        BatchInsertPlan plan = renderer.batchRenderer.insertPlan(FORM, row(1L, sum(2, 3), "first"));
        assertArrayEquals(new Object[]{1L, 2, 3, "first"}, plan.firstParameters());
        Map<String, Object> reordered = new LinkedHashMap<>();
        reordered.put("note", "second");
        reordered.put("quantity", sum(4, 5));
        reordered.put("id", 2L);
        assertArrayEquals(new Object[]{2L, 4, 5, "second"}, plan.parameters(reordered, 1));
        assertEquals(List.of(Long.class, Integer.class, Integer.class, String.class), plan.parameterTypes());
    }

    @Test
    void upsertUsesTheSameValueExpressions() {
        BatchInsertPlan plan = renderer.batchRenderer.upsertPlan(FORM, row(1L, sum(2, 3), "first"));
        assertTrue(plan.sql().contains("(? + ?)"), plan.sql());
        assertArrayEquals(new Object[]{1L, 2, 3, "first"}, plan.firstParameters());
        assertArrayEquals(new Object[]{2L, 4, 5, "second"}, plan.parameters(row(2L, sum(4, 5), "second"), 1));
    }

    @Test
    void insertCannotReadAnUnboundTableRow() {
        assertThrows(IllegalArgumentException.class,
                () -> renderer.insert(FORM, row(1L, ValueExpression.field("quantity").add(ValueExpression.value(1)), "x")));
    }

    @Test
    void zeroParameterExpressionsAndOrdinaryRowsHaveDifferentCachedPlans() {
        SqlRequest literal = renderer.insert(FORM, row(1L, new ValueExpression.IntegerConstant(7), "first"));
        SqlRequest ordinary = renderer.insert(FORM, row(2L, 8, "second"));
        assertEquals(List.of(1L, "first"), literal.parameters());
        assertTrue(literal.sql().contains("(?, 7, ?)"), literal.sql());
        assertEquals(List.of(2L, 8, "second"), ordinary.parameters());
        assertTrue(ordinary.sql().contains("(?, ?, ?)"), ordinary.sql());
    }

    @Test
    void scopeFollowsExpandedExpressionParametersForEveryDialect() {
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(), RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormDataSqlRenderer sql = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), dialect);
            Map<String, Object> first = row(1L, sum(2, 3), "first");
            BatchInsertPlan plan = sql.batchRenderer.upsertPlan(FORM, first, FORM, first, first,
                    com.flying.orm.core.condition.ConditionGroup.and().where("id", "=", 9L).build(), List.of(), null);
            assertArrayEquals(new Object[]{1L, 2, 3, "first", 9L}, plan.firstParameters(), dialect.name());
            assertArrayEquals(new Object[]{2L, 4, 5, "second", 9L},
                    plan.parameters(row(2L, sum(4, 5), "second"), 1), dialect.name());
        }
    }

    @Test
    void registeredFunctionsKeepTheirArgumentsBound() {
        FormDataSqlRenderer sql = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms()
                .expressionFunctions(com.flying.orm.core.sql.expression.SqlExpressionFunctions.builder()
                        .register("lower", "lower", com.flying.orm.core.type.LogicalType.TEXT,
                                com.flying.orm.core.type.LogicalType.TEXT).build()).build(), RdbDialect.postgresql());
        SqlRequest request = sql.insert(FORM, row(1L, 3, ValueExpression.call("lower", ValueExpression.value("SECRET"))));
        assertTrue(request.sql().contains("lower(?)"), request.sql());
        assertEquals(List.of(1L, 3, "SECRET"), request.parameters());
    }

    @Test
    void publicInsertKeepsArrayExpressionsUntilFieldEncoding() {
        DynamicForm form = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("tags", "VARCHAR[]")).build();
        ValueExpression expression = ValueExpression.value(new String[]{"a", "b"});
        com.flying.orm.rdb.form.spec.WriteSpec spec = com.flying.orm.rdb.form.spec.WriteSpec.insert(
                form, Map.of("tags", expression));
        FormOperationPlanner planner = new FormOperationPlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), com.flying.orm.core.scope.DataScope.none()),
                com.flying.orm.rdb.execution.SqlExecutionOptions.unlimited());
        Object value = planner.insert(spec).request().parameters().getFirst();
        assertArrayEquals(new String[]{"a", "b"}, assertInstanceOf(String[].class, value));
    }

    private static ValueExpression sum(int left, int right) {
        return ValueExpression.value(left).add(ValueExpression.value(right));
    }

    private static Map<String, Object> row(long id, Object quantity, Object note) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", id);
        row.put("quantity", quantity);
        row.put("note", note);
        return row;
    }
}
