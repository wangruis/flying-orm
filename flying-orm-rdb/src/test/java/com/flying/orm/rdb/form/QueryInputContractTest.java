package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.QueryInput;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class QueryInputContractTest {
    private static final DynamicForm FORM = DynamicForm.builder("users", "users")
            .addField(DynamicField.primaryKey("id", "BIGINT"))
            .addField(DynamicField.of("name", "VARCHAR")).build();

    @Test
    void inputFreezesShapeWithoutReplacingServerConditionsOrScope() {
        List<String> fields = new ArrayList<>(List.of("id", "name"));
        QueryInput input = new QueryInput(StructuredConditionInput.term("name", "eq", "Ada"),
                fields, List.of("name"), List.of(PageSort.desc("id")), PageQuery.of(2, 3));
        fields.clear();
        ConditionGroup where = ConditionGroup.and().where("id", ">", 0L).build();
        DataScope scope = DataScope.where(ConditionGroup.and().where("id", "<", 100L).build());
        QuerySpec.Builder builder = QuerySpec.builder(FORM).where(where).scope(scope).input(input);
        QuerySpec frozen = builder.build();
        QuerySpec.Builder branch = builder.copy();
        builder.input(new QueryInput(null, null, null, null, null));
        assertEquals(List.of("id"), frozen.projections());
        assertSame(where, frozen.where());
        assertSame(scope, frozen.scope());
        assertEquals(PageQuery.of(2, 3), frozen.page().orElseThrow());
        assertEquals(frozen.page(), branch.build().page());
        assertTrue(builder.build().page().isEmpty());
        assertTrue(builder.build().structuredInput().isEmpty());
        assertSame(where, builder.build().where());
        assertTrue(branch.buildCount().page().isEmpty());
        assertTrue(branch.buildConditions().page().isEmpty());
        assertEquals(frozen.page(), frozen.withScope(DataScope.none()).withWhere(where)
                .withSorts(List.of()).withExcludedFields(List.of()).page());
    }

    @Test
    void dataReadsApplyTheInputWindowWithoutGeneratingACount() {
        QuerySpec spec = QuerySpec.builder(FORM).input(new QueryInput(
                StructuredConditionInput.term("name", "eq", "Ada"), List.of("id"), null,
                List.of(PageSort.desc("id")), PageQuery.of(2, 3))).build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        FormReadPlanner planner = new FormReadPlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited());
        FormReadPlanner.PlannedQuery selected = planner.select(spec);
        assertTrue(selected.request().sql().endsWith("order by \"id\" desc limit ? offset ?"));
        assertEquals(List.of("Ada", 3, 3L), selected.request().parameters());
        assertNull(selected.countRequest());
        assertEquals(List.of("Ada", 1, 0L), planner.first(spec).request().parameters());
        assertEquals(List.of("Ada"), planner.count(spec, null, null, false).request().parameters());
        assertEquals(List.of("Ada", 7, 0L), planner.page(spec, PageQuery.of(1, 7)).request().parameters());
    }
}
