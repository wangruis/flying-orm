package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;

import java.lang.reflect.RecordComponent;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class LegacyCursorCompatibilityTest {

    @Test
    void legacyCursorRecordsAndPlannerLayoutRemainUnchanged() {
        assertEquals(List.of("size", "sorts", "cursor"),
                     Arrays.stream(CursorPageQuery.class.getRecordComponents())
                           .map(RecordComponent::getName)
                           .toList());
        assertEquals(List.of(
                             "form", "request", "page", "options", "scope", "displayMode",
                             "containsQuery", "outputFields"),
                     Arrays.stream(FormOperationPlanner.PlannedCursorPage.class.getRecordComponents())
                           .map(RecordComponent::getName)
                           .toList());

        DynamicForm form = DynamicForm.builder("events", "events")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .build();
        FormOperationPlanner.PlannedCursorPage plan = planner().cursorPage(
                QuerySpec.of(form, ConditionGroup.and().build()),
                CursorPageQuery.first(20, CursorSort.asc("id")));

        assertFalse((Object) plan instanceof GovernedPlanEnvelope<?>);
        assertEquals(List.of("id"), plan.outputFields());
    }

    private static FormOperationPlanner planner() {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        return new FormOperationPlanner(
                renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.safeDefaults());
    }
}
