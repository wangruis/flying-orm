package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

class LegacyPlanCacheCompatibilityTest {

    @Test
    void unrestrictedDefaultsKeepTheLegacyPlanTypeAndSharedSingletons() {
        FormOperationPlanner planner = planner();
        DynamicForm form = DynamicForm.builder("accounts", "accounts")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .build();

        FormOperationPlanner.PlannedQuery plan = planner.select(
                QuerySpec.of(form, ConditionGroup.and().build()));

        assertFalse((Object) plan instanceof GovernedPlanEnvelope<?>);
        assertFalse(FieldUseGuard.governed(FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults()));
        assertSame(FieldUsePolicy.unrestricted(), FieldUsePolicy.unrestricted());
        assertSame(QueryShapeLimits.defaults(), QueryShapeLimits.defaults());
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
