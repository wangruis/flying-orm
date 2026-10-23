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
import static org.junit.jupiter.api.Assertions.assertTrue;

class LegacyPlanCacheCompatibilityTest {

    @Test
    void equivalentUnlimitedLimitsDoNotEnableGovernance() {
        QueryShapeLimits limits = QueryShapeLimits.defaults().withMaxBindCount(1)
                .withMaxBindCount(Integer.MAX_VALUE);
        assertFalse(FieldUseGuard.governed(FieldUsePolicy.unrestricted(), limits));
        assertTrue(FieldUseGuard.governed(FieldUsePolicy.unrestricted(), limits.withMaxBindCount(1)));
        assertTrue(FieldUseGuard.governed(FieldUsePolicy.builder().build(), limits));
    }

    @Test
    void unrestrictedDefaultsKeepTheLegacyPlanTypeAndSharedSingletons() {
        FormOperationPlanner planner = planner();
        DynamicForm form = DynamicForm.builder("accounts", "accounts")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .build();

        FormReadPlanner.PlannedQuery plan = planner.reads.select(
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
                SqlExecutionOptions.unlimited());
    }
}
