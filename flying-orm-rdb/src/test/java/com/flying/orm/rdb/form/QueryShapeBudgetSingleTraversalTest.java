package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUseOrigin;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QueryShapeBudgetSingleTraversalTest {

    @Test
    void theSameGovernancePassDeduplicatesFieldUseAndCountsEveryBind() {
        DynamicForm form = DynamicForm.builder("accounts", "accounts")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("state", "VARCHAR"))
                                      .build();
        QuerySpec spec = QuerySpec.of(form, ConditionGroup.and()
                                                         .where("state", "=", "OPEN")
                                                         .where("state", "!=", "CLOSED")
                                                         .build())
                                  .withProjection(List.of("id"), List.of());
        FieldUsePolicy policy = FieldUsePolicy.builder()
                                              .visibility("id", FieldVisibility.FULL)
                                              .allow("state", FieldUse.FILTER)
                                              .build();
        QueryShapeLimits limits = QueryShapeLimits.defaults()
                                                 .withMaxProjectionCount(1)
                                                 .withMaxBindCount(2);

        GovernedPlanEnvelope<FormOperationPlanner.PlannedQuery> envelope =
                planner().selectGoverned(spec, policy, limits);

        assertTrue(envelope.fieldUse().allowed());
        assertEquals(1, envelope.fieldUse().decisions().stream()
                                .filter(decision -> decision.field().equals("state")
                                        && decision.use() == FieldUse.FILTER
                                        && decision.origin() == FieldUseOrigin.CALLER)
                                .count());
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
