package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtectedTermGovernanceCompositionTest {

    @Test
    void unusedRelationRegistrationPreservesProtectedFilterApproval() {
        try (ProtectedFieldRuntime runtime = runtime()) {
            for (String operator : protectedOperators()) {
                var plan = planner(runtime, true).selectGoverned(
                        query(operator), policy(true), QueryShapeLimits.defaults());
                var baseline = planner(runtime, false).selectGoverned(
                        query(operator), policy(true), QueryShapeLimits.defaults());

                assertEquals(baseline.plan().request().sql(), plan.plan().request().sql());
                assertEquals(List.of("id"), plan.plan().outputFields());
                assertTrue(plan.fieldUse().decisions().stream().anyMatch(decision ->
                        decision.field().equals("secret") && decision.use() == FieldUse.FILTER && decision.allowed()));
                assertFalse(plan.plan().request().parameters().contains("123"));
            }
        }
    }

    @Test
    void unusedRelationRegistrationPreservesProtectedFilterDenial() {
        try (ProtectedFieldRuntime runtime = runtime()) {
            for (String operator : protectedOperators()) {
                assertThrows(ScopeAccessException.class, () -> planner(runtime, true).selectGoverned(
                        query(operator), policy(false), QueryShapeLimits.defaults()));
            }
        }
    }

    @Test
    void unknownOperatorsRemainRejected() {
        for (String operator : List.of("protected-unknown", "unknown")) {
            assertThrows(IllegalArgumentException.class, () -> GovernedTermGuard.require(
                    TermCondition.of("id", operator, 1L), FieldUse.FILTER,
                    conditions(true).terms(), RdbDialect.postgresql().capabilities()));
        }
    }

    @Test
    void registeredCustomHandlersStillRequireDescriptorsEvenWithProtectedNames() {
        for (String operator : protectedOperators()) {
            SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms()
                    .addTerm(SqlTermHandler.of(operator, (term, context) -> new SqlFragment("1 = 1", List.of())))
                    .build();
            assertThrows(IllegalArgumentException.class, () -> GovernedTermGuard.require(
                    TermCondition.of("id", operator, 1L), FieldUse.FILTER,
                    renderer.terms(), RdbDialect.postgresql().capabilities()));
        }
    }

    private static List<String> protectedOperators() {
        return List.of(ProtectedConditions.EXACT, ProtectedConditions.SUFFIX, ProtectedConditions.CONTAINS);
    }

    private static FormOperationPlanner planner(ProtectedFieldRuntime runtime, boolean relation) {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions(relation), RdbDialect.postgresql())
                .withProtectedFields(runtime);
        return new FormOperationPlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.safeDefaults());
    }

    private static SqlRenderer conditions(boolean relation) {
        SqlRenderer.Builder builder = SqlRenderer.builder().addDefaultTerms();
        if (relation) {
            builder.addTerm(SqlTermHandler.relationExists("has-role", "role_edge", "re", "member_id", "role_id"));
        }
        return builder.build();
    }

    private static QuerySpec query(String operator) {
        DynamicForm form = DynamicForm.builder("accounts", "accounts")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .encrypted("secret", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.EXACT, EncryptedSearchMode.SUFFIX, EncryptedSearchMode.CONTAINS)
                        .suffixLengths(3).build())
                .build();
        return QuerySpec.of(form, ConditionGroup.and().where("secret", operator, "123").build())
                .withProjection(List.of("id"), List.of());
    }

    private static FieldUsePolicy policy(boolean filter) {
        FieldUsePolicy.Builder builder = FieldUsePolicy.builder().visibility("id", FieldVisibility.FULL);
        if (filter) {
            builder.allow("secret", FieldUse.FILTER);
        }
        return builder.build();
    }

    private static ProtectedFieldRuntime runtime() {
        return ProtectedFieldRuntime.create(ProtectedFieldKeyRing.single("v1", new byte[32]));
    }
}
