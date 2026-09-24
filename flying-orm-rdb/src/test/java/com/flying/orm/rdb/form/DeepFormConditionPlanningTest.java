package com.flying.orm.rdb.form;

import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.condition.TermRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateHaving;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.aggregate.FormAggregatePlanner;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DeepFormConditionPlanningTest {

    private static final int DEPTH = 10_000;
    private final DynamicForm form = DynamicForm.builder("deep_forms", "deep_forms")
            .addField(DynamicField.primaryKey("id", "INTEGER"))
            .addField(DynamicField.of("label", "VARCHAR")).build();

    @Test
    void ordinaryFormQueryPlansTenThousandNestedGroups() {
        FormOperationPlanner.PlannedQuery plan = planner(renderer()).select(
                QuerySpec.of(form, deep(ConditionGroup.and().where("id", "=", 7).build())));

        assertEquals(List.of(7), plan.request().parameters());
        assertTrue(plan.request().sql().contains("where"));
    }

    @Test
    void governedQueryStillCollectsDeepCallerFields() {
        QuerySpec query = QuerySpec.of(form, deep(ConditionGroup.and().where("id", "=", 7).build()));
        GovernedPlanEnvelope<FormOperationPlanner.PlannedQuery> plan = planner(renderer()).selectGoverned(
                query, FieldUsePolicy.unrestricted(), QueryShapeLimits.defaults().withMaxBindCount(4));

        assertEquals(List.of(7), plan.plan().request().parameters());
    }

    @Test
    void deepServerScopeIsPreservedAfterBusinessParameters() {
        ConditionGroup scope = deep(ConditionGroup.and().where("id", "=", 7).build());
        QuerySpec query = QuerySpec.of(form, ConditionGroup.and().where("label", "=", "visible").build())
                .withScope(DataScope.where(scope));

        assertEquals(List.of("visible", 7), planner(renderer()).select(query).request().parameters());
    }

    @Test
    void writeGuardAcceptsADeepBusinessPredicate() {
        ConditionGroup where = deep(ConditionGroup.and().where("id", "=", 7).build());
        FormOperationPlanner.PlannedWrite plan = planner(renderer()).update(
                WriteSpec.update(form, Map.of("label", "updated"), where));

        assertEquals(List.of("updated", 7), plan.request().parameters());
    }

    @Test
    void writeGuardRejectsDeepEmptyGroupsEvenWithANonemptyServerScope() {
        WriteSpec write = WriteSpec.delete(form, deep(ConditionGroup.and().build()))
                .withScope(DataScope.where(ConditionGroup.and().where("id", "=", 7).build()));

        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> planner(renderer()).delete(write));
        assertEquals("write business where condition must not be empty", failure.getMessage());
    }

    @Test
    void deepProtectedSearchIsRewrittenToABoundToken() {
        DynamicForm protectedForm = protectedForm();
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            FormDataSqlRenderer renderer = renderer().withProtectedFields(runtime);
            ConditionGroup where = deep(ConditionGroup.and().add(ProtectedConditions.exact("secret", "value")).build());
            FormOperationPlanner.PlannedQuery plan = planner(renderer).select(QuerySpec.of(protectedForm, where));

            assertEquals(1, plan.request().parameters().size());
            assertInstanceOf(byte[].class, plan.request().parameters().getFirst());
            assertFalse(plan.request().sql().contains("value"));
        }
    }

    @Test
    void reactiveProtectionDetectionFindsDeepEncryptedLeaves() {
        ConditionGroup where = deep(ConditionGroup.and().add(ProtectedConditions.exact("secret", "value")).build());

        assertTrue(ReactiveProtectionCpuBoundary.usesEncryptedCondition(protectedForm(), where));
        assertFalse(ReactiveProtectionCpuBoundary.usesEncryptedCondition(protectedForm(),
                deep(ConditionGroup.and().where("id", "=", 7).build())));
    }

    @Test
    void entityEnumEncodingRewritesDeepLeavesAndReusesUnchangedTrees() {
        EntityValues<EnumRow> values = EntityValues.createUncached(EnumRow.class);
        ConditionGroup where = deep(ConditionGroup.and().where("status", "=", Status.ACTIVE).build());
        ConditionGroup normalized = values.normalizeCondition(where, TermRegistry.standard());

        assertEquals(List.of("A"), SqlRenderer.builder().addDefaultTerms().build()
                .renderWhere(normalized).parameters());
        ConditionGroup unchanged = deep(ConditionGroup.and().where("status", "=", "A").build());
        assertSame(unchanged, values.normalizeCondition(unchanged, TermRegistry.standard()));
    }

    @Test
    void aggregateHavingCountsAndRendersDeepGroups() {
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(form, ConditionGroup.and().build()))
                .aggregate(AggregateExpression.count("id", "total"))
                .having(AggregateHaving.of(deep(ConditionGroup.and().where("total", ">", 1L).build())))
                .build();
        FormDataSqlRenderer renderer = renderer();
        FormAggregatePlanner planner = new FormAggregatePlanner(renderer, StructuredConditionResolver.defaults(),
                DataScope.none(), SqlExecutionOptions.safeDefaults(), FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults());
        FormAggregatePlanner.Plan plan = planner.plan(spec);

        assertEquals(List.of(1L), plan.request().parameters());
        assertTrue(plan.request().sql().contains("having"));
    }

    @Test
    void ordinaryAggregateCollectsDeepStructuredFilterFields() {
        StructuredConditionInput input = StructuredConditionInput.term("id", "eq", 7);
        for (int depth = 1; depth < DEPTH; depth++) {
            input = StructuredConditionInput.and(input);
        }
        QuerySpec query = QuerySpec.structured(form, input).withStructuredPolicy(
                StructuredConditionPolicy.defaults().withMaxDepth(DEPTH).withMaxNodes(DEPTH));
        AggregateSpec spec = AggregateSpec.builder(query)
                .aggregate(AggregateExpression.count("id", "total")).build();
        FormAggregatePlanner planner = new FormAggregatePlanner(renderer(), StructuredConditionResolver.defaults(),
                DataScope.none(), SqlExecutionOptions.safeDefaults(), FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults());

        FormAggregatePlanner.Plan plan = planner.plan(spec);

        assertEquals(List.of(7), plan.request().parameters());
        assertTrue(plan.request().sql().contains("where"));
    }

    @Test
    void joinGuardValidatesAndPlansDeepSourceConditions() {
        JoinQuerySpec.Builder builder = JoinQuerySpec.builder(form);
        JoinSource joined = builder.join(JoinType.INNER, form, builder.root(), "id", "id");
        JoinQuerySpec spec = builder.select(builder.root(), "id")
                .where(joined, deep(ConditionGroup.and().where("id", "=", 7).build())).build();
        FormDataSqlRenderer renderer = renderer();
        JoinQueryPlanner planner = new JoinQueryPlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.safeDefaults());

        assertEquals(List.of(7), planner.plan(spec, null).request().parameters());
    }

    private static DynamicForm protectedForm() {
        return DynamicForm.builder("deep_secrets", "deep_secrets")
                .addField(DynamicField.primaryKey("id", "INTEGER"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .encrypted("secret", EncryptedFieldDefinition.builder().build()).build();
    }

    private static ConditionGroup deep(ConditionGroup leaf) {
        ConditionGroup result = leaf;
        for (int level = 1; level < DEPTH; level++) {
            result = (level % 2 == 0 ? ConditionGroup.and() : ConditionGroup.or()).add(result).build();
        }
        return result;
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
    }

    private static FormOperationPlanner planner(FormDataSqlRenderer renderer) {
        return new FormOperationPlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.safeDefaults());
    }

    @TableName("deep_enum_rows")
    private record EnumRow(Status status) { }

    private enum Status {
        ACTIVE("A");
        @EnumValue private final String code;
        Status(String code) { this.code = code; }
    }
}
