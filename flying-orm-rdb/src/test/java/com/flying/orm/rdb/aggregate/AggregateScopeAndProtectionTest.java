package com.flying.orm.rdb.aggregate;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AggregateScopeAndProtectionTest {

    @Test
    void scopeLogicDeleteProtectionAndShapeBudgetsFailBeforeExecution() {
        DynamicForm form = DynamicForm.builder("orders", "orders")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("tenant_id", "BIGINT"))
                .addField(DynamicField.of("deleted", "INTEGER"))
                .addField(DynamicField.of("status", "VARCHAR"))
                .addField(DynamicField.of("amount", "DECIMAL"))
                .tenant("tenant_id", TenantStrategy.AUTO)
                .logicDelete("deleted", 0, 1)
                .build();
        QuerySpec query = QuerySpec.of(
                form, ConditionGroup.and().where("amount", ">", BigDecimal.ZERO).build())
                .withScope(DataScope.tenant("tenant_id", 7L));
        AggregateSpec spec = AggregateSpec.builder(query)
                .group(GroupSelection.of("status", "status_group"))
                .aggregate(AggregateExpression.sum("amount", "gross"))
                .having(AggregateHaving.of(
                        ConditionGroup.and().where("gross", ">", BigDecimal.TEN).build()))
                .build();

        FormAggregatePlanner.Plan plan = planner(QueryShapeLimits.defaults()).plan(spec);

        assertTrue(plan.request().sql().contains("tenant_id"));
        assertTrue(plan.request().sql().contains("deleted"));
        assertEquals(List.of(BigDecimal.ZERO, 7L, 0, BigDecimal.TEN), plan.request().parameters());

        QueryShapeLimits oneAggregate = QueryShapeLimits.defaults().withMaxAggregateCount(1);
        AggregateSpec tooManyAggregates = AggregateSpec.builder(query)
                .aggregate(AggregateExpression.count("id", "total"))
                .aggregate(AggregateExpression.sum("amount", "gross"))
                .build();
        assertThrows(IllegalArgumentException.class,
                     () -> planner(oneAggregate).plan(tooManyAggregates));
        assertThrows(IllegalArgumentException.class,
                     () -> planner(QueryShapeLimits.defaults().withMaxBindCount(3)).plan(spec));

        DynamicForm protectedForm = DynamicForm.builder("secrets", "secrets")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .encrypted("secret", EncryptedFieldDefinition.builder().build())
                .build();
        AggregateSpec protectedAggregate = AggregateSpec.builder(QuerySpec.of(
                        protectedForm, ConditionGroup.and().build()))
                .aggregate(AggregateExpression.countDistinct("secret", "secret_count"))
                .build();
        assertThrows(IllegalArgumentException.class,
                     () -> planner(QueryShapeLimits.defaults()).plan(protectedAggregate));
    }

    private static FormAggregatePlanner planner(QueryShapeLimits limits) {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
        return new FormAggregatePlanner(
                renderer,
                StructuredConditionResolver.defaults(renderer.valueCodecs()),
                DataScope.none(),
                SqlExecutionOptions.safeDefaults(),
                FieldUsePolicy.unrestricted(),
                limits);
    }
}
