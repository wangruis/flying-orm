package com.flying.orm.rdb.aggregate;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AggregateHavingFieldUseTest {

    @Test
    void havingUsesOnlyDeclaredAliasesAndRequiresHavingPermissionOnTheSourceField() {
        DynamicForm form = DynamicForm.builder("orders", "orders")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("status", "VARCHAR"))
                .addField(DynamicField.of("amount", "DECIMAL"))
                .build();
        QuerySpec query = QuerySpec.of(
                form, ConditionGroup.and().where("status", "!=", "cancelled").build());
        AggregateSpec spec = AggregateSpec.builder(query)
                .group(GroupSelection.of("status", "status_group"))
                .aggregate(AggregateExpression.sum("amount", "gross"))
                .having(AggregateHaving.of(
                        ConditionGroup.and().where("gross", ">", BigDecimal.TEN).build()))
                .build();

        FieldUsePolicy allowed = FieldUsePolicy.builder()
                .allow("status", FieldUse.FILTER, FieldUse.GROUP, FieldUse.PROJECT)
                .allow("amount", FieldUse.AGGREGATE, FieldUse.HAVING)
                .build();
        FormAggregatePlanner.Plan plan = planner(allowed).plan(spec);

        assertTrue(plan.request().sql().contains("having sum(amount) > ?"));

        FieldUsePolicy missingHaving = FieldUsePolicy.builder()
                .allow("status", FieldUse.FILTER, FieldUse.GROUP, FieldUse.PROJECT)
                .allow("amount", FieldUse.AGGREGATE)
                .build();
        assertThrows(ScopeAccessException.class, () -> planner(missingHaving).plan(spec));

        SqlRenderer customTerms = SqlRenderer.builder()
                .addDefaultTerms()
                .addTerm(SqlTermHandler.of(
                        "trusted-only",
                        (term, context) -> new SqlFragment(
                                context.identifier(term.field()) + " > ?",
                                List.of(context.parameter(term.value())))))
                .build();
        AggregateSpec extensionHaving = AggregateSpec.builder(query)
                .aggregate(AggregateExpression.sum("amount", "gross"))
                .having(AggregateHaving.of(
                        ConditionGroup.and().where(
                                "gross", "trusted-only", BigDecimal.TEN).build()))
                .build();
        assertThrows(IllegalArgumentException.class,
                     () -> planner(allowed, customTerms).plan(extensionHaving));
        assertDoesNotThrow(
                () -> planner(FieldUsePolicy.unrestricted(), customTerms).plan(extensionHaving));

        AggregateSpec unknownAlias = AggregateSpec.builder(query)
                .aggregate(AggregateExpression.count("id", "total"))
                .having(AggregateHaving.of(
                        ConditionGroup.and().where("missing", ">", 0).build()))
                .build();
        assertThrows(IllegalArgumentException.class,
                     () -> planner(FieldUsePolicy.unrestricted()).plan(unknownAlias));
    }

    private static FormAggregatePlanner planner(FieldUsePolicy policy) {
        return planner(policy, SqlRenderer.builder().addDefaultTerms().build());
    }

    private static FormAggregatePlanner planner(FieldUsePolicy policy, SqlRenderer conditions) {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, RdbDialect.h2());
        return new FormAggregatePlanner(
                renderer,
                StructuredConditionResolver.defaults(renderer.valueCodecs()),
                DataScope.none(),
                SqlExecutionOptions.safeDefaults(),
                policy,
                QueryShapeLimits.defaults());
    }
}
