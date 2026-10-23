package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.TermExtensionDescriptor;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.QueryRelation;
import com.flying.orm.core.metadata.Feature;
import com.flying.orm.core.metadata.FeatureRegistry;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.core.sql.render.CorrelatedSqlTermHandler;
import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class FieldAwareTermContextTest {
    record ValuePrefix(String id, String prefix) implements Feature { }

    @Test
    void fieldAndTableTermOverridesAreUsedInsteadOfCachedStandardSql() {
        SqlTermHandler length = SqlTermHandler.of("=",
                (term, context) -> new SqlFragment("length(" + context.identifier(term.field()) + ") = ?",
                        List.of(context.parameter(term.value()))));
        SqlTermHandler lower = SqlTermHandler.of("=",
                (term, context) -> new SqlFragment("lower(" + context.identifier(term.field()) + ") = ?",
                        List.of(context.parameter(term.value()))));
        DynamicField name = DynamicField.of("name", "VARCHAR");
        DynamicForm ordinary = DynamicForm.builder("ordinary", "items").addField(name).build();
        DynamicForm table = DynamicForm.builder("table", "items")
                .features(FeatureRegistry.builder().add(lower).build()).addField(name).build();
        DynamicForm column = DynamicForm.builder("column", "items").features(table.features())
                .addField(name.withFeatures(FeatureRegistry.builder().add(length).build())).build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        ConditionGroup where = ConditionGroup.and().where("name", "=", 2).build();
        assertFalse(renderer.select(ordinary, where).sql().contains("length"));
        assertTrue(renderer.select(column, where).sql().contains("length(\"name\") = ?"));
        assertTrue(renderer.select(table, where).sql().contains("lower(\"name\") = ?"));
        assertFalse(renderer.select(ordinary, where).sql().contains("lower"));
        assertTrue(renderer.update(column, Map.of("name", "ok"), where).sql().contains("length(\"name\") = ?"));
        assertTrue(renderer.delete(table, where).sql().contains("lower(\"name\") = ?"));
        ConditionGroup scoped = FormDataScopes.apply(column, where,
                DataScope.where(ConditionGroup.and().where("tenant_id", "=", 7L).build()));
        SqlRequest scopedQuery = renderer.select(column, scoped);
        assertTrue(scopedQuery.sql().contains("\"tenant_id\" = ?"), scopedQuery.sql());
        assertEquals(List.of(2, 7L), scopedQuery.parameters());
    }

    @Test
    void inheritedCorrelatedOverrideUsesTheAggregateResultAsItsOuterScope() {
        CorrelatedSqlTermHandler correlated = new CorrelatedSqlTermHandler() {
            @Override public String id() { return "="; }
            @Override public SqlFragment render(com.flying.orm.core.condition.TermCondition term,
                    com.flying.orm.core.sql.render.SqlRenderContext context,
                    java.util.function.UnaryOperator<String> outerFields,
                    java.util.function.UnaryOperator<String> outerQualifiers) {
                return new SqlFragment("exists(select 1 where " + outerFields.apply(term.field()) + " = ?)",
                        List.of(context.parameter(term.value())));
            }
        };
        DynamicForm form = DynamicForm.builder("items", "items")
                .features(FeatureRegistry.builder().add(correlated).build())
                .addField(DynamicField.of("code", "VARCHAR")).build();
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(form, ConditionGroup.and().build()))
                .aggregate(AggregateExpression.countRows("total"))
                .having(ConditionGroup.and().where("total", "=", 2L).build()).build();
        FormAggregatePlanner planner = new FormAggregatePlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults());
        SqlRequest request = planner.plan(spec).request();
        assertTrue(request.sql().contains("\"flying_aggregate_result\".\"total\" = ?"), request.sql());
        assertEquals(List.of(2L), request.parameters());
    }

    @Test
    void customConditionsConsumeInheritedFeaturesWithoutCrossModelCachePollution() {
        FeatureRegistry defaults = FeatureRegistry.builder().add(new ValuePrefix("prefix", "schema")).build();
        FeatureRegistry table = FeatureRegistry.builder().parent(defaults)
                .add(new ValuePrefix("prefix", "table")).build();
        DynamicField field = DynamicField.of("name", "VARCHAR").withFeatures(FeatureRegistry.builder()
                .add(new ValuePrefix("prefix", "column")).build());
        DynamicForm first = DynamicForm.builder("first", "items").features(table)
                .addField(field).addField(DynamicField.of("other", "VARCHAR"))
                .addField(DynamicField.of("secret", "VARCHAR")).build();
        DynamicForm second = DynamicForm.builder("second", "items").features(defaults)
                .addField(first.field("other")).build();
        SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms().addTerm(SqlTermHandler.of(
                TermExtensionDescriptor.filter("prefixed", List.of(), 1, 1), ConditionValueShape.SCALAR,
                (term, context) -> new SqlFragment(context.identifier(term.field()) + " = ?", List.of(
                        context.field(term.field()).features().feature(ValuePrefix.class).prefix() + ":" + term.value()))))
                .build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, RdbDialect.postgresql());
        assertEquals(List.of("column:x"), renderer.select(first,
                conditions.conditions().where("name", "prefixed", "x").build()).parameters());
        ConditionGroup where = conditions.conditions().where("other", "prefixed", "x").build();
        assertEquals(List.of("table:x"), renderer.select(first, where).parameters());
        assertEquals(List.of("schema:x"), renderer.select(second, where).parameters());
        assertEquals(List.of("table:x"), renderer.delete(first, where).parameters());
        DynamicForm.Builder protectedModel = DynamicForm.builder("protected", "items").features(table);
        first.fields().forEach(protectedModel::addField);
        DynamicForm physical = com.flying.orm.rdb.protection.ProtectedFormLayout.physical(
                protectedModel.encrypted("secret", EncryptedFieldDefinition.builder().build()).build());
        assertEquals(List.of("table:x"), renderer.select(physical, where).parameters());
        assertEquals(List.of("column:x"), renderer.select(physical,
                conditions.conditions().where("name", "prefixed", "x").build()).parameters());
    }

    private final List<DynamicField> resolved = new ArrayList<>();
    private final SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms().addTerm(
            SqlTermHandler.of(TermExtensionDescriptor.filter("declared-value", List.of(), 1, 1),
                    ConditionValueShape.SCALAR, (term, context) -> {
                        DynamicField field = context.field(term.field());
                        resolved.add(field);
                        String column = context.identifier(term.field());
                        String expression = field.databaseType().isNumeric() ? column : "lower(" + column + ")";
                        return new SqlFragment(expression + " = ?", List.of(context.parameter(term.value())));
                    })).build();
    private final FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, RdbDialect.postgresql());

    @Test
    void singleTableCrudUsesCurrentFieldWithoutLeakingAnotherModelsMetadata() {
        DynamicField text = DynamicField.of("code", "VARCHAR").withLength(80);
        DynamicField number = DynamicField.of("code", "BIGINT");
        DynamicForm strings = DynamicForm.builder("strings", "items").addField(text).build();
        DynamicForm numbers = DynamicForm.builder("numbers", "items").addField(number).build();
        String value = "x' OR 1=1 --";
        ConditionGroup textWhere = where("code", value);
        List<SqlRequest> requests = List.of(renderer.select(strings, textWhere),
                renderer.count(strings, textWhere), renderer.update(strings, Map.of("code", "new"), textWhere),
                renderer.delete(strings, textWhere));
        for (SqlRequest request : requests) {
            assertTrue(request.sql().contains("lower(\"code\") = ?"), request.sql());
            assertFalse(request.sql().contains(value));
            assertEquals(value, request.parameters().getLast());
        }
        SqlRequest numeric = renderer.select(numbers, where("code", 7L));
        assertFalse(numeric.sql().contains("lower"));
        assertEquals(List.of(7L), numeric.parameters());
        assertTrue(renderer.select(strings, textWhere).sql().contains("lower(\"code\")"));
        assertEquals(List.of(text, text, text, text, number, text), resolved);
    }

    @Test
    void relatedDirectAndStructuredConditionsResolveTheTargetField() {
        DynamicField target = DynamicField.of("name", "VARCHAR").withLength(32);
        DynamicForm department = DynamicForm.builder("departments", "departments")
                .features(FeatureRegistry.builder().add(new ValuePrefix("prefix", "department")).build())
                .addField(DynamicField.primaryKey("id", "BIGINT")).addField(target).build();
        DynamicForm users = DynamicForm.builder("users", "users")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("departmentId", "BIGINT"))
                .relation("department", QueryRelation.of(department, "departmentId", "id")).build();
        StructuredConditionResolver resolver = StructuredConditionResolver.allowOperators("declared-value");
        FormReadPlanner planner = new FormReadPlanner(renderer,
                new FormScopeSupport(renderer, resolver, DataScope.none()), SqlExecutionOptions.unlimited());
        SqlRequest direct = planner.select(QuerySpec.of(users, where("department.name", "sales"))).request();
        SqlRequest structured = planner.select(QuerySpec.structured(users,
                StructuredConditionInput.term("department.name", "declared-value", "sales"))).request();
        assertEquals(direct.sql(), structured.sql());
        assertEquals(List.of("sales"), direct.parameters());
        assertEquals(direct.parameters(), structured.parameters());
        assertTrue(direct.sql().contains("lower(\"t1\".\"name\") = ?"), direct.sql());
        assertTrue(resolved.stream().allMatch(field -> field == department.field("name")));
        assertEquals("department", resolved.getFirst().features().feature(ValuePrefix.class).prefix());
        assertFalse(resolved.isEmpty());
    }

    @Test
    void aggregateHavingExposesItsResultTypeAndRendersTheBoundExpression() {
        DynamicForm form = DynamicForm.builder("items", "items")
                .features(FeatureRegistry.builder().add(new ValuePrefix("prefix", "aggregate")).build())
                .addField(DynamicField.of("code", "VARCHAR")).build();
        AggregateSpec spec = AggregateSpec.builder(QuerySpec.of(form, ConditionGroup.and().build()))
                .aggregate(AggregateExpression.countRows("total"))
                .having(where("total", 2L)).build();
        FormAggregatePlanner planner = new FormAggregatePlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults());
        SqlRequest request = planner.plan(spec).request();
        assertTrue(request.sql().contains("count(*) = ?"), request.sql());
        assertEquals(List.of(2L), request.parameters());
        assertEquals("total", resolved.getFirst().name());
        assertTrue(resolved.getFirst().databaseType().isNumeric());
        assertEquals("aggregate", resolved.getFirst().features().feature(ValuePrefix.class).prefix());
    }

    private ConditionGroup where(String field, Object value) {
        return conditions.conditions().where(field, "declared-value", value).build();
    }
}
