package com.flying.orm.rdb.form.spec;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.page.ExpressionSort;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNotSame;

/** 查询规格只在调用方输入进入不可变边界时冻结列表。 */
class QuerySpecInputOwnershipTest {

    @Test
    void copiedBuilderOwnsItsSelectionsSortsFiltersAndExclusions() {
        QuerySpec.Builder original = QuerySpec.builder(form()).select("id", "name")
                .expression("label", ValueExpression.value("original"))
                .sort(PageSort.asc("id")).exclude("created_at");
        QuerySpec.Builder branch = original.copy();
        QuerySpec before = original.build();
        original.expression("only_original", ValueExpression.value(1L)).exclude("name");
        branch.expression("only_branch", ValueExpression.value(2L)).exclude("id")
                .groupBy("name").sort(PageSort.desc("name"));

        assertEquals(List.of("id", "name", "label"), before.projections());
        assertEquals(List.of("id", "label", "only_original"), original.build().projections());
        assertEquals(List.of("name", "label", "only_branch"), branch.build().projections());
        assertEquals(List.of(PageSort.asc("id")), original.build().sorts());
        assertEquals(List.of(PageSort.asc("id"), PageSort.desc("name")), branch.build().sorts());
        assertEquals(List.of("name"), branch.buildCount().projections());
        assertEquals(Map.of(), branch.buildCount().expressions());
        assertEquals(List.of(), branch.buildConditions().groups());
        assertEquals(List.of(), branch.buildConditions().projections());
    }

    @Test
    void deferredSortMappingResolvesLaterAliasesWithoutMutatingEarlierPlans() {
        QuerySpec.Builder builder = QuerySpec.builder(form()).select("id")
                .orderBy(ValueExpression.field("label"), PageSort.Direction.ASC);
        QuerySpec before = builder.build(field -> "name");
        builder.expression("LABEL", ValueExpression.value(1L));
        QuerySpec after = builder.build(field -> "name");

        assertEquals(List.of(PageSort.asc("name")), before.sorts());
        assertEquals(List.of(PageSort.asc("label")), after.sorts());
        assertEquals(Map.of(), before.expressions());
        assertEquals(Map.of("label", ValueExpression.value(1L)), after.expressions());
        assertEquals(after.sorts(), builder.build(field -> "name").sorts());
    }

    @Test
    void builderFreezesACompleteQueryWithoutChangingEarlierSnapshots() {
        ConditionGroup where = ConditionGroup.and().where("name", "=", "Alice").build();
        QuerySpec.Builder builder = QuerySpec.builder(form()).where(where)
                .select("id").sort(PageSort.asc("id"));
        QuerySpec first = builder.build();
        QuerySpec second = builder.select("name").exclude("ID").groupBy("name")
                .scope(DataScope.tenant("id", 7L)).build();

        assertEquals(List.of("id"), first.projections());
        assertEquals(List.of(), first.groups());
        assertEquals(List.of("name"), second.projections());
        assertEquals(List.of("name"), second.groups());
        assertSame(where, first.where());
        assertSame(where, second.where());
        assertEquals(List.of(PageSort.asc("id")), second.sorts());
        assertThrows(UnsupportedOperationException.class, () -> first.projections().add("name"));
    }

    @Test
    void builderKeepsProjectionAndSortExpressionSemantics() {
        DynamicForm form = form();
        Map<String, ValueExpression> expressions = new LinkedHashMap<>();
        expressions.put("next_id", ValueExpression.field("id").add(ValueExpression.value(1L)));
        expressions.put("unused", ValueExpression.value("unused"));
        List<ExpressionSort> sorts = List.of(
                new ExpressionSort(ValueExpression.field("next_id"), PageSort.Direction.DESC),
                new ExpressionSort(ValueExpression.field("id").add(ValueExpression.value(2L)), PageSort.Direction.ASC));
        StructuredConditionInput input = new StructuredConditionInput("id", "=", 7L, null, List.of());
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults();
        SqlExecutionOptions options = SqlExecutionOptions.unlimited();
        ConditionGroup where = ConditionGroup.and().where("name", "=", "Alice").build();
        DataScope scope = DataScope.tenant("id", 7L);
        QuerySpec expected = QuerySpec.structured(form, input).withStructuredPolicy(policy)
                .withWhere(where).withScope(scope).withExpressions(expressions).withOrderBy(sorts)
                .withProjection(List.of("id", "next_id"), List.of("id"))
                .withExcludedFields(List.of("NEXT_ID")).withExecutionOptions(options).masked();
        QuerySpec.Builder builder = QuerySpec.builder(form).filter(input, policy).where(where).scope(scope)
                .expressions(expressions).orderBy(sorts).projection(List.of("id", "next_id"), List.of("id"))
                .exclude("NEXT_ID").executionOptions(options).display(SensitiveDisplayMode.MASKED);
        QuerySpec actual = builder.build();
        expressions.clear();
        builder.expressions(Map.of("later", ValueExpression.value(3L))).sort(PageSort.asc("name"));

        assertEquals(expected.projections(), actual.projections());
        assertEquals(expected.groups(), actual.groups());
        assertEquals(expected.sorts(), actual.sorts());
        assertEquals(expected.expressions(), actual.expressions());
        assertEquals(expected.excludedFields(), actual.excludedFields());
        assertEquals(expected.executionOptions(), actual.executionOptions());
        assertEquals(expected.structuredInput(), actual.structuredInput());
        assertEquals(expected.structuredPolicy(), actual.structuredPolicy());
        assertEquals(expected.sensitiveDisplayMode(), actual.sensitiveDisplayMode());
        assertSame(where, actual.where());
        assertSame(scope, actual.scope());
        assertThrows(UnsupportedOperationException.class, () -> actual.expressions().clear());
    }

    @Test
    void unchangedQuerySettingsReuseThePublishedSpec() {
        QuerySpec spec = QuerySpec.of(form(), ConditionGroup.and().build());
        assertSame(spec, spec.withScope(DataScope.none()));
        assertSame(spec, spec.withWhere(spec.where()));
        assertSame(spec, spec.withSorts(List.of()));
        assertSame(spec, spec.declaredDisplay());

        SqlExecutionOptions options = SqlExecutionOptions.unlimited();
        QuerySpec configured = spec.withScope(DataScope.tenant("id", 7L))
                .withExecutionOptions(options).withSorts(List.of(PageSort.asc("id"))).masked();
        assertNotSame(spec, configured);
        assertSame(configured, configured.withScope(configured.scope()));
        assertSame(configured, configured.withExecutionOptions(options));
        assertSame(configured, configured.masked());
        List<PageSort> equalSorts = new ArrayList<>(configured.sorts());
        assertSame(configured, configured.withSorts(equalSorts));
        equalSorts.clear();
        assertEquals(List.of(PageSort.asc("id")), configured.sorts());
        QuerySpec full = configured.showSensitive();
        assertNotSame(configured, full);
        assertSame(full, full.showSensitive());
        assertThrows(NullPointerException.class, () -> spec.withExecutionOptions(null));
        assertThrows(NullPointerException.class, () -> configured.withScope(null));
        assertThrows(NullPointerException.class, () -> configured.withWhere(null));
    }

    @Test
    void unchangedStructuredPolicyReusesSpecWithoutAcceptingNull() {
        StructuredConditionInput input = new StructuredConditionInput("id", "=", 1L, null, List.of());
        QuerySpec spec = QuerySpec.structured(form(), input);
        assertSame(spec, spec.withStructuredPolicy(spec.structuredPolicy().orElseThrow()));
        StructuredConditionPolicy policy = StructuredConditionPolicy.defaults();
        QuerySpec configured = spec.withStructuredPolicy(policy);
        assertSame(policy, configured.structuredPolicy().orElseThrow());
        assertSame(configured, configured.withStructuredPolicy(policy));
        assertThrows(NullPointerException.class, () -> configured.withStructuredPolicy(null));
    }

    @Test
    void snapshotsProjectionGroupAndSortInputsAtTheirReplacementBoundaries() {
        List<String> projections = new ArrayList<>(List.of("id", "name"));
        List<String> groups = new ArrayList<>(List.of("name"));
        List<PageSort> sorts = new ArrayList<>(List.of(PageSort.asc("name")));

        QuerySpec spec = QuerySpec.of(form(), ConditionGroup.and().build())
                .withProjection(projections, groups)
                .withSorts(sorts);
        projections.add("created_at");
        groups.clear();
        sorts.add(PageSort.desc("id"));

        assertEquals(List.of("id", "name"), spec.projections());
        assertEquals(List.of("name"), spec.groups());
        assertEquals(List.of(PageSort.asc("name")), spec.sorts());
        assertThrows(UnsupportedOperationException.class, () -> spec.projections().add("created_at"));
        assertThrows(UnsupportedOperationException.class, () -> spec.groups().clear());
        assertThrows(UnsupportedOperationException.class, () -> spec.sorts().clear());
    }

    @Test
    void preservesProjectionGroupAndSortRejectionContracts() {
        QuerySpec spec = QuerySpec.of(form(), ConditionGroup.and().build());

        assertEquals("query projections must not be null",
                assertThrows(NullPointerException.class,
                        () -> spec.withProjection(null, List.of())).getMessage());
        assertEquals("projected query must select at least one field",
                assertThrows(IllegalArgumentException.class,
                        () -> spec.withProjection(List.of(), null)).getMessage());
        assertThrows(NullPointerException.class,
                () -> spec.withProjection(Arrays.asList("id", null), List.of()));
        assertEquals("query projections must not contain blank values",
                assertThrows(IllegalArgumentException.class,
                        () -> spec.withProjection(List.of(" "), List.of())).getMessage());
        assertEquals("query groups must not contain blank values",
                assertThrows(IllegalArgumentException.class,
                        () -> spec.withProjection(List.of("id", "id"), List.of("\t"))).getMessage());
        assertEquals("query sorts must not be null",
                assertThrows(NullPointerException.class, () -> spec.withSorts(null)).getMessage());
        assertThrows(NullPointerException.class,
                () -> spec.withSorts(Arrays.asList(PageSort.asc("id"), null)));
        assertEquals(List.of("id", "id"),
                spec.withProjection(List.of("id", "id"), List.of()).projections());
    }

    @Test
    void preservesFactoryAndStructuredPolicyNullPrecedence() {
        assertEquals("query form must not be null",
                assertThrows(NullPointerException.class, () -> QuerySpec.of(null, null)).getMessage());
        assertEquals("structured condition input must not be null",
                assertThrows(NullPointerException.class, () -> QuerySpec.structured(null, null)).getMessage());
        assertEquals("structured policy requires a structured query spec",
                assertThrows(IllegalStateException.class,
                        () -> QuerySpec.of(form(), ConditionGroup.and().build())
                                .withStructuredPolicy(null)).getMessage());
    }

    private static DynamicForm form() {
        return DynamicForm.builder("records", "records")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR"))
                .addField(DynamicField.of("created_at", "TIMESTAMP"))
                .build();
    }
}
