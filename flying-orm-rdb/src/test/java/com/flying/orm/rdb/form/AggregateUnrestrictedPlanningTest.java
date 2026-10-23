package com.flying.orm.rdb.form;

import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.aggregate.GroupSelection;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import com.flying.orm.rdb.internal.plan.StructuralPlanCaches;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class AggregateUnrestrictedPlanningTest {
    private static volatile FormAggregatePlanner.Plan sink;
    private static volatile GroupSelection groupSink;
    private static volatile AggregateExpression<Long> countSink;

    @Test
    void reusesOnlySqlStructureAndInvalidatesItWithTheExistingTableCache() {
        StructuralPlanCaches caches = StructuralPlanCaches.create(OrmCachePolicy.safeDefaults());
        FormAggregatePlanner planner = planner(caches);
        AggregateSpec firstSpec = report(DataScope.none(), 1L, 1000L);
        FormAggregatePlanner.Plan first = planner.plan(firstSpec);
        FormAggregatePlanner.Plan second = planner.plan(report(DataScope.none(), 2L, 2000L));

        assertSame(first.request().statement(), second.request().statement());
        assertEquals(List.of(1L, 1000L), first.request().parameters());
        assertEquals(List.of(2L, 2000L), second.request().parameters());
        assertNotSame(first, second);
        caches.invalidate(firstSpec.query().form().relationIdentity());
        assertNotSame(first.request().statement(), planner.plan(firstSpec).request().statement());
    }

    @Test
    void cachedStructureDoesNotReuseFieldApprovals() {
        FormAggregatePlanner planner = planner();
        FormAggregatePlanner.Plan unrestricted = planner.plan(report(DataScope.none()));
        FormAggregatePlanner.Plan restricted = planner.plan(report(
                DataScope.none().withFields(FieldScope.readable("id", "status", "score"))));
        assertSame(unrestricted.request().statement(), restricted.request().statement());
        assertTrue(unrestricted.fieldUse().isUnrestricted());
        assertFalse(restricted.fieldUse().isUnrestricted());
        assertThrows(IllegalArgumentException.class, () -> planner.plan(report(
                DataScope.none().withFields(FieldScope.readable("id", "status")))));
    }

    @Test
    void computedProjectionAndOrderKeepFreshBindingsAndSeparateSqlShapes() {
        FormAggregatePlanner planner = planner();
        QuerySpec query = report(DataScope.none()).query();
        FormAggregatePlanner.Plan first = planner.plan(computed(query, 7));
        FormAggregatePlanner.Plan second = planner.plan(computed(query, 9));
        assertSame(first.request().statement(), second.request().statement());
        assertEquals(List.of(7, 1L, 1000L, 7, 7), first.request().parameters());
        assertEquals(List.of(9, 1L, 1000L, 9, 9), second.request().parameters());
        for (AggregateSpec changed : List.of(
                AggregateSpec.builder(query).aggregate(AggregateExpression.sum("score", "total")).build(),
                AggregateSpec.builder(query).aggregate(AggregateExpression.sum("score", "renamed")).build(),
                AggregateSpec.builder(query).group("status", "status")
                        .aggregate(AggregateExpression.sum("score", "total")).build(),
                AggregateSpec.builder(query).aggregate(AggregateExpression.sum("score", "total"))
                        .orderBy(PageSort.desc("total")).build())) {
            FormAggregatePlanner.Plan cached = planner.plan(changed);
            FormAggregatePlanner.Plan uncached = planner(StructuralPlanCaches.create(
                    OrmCachePolicy.builder().sqlPlans(CacheRegionPolicy.disabled()).build())).plan(changed);
            assertEquals(uncached.request().sql(), cached.request().sql());
            assertNotSame(first.request().statement(), cached.request().statement());
        }
    }

    @Test
    void disabledPolicyAndCustomTermsDoNotEnterTheSqlPlanCache() {
        StructuralPlanCaches disabled = StructuralPlanCaches.create(
                OrmCachePolicy.builder().sqlPlans(CacheRegionPolicy.disabled()).build());
        FormAggregatePlanner planner = planner(disabled);
        AggregateSpec spec = report(DataScope.none());
        assertNotSame(planner.plan(spec).request().statement(), planner.plan(spec).request().statement());
        assertEquals(0, disabled.sqlSnapshot().estimatedSize());

        StructuralPlanCaches caches = StructuralPlanCaches.create(OrmCachePolicy.safeDefaults());
        SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms()
                .addTerm(SqlTermHandler.of("custom", (term, context) -> new SqlFragment(
                        context.identifier(term.field()) + " = ?", List.of(context.parameter(term.value()))))).build();
        FormAggregatePlanner custom = planner(FormDataSqlRenderer.create(conditions, RdbDialect.postgresql(),
                caches, CacheRegionPolicy.sqlPlanDefaults(), ProtectedFieldRuntime.withoutKeys(), List.of()));
        AggregateSpec customSpec = AggregateSpec.builder(QuerySpec.of(spec.query().form(),
                conditions.conditions().where("id", "custom", 5L).build()))
                .aggregate(AggregateExpression.countRows("total")).build();
        assertNotSame(custom.plan(customSpec).request().statement(), custom.plan(customSpec).request().statement());
        assertEquals(List.of(5L), custom.plan(customSpec).request().parameters());
        assertEquals(0, caches.sqlSnapshot().estimatedSize());
    }

    private static AggregateSpec computed(QuerySpec query, int fallback) {
        return AggregateSpec.builder(query).aggregate(AggregateExpression.sum(
                ValueExpression.field("score").coalesce(ValueExpression.value(fallback)), "total"))
                .orderByFirst("total", fallback).build();
    }

    @Test
    void declarationsDoNotAllocateUnusedFoldedNames() {
        org.junit.jupiter.api.Assumptions.assumeTrue(
                ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
                        .noneMatch(argument -> argument.startsWith("-javaagent:")),
                "allocation limits require an uninstrumented VM");
        com.sun.management.ThreadMXBean allocation =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        allocation.setThreadAllocatedMemoryEnabled(true);
        String name = "REPORT_STATUS";
        int iterations = 50_000;
        for (int index = 0; index < iterations; index++) {
            groupSink = GroupSelection.of(name, name);
            countSink = AggregateExpression.countRows(name);
        }
        long thread = Thread.currentThread().threadId();
        long before = allocation.getThreadAllocatedBytes(thread);
        for (int index = 0; index < iterations; index++) {
            groupSink = GroupSelection.of(name, name);
            countSink = AggregateExpression.countRows(name);
        }
        long bytes = allocation.getThreadAllocatedBytes(thread) - before;
        assertTrue(bytes < iterations * 128L, "bytes per declaration pair: " + bytes / (double) iterations);
        assertEquals(name, groupSink.field());
        assertEquals(name, groupSink.alias());
        assertEquals(name, countSink.alias());
        assertEquals("Report", GroupSelection.of(" Report ", " Alias ").field());
        assertEquals("Alias", AggregateExpression.countRows(" Alias ").alias());
        assertThrows(IllegalArgumentException.class, () -> GroupSelection.of(" ", "alias"));
        assertThrows(IllegalArgumentException.class, () -> AggregateExpression.countRows(null));
    }

    @Test
    void unrestrictedPlanningAvoidsUnusedRequirementAllocations() {
        org.junit.jupiter.api.Assumptions.assumeTrue(
                ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
                        .noneMatch(argument -> argument.startsWith("-javaagent:")),
                "allocation limits require an uninstrumented VM; run this test without -Pquality");
        FormAggregatePlanner planner = planner();
        AggregateSpec report = report(DataScope.none());
        com.sun.management.ThreadMXBean allocation =
                (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
        allocation.setThreadAllocatedMemoryEnabled(true);
        int iterations = 50_000;
        for (int index = 0; index < iterations; index++) sink = planner.plan(report);
        long thread = Thread.currentThread().threadId();
        long before = allocation.getThreadAllocatedBytes(thread);
        for (int index = 0; index < iterations; index++) sink = planner.plan(report);
        long bytes = allocation.getThreadAllocatedBytes(thread) - before;
        assertTrue(bytes < iterations * 6_000L, "bytes per plan: " + bytes / (double) iterations);
        assertTrue(sink.fieldUse().isUnrestricted());
    }

    @Test
    void explicitGovernanceKeepsTheSameSqlAndBindings() {
        FormAggregatePlanner planner = planner();
        AggregateSpec report = report(DataScope.none());
        FormAggregatePlanner.Plan ordinary = planner.plan(report);
        FormAggregatePlanner.Plan governed = planner.plan(report, true);
        assertEquals(governed.request().sql(), ordinary.request().sql());
        assertEquals(governed.request().parameters(), ordinary.request().parameters());
        assertEquals(governed.layout().groups(), ordinary.layout().groups());
        assertEquals(governed.layout().aggregates(), ordinary.layout().aggregates());
    }

    @Test
    void eitherReadOrWriteScopeRestrictionStillPublishesDecisions() {
        FormAggregatePlanner planner = planner();
        for (FieldScope fields : new FieldScope[]{FieldScope.readable("id", "status", "score"),
                FieldScope.writable("id")}) {
            FormAggregatePlanner.Plan plan = planner.plan(report(DataScope.none().withFields(fields)));
            assertFalse(plan.fieldUse().isUnrestricted());
            assertFalse(plan.fieldUse().decisions().isEmpty());
            assertTrue(plan.fieldUse().allowed());
        }
    }

    @Test
    void restrictedAndEmptyIntersectedReadScopesDoNotBecomeUnrestricted() {
        FormAggregatePlanner planner = planner();
        DataScope excludesScore = DataScope.none().withFields(FieldScope.readable("id", "status"));
        assertThrows(IllegalArgumentException.class, () -> planner.plan(report(excludesScore)));
        DataScope noReadableFields = DataScope.none().withFields(FieldScope.readable("id"))
                .withFields(FieldScope.readable("status"));
        assertThrows(ScopeAccessException.class, () -> planner.plan(report(noReadableFields)));
    }

    private static AggregateSpec report(DataScope scope) {
        return report(scope, 1L, 1000L);
    }

    private static AggregateSpec report(DataScope scope, long lower, long upper) {
        DynamicForm form = DynamicForm.builder("bench_rows", "bench_rows")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("status", "INTEGER"))
                .addField(DynamicField.of("score", "DECIMAL")).build();
        return AggregateSpec.builder(QuerySpec.of(form,
                        ConditionGroup.and().where("id", ">=", lower).where("id", "<=", upper).build()).withScope(scope))
                .group(GroupSelection.of("status", "status"))
                .aggregate(AggregateExpression.count("id", "row_count"))
                .aggregate(AggregateExpression.count("score", "nonnull_count"))
                .aggregate(AggregateExpression.sum("score", "total_score"))
                .orderBy(PageSort.asc("status")).build();
    }

    private static FormAggregatePlanner planner() {
        return planner(StructuralPlanCaches.create(OrmCachePolicy.safeDefaults()));
    }

    private static FormAggregatePlanner planner(StructuralPlanCaches caches) {
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql(), caches,
                CacheRegionPolicy.sqlPlanDefaults(), ProtectedFieldRuntime.withoutKeys(), List.of());
        return planner(renderer);
    }

    private static FormAggregatePlanner planner(FormDataSqlRenderer renderer) {
        return new FormAggregatePlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(renderer.valueCodecs()), DataScope.none()),
                SqlExecutionOptions.unlimited(),
                FieldUsePolicy.unrestricted(),
                QueryShapeLimits.defaults());
    }
}
