package com.flying.orm.rdb.operator;

import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class ComputedProjectionAssemblyTest {
    private static volatile QuerySpec specSink;

    @Test
    void entityCommandDoesNotOwnAnotherSetOfQueryCollections() {
        for (Class<?> command : java.util.List.of(EntityQueryCommand.class, DmlQueryCommand.class)) {
            for (java.lang.reflect.Field field : command.getDeclaredFields()) {
                assertFalse(java.util.Collection.class.isAssignableFrom(field.getType())
                                || java.util.Map.class.isAssignableFrom(field.getType()),
                        "query state belongs to QuerySpec.Builder, not " + command.getSimpleName() + "." + field.getName());
            }
        }
        assertFalse(java.util.Arrays.stream(DmlQueryCommand.class.getDeclaredMethods())
                        .anyMatch(method -> method.getName().equals("request") || method.getName().equals("selectColumns")),
                "SQL assembly belongs to the shared query renderer");
    }

    @Test
    void dynamicProjectionAssemblyDoesNotCopyEachPrefix() {
        PlanningDynamicQuery small = dynamic(16);
        PlanningDynamicQuery large = dynamic(64);
        assertLinearGrowth(() -> small.governedQuery(null).spec(), () -> large.governedQuery(null).spec());
    }

    @Test
    void entityProjectionAssemblyDoesNotCopyEachPrefix() {
        PlanningQuery small = entity(16);
        PlanningQuery large = entity(64);
        assertLinearGrowth(small::projectedSpec, large::projectedSpec);
    }

    @Test
    void entityBranchAndCountDoNotRetainLaterExpressionChanges() {
        PlanningQuery original = entity(2);
        PlanningQuery branch = original.copy();
        QuerySpec snapshot = original.projectedSpec();
        branch.selectExpression(ignored -> ValueExpression.value(3), "branch_value");
        original.selectExpression(ignored -> ValueExpression.value(4), "original_value");
        assertEquals(2, snapshot.expressions().size());
        assertEquals(3, branch.projectedSpec().expressions().size());
        assertFalse(branch.projectedSpec().expressions().containsKey("original_value"));
        assertFalse(original.projectedSpec().expressions().containsKey("branch_value"));
        assertTrue(branch.countSpec().expressions().isEmpty());
        assertTrue(original.countSpec().expressions().isEmpty());
        assertTrue(dynamic(4).governedCountQuery(null).spec().expressions().isEmpty());
    }

    private static void assertLinearGrowth(Supplier<QuerySpec> small, Supplier<QuerySpec> large) {
        java.lang.management.ThreadMXBean standard = ManagementFactory.getThreadMXBean();
        assumeTrue(standard instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean allocation = (com.sun.management.ThreadMXBean) standard;
        assumeTrue(allocation.isThreadAllocatedMemorySupported());
        allocation.setThreadAllocatedMemoryEnabled(true);
        for (int index = 0; index < 2_000; index++) {
            specSink = small.get();
            specSink = large.get();
        }
        double smallBytes = allocated(allocation, small);
        double largeBytes = allocated(allocation, large);
        assertEquals(64, specSink.expressions().size());
        assertEquals("value63", specSink.projections().getLast());
        System.out.println("projection assembly B/op: 16=" + smallBytes + ", 64=" + largeBytes);
        assertTrue(largeBytes < smallBytes * 6,
                "four times the columns must not require quadratic copying: " + smallBytes + " -> " + largeBytes);
    }

    private static double allocated(com.sun.management.ThreadMXBean allocation, Supplier<QuerySpec> source) {
        long thread = Thread.currentThread().threadId();
        long before = allocation.getThreadAllocatedBytes(thread);
        for (int index = 0; index < 1_000; index++) specSink = source.get();
        return (allocation.getThreadAllocatedBytes(thread) - before) / 1_000.0;
    }

    private static PlanningDynamicQuery dynamic(int columns) {
        PlanningDynamicQuery command = new PlanningDynamicQuery(renderer(), DataScope.none());
        command.from(DynamicForm.builder("items", "items").addField(DynamicField.of("id", "BIGINT")).build());
        for (int index = 0; index < columns; index++) command.select(ValueExpression.value(index), "value" + index);
        return command;
    }

    private static PlanningQuery entity(int columns) {
        PlanningQuery command = new PlanningQuery(new EntityDmlModel<>(
                EntityModelRegistry.create(CacheRegionPolicy.disabled()), null, Item.class), null);
        for (int index = 0; index < columns; index++) {
            ValueExpression expression = ValueExpression.value(index);
            command.selectExpression(ignored -> expression, "value" + index);
        }
        return command;
    }

    private static final class PlanningQuery extends EntityQueryCommand<Item, PlanningQuery> {
        PlanningQuery(EntityDmlModel<Item> model, com.flying.orm.core.scope.FieldScope fields) {
            super(model, renderer(), fields);
        }
        PlanningQuery(PlanningQuery source) { super(source); }
        PlanningQuery copy() { return new PlanningQuery(this); }
        @Override protected PlanningQuery self() { return this; }
    }

    private static SqlRenderer renderer() {
        return SqlRenderer.builder().addDefaultTerms().build();
    }

    @TableName("items")
    public static class Item {
        public Long id;
    }
}
