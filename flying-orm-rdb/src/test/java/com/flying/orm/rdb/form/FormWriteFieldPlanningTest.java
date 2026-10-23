package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.plan.StructuralPlanCaches;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.lang.management.ManagementFactory;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class FormWriteFieldPlanningTest {
    private static volatile FormSqlRenderSupport.WriteFields fieldsSink;
    private static volatile com.flying.orm.core.sql.render.SqlRequest updateSink;
    private static volatile Object[] batchSink;
    private static volatile boolean layoutMatches;

    @Test
    void batchLayoutMatchingDoesNotAllocateSeparateKeyAndValueViews() {
        requireUninstrumentedVm();
        java.lang.management.ThreadMXBean standard = ManagementFactory.getThreadMXBean();
        assumeTrue(standard instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean allocation = (com.sun.management.ThreadMXBean) standard;
        assumeTrue(allocation.isThreadAllocatedMemorySupported());
        allocation.setThreadAllocatedMemoryEnabled(true);
        Map<String, Object> first = new LinkedHashMap<>();
        first.put("id", 7L);
        first.put("name", "Ada");
        BatchInsertPlan plan = new FormBatchSqlRenderer(support(), RdbDialect.postgresql().upsert())
                .insertPlan(form(), first);
        BatchInsertPlan.Cursor cursor = new BatchInsertPlan.Cursor(first, plan, true);
        int iterations = 30_000;
        double[] bytes = new double[2];
        long thread = Thread.currentThread().threadId();
        for (int mode = 0; mode < 2; mode++) {
            for (int index = 0; index < iterations; index++) {
                Map<String, Object> row = new LinkedHashMap<>(first);
                if (mode == 1) layoutMatches = cursor.matches(row);
                batchSink = plan.parameters(row, index);
            }
            long before = allocation.getThreadAllocatedBytes(thread);
            for (int index = 0; index < iterations; index++) {
                Map<String, Object> row = new LinkedHashMap<>(first);
                if (mode == 1) layoutMatches = cursor.matches(row);
                batchSink = plan.parameters(row, index);
            }
            bytes[mode] = (allocation.getThreadAllocatedBytes(thread) - before) / (double) iterations;
        }
        assertTrue(layoutMatches);
        assertArrayEquals(new Object[]{7L, "Ada"}, batchSink);
        System.out.println("batch layout matching additional allocation B/row=" + (bytes[1] - bytes[0]));
        assertTrue(bytes[1] - bytes[0] < 24, "matching must reuse the binding entry view: " + java.util.Arrays.toString(bytes));
    }

    @Test
    void orderedBatchRowsDoNotAllocateNormalizedFieldNames() {
        requireUninstrumentedVm();
        java.lang.management.ThreadMXBean standard = ManagementFactory.getThreadMXBean();
        assumeTrue(standard instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean allocation = (com.sun.management.ThreadMXBean) standard;
        assumeTrue(allocation.isThreadAllocatedMemorySupported());
        allocation.setThreadAllocatedMemoryEnabled(true);
        DynamicForm.Builder builder = DynamicForm.builder("wide_items", "wide_items")
                .addField(DynamicField.primaryKey("id", "BIGINT"));
        Map<String, Object> values = new LinkedHashMap<>();
        for (int index = 0; index < 64; index++) {
            String name = "fieldValue" + index;
            builder.addField(DynamicField.of(name, "VARCHAR"));
            values.put(name, index % 2 == 0 ? null : "value");
        }
        DynamicForm form = builder.build();
        FormSqlRenderSupport support = support();
        BatchColumnLayout layout = BatchColumnLayout.of(form, support.writeFields(form, values, 0L), support);
        int iterations = 20_000;
        for (int index = 0; index < iterations; index++) batchSink = layout.parameters(values, index);
        long thread = Thread.currentThread().threadId();
        long before = allocation.getThreadAllocatedBytes(thread);
        for (int index = 0; index < iterations; index++) batchSink = layout.parameters(values, index);
        double bytes = (allocation.getThreadAllocatedBytes(thread) - before) / (double) iterations;
        assertArrayEquals(values.values().toArray(), batchSink);
        System.out.println("ordered batch allocation width=64 B/row=" + bytes);
        assertTrue(bytes < 1024, "ordered batch rows must not allocate per-field normalized names: " + bytes);
    }

    @Test
    void updatesDoNotRetainAPerFieldIntermediateCarrier() {
        assertTrue(java.util.Arrays.stream(FormWriteSqlRenderer.class.getDeclaredClasses())
                .noneMatch(type -> type.getSimpleName().equals("UpdateAssignment")),
                "update fields must feed the final bindings and SQL shape directly");
    }

    @Test
    void cachedUpdatesKeepTheirBindingsAcrossSmallAndWideLayouts() {
        requireUninstrumentedVm();
        java.lang.management.ThreadMXBean standard = ManagementFactory.getThreadMXBean();
        assumeTrue(standard instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean allocation = (com.sun.management.ThreadMXBean) standard;
        assumeTrue(allocation.isThreadAllocatedMemorySupported());
        allocation.setThreadAllocatedMemoryEnabled(true);
        for (int width : new int[] {1, 16, 64}) {
            DynamicForm.Builder builder = DynamicForm.builder("updated_items", "updated_items")
                    .addField(DynamicField.primaryKey("id", "BIGINT"));
            Map<String, Object> values = new LinkedHashMap<>();
            for (int index = 0; index < width; index++) {
                String field = "field_" + index;
                builder.addField(DynamicField.of(field, "VARCHAR"));
                values.put(field, "value_" + index);
            }
            DynamicForm form = builder.build();
            FormDataSqlRenderer renderer = renderer();
            com.flying.orm.core.condition.ConditionGroup where =
                    com.flying.orm.core.condition.ConditionGroup.and().where("id", "=", 1L).build();
            com.flying.orm.core.sql.render.SqlRequest first = renderer.update(form, values, where);
            int iterations = 20_000;
            for (int index = 0; index < iterations; index++) updateSink = renderer.update(form, values, where);
            long thread = Thread.currentThread().threadId();
            long before = allocation.getThreadAllocatedBytes(thread);
            for (int index = 0; index < iterations; index++) updateSink = renderer.update(form, values, where);
            double bytes = (allocation.getThreadAllocatedBytes(thread) - before) / (double) iterations;
            assertSame(first.statement(), updateSink.statement());
            assertEquals(width + 1, updateSink.parameters().size());
            assertEquals(List.copyOf(values.values()), updateSink.parameters().subList(0, width));
            assertEquals(1L, updateSink.parameters().getLast());
            System.out.println("cached update allocation width=" + width + " B/op=" + bytes);
            if (width == 64) assertTrue(bytes < 3200, "canonical updates must avoid a duplicate-field index: " + bytes);
        }
    }

    @Test
    void ordinaryUpsertDoesNotRenderTheSameInsertAndParameterColumnsTwice() {
        RdbDialect dialect = RdbDialect.postgresql();
        java.util.concurrent.atomic.AtomicInteger nameRenderings = new java.util.concurrent.atomic.AtomicInteger();
        FormSqlRenderSupport support = new FormSqlRenderSupport(SqlRenderer.builder().addDefaultTerms().build(),
                dialect.json(), dialect.name(), true, name -> {
                    if (name.equals("name")) nameRenderings.incrementAndGet();
                    return dialect.schema().identifier(name);
                }, StructuralPlanCaches.create(OrmCachePolicy.safeDefaults()), java.util.Map.of(),
                dialect.capabilities(), dialect.schema()::relationIdentifier);
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("name", "Ada");
        values.put("id", 1L);
        BatchInsertPlan plan = new FormBatchSqlRenderer(support, dialect.upsert()).upsertPlan(form(), values);
        assertEquals(2, nameRenderings.get(), "one INSERT list and one UPDATE list, without a duplicate parameter list");
        assertArrayEquals(new Object[]{"Ada", 1L}, plan.firstParameters());
        assertArrayEquals(new Object[]{"Bob", 2L}, plan.parameters(Map.of("id", 2L, "NAME", "Bob"), 1));
    }

    @Test
    void singleFieldPlanningDoesNotAllocateDuplicateTracking() {
        requireUninstrumentedVm();
        java.lang.management.ThreadMXBean standard = ManagementFactory.getThreadMXBean();
        assumeTrue(standard instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean allocation = (com.sun.management.ThreadMXBean) standard;
        assumeTrue(allocation.isThreadAllocatedMemorySupported());
        allocation.setThreadAllocatedMemoryEnabled(true);
        FormSqlRenderSupport support = support();
        DynamicForm form = form();
        Map<String, Object> values = Collections.singletonMap("name", null);
        int iterations = 50_000;
        for (int index = 0; index < iterations; index++) fieldsSink = support.writeFields(form, values, 0L);
        long thread = Thread.currentThread().threadId();
        long before = allocation.getThreadAllocatedBytes(thread);
        for (int index = 0; index < iterations; index++) fieldsSink = support.writeFields(form, values, 0L);
        double bytes = (allocation.getThreadAllocatedBytes(thread) - before) / (double) iterations;
        assertNull(fieldsSink.parameters()[0]);
        // 偏离声明顺序的布局和迭代器即使未被 JVM 消除，也不等于创建了去重索引。
        assertTrue(bytes < 176, "bytes per single-field plan: " + bytes);
    }

    @Test
    void wideRowPlanningAvoidsPerFieldTrackingNodesAndValueWrappers() {
        requireUninstrumentedVm();
        java.lang.management.ThreadMXBean standard = ManagementFactory.getThreadMXBean();
        assumeTrue(standard instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean allocation = (com.sun.management.ThreadMXBean) standard;
        assumeTrue(allocation.isThreadAllocatedMemorySupported());
        allocation.setThreadAllocatedMemoryEnabled(true);
        FormSqlRenderSupport support = support();
        DynamicForm.Builder builder = DynamicForm.builder("wide_items", "wide_items");
        Map<String, Object> values = new LinkedHashMap<>();
        for (int index = 0; index < 16; index++) {
            String name = "field_" + index;
            builder.addField(DynamicField.of(name, "VARCHAR"));
            values.put(name, null);
        }
        DynamicForm form = builder.build();
        int iterations = 50_000;
        for (int index = 0; index < iterations; index++) fieldsSink = support.writeFields(form, values, 0L);
        long thread = Thread.currentThread().threadId();
        long before = allocation.getThreadAllocatedBytes(thread);
        for (int index = 0; index < iterations; index++) fieldsSink = support.writeFields(form, values, 0L);
        double bytes = (allocation.getThreadAllocatedBytes(thread) - before) / (double) iterations;
        assertEquals(16, fieldsSink.fields().size());
        assertTrue(bytes < 768, "bytes per sixteen-field plan: " + bytes);
    }

    @Test
    void firstRowStillRejectsNormalizedDuplicatesIncludingNull() {
        FormDataSqlRenderer renderer = renderer();
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("name", null);
        values.put("NAME", "duplicate");
        assertEquals("duplicate normalized dynamic write field", assertThrows(IllegalArgumentException.class,
                () -> renderer.insert(form(), values)).getMessage());
        assertEquals("duplicate normalized dynamic write field", assertThrows(IllegalArgumentException.class,
                () -> renderer.insertBatch(form(), List.of(values))).getMessage());
        assertEquals("duplicate normalized dynamic update field", assertThrows(IllegalArgumentException.class,
                () -> renderer.update(form(), values,
                        com.flying.orm.core.condition.ConditionGroup.and().where("id", "=", 1L).build())).getMessage());
    }

    @Test
    void canonicalFieldIdentityStillRejectsEqualNamesInIdentityMaps() {
        Map<String, Object> values = new java.util.IdentityHashMap<>();
        values.put(new String("name"), null);
        values.put(new String("name"), "duplicate");
        assertEquals(2, values.size());
        assertEquals("duplicate normalized dynamic write field", assertThrows(IllegalArgumentException.class,
                () -> renderer().insertBatch(form(), List.of(values))).getMessage());
        assertEquals("duplicate normalized dynamic update field", assertThrows(IllegalArgumentException.class,
                () -> renderer().update(form(), values,
                        com.flying.orm.core.condition.ConditionGroup.and().where("id", "=", 1L).build())).getMessage());
    }

    @Test
    void canonicalFieldIdentityPreservesUnicodeAndWhitespaceAliases() {
        DynamicForm form = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("\u0130D", "VARCHAR")).build();
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("\u0130D", null);
        values.put(" i\u0307d ", "duplicate");
        assertEquals("duplicate normalized dynamic write field", assertThrows(IllegalArgumentException.class,
                () -> renderer().insert(form, values)).getMessage());
        assertEquals("duplicate normalized dynamic update field", assertThrows(IllegalArgumentException.class,
                () -> renderer().update(form, values,
                        com.flying.orm.core.condition.ConditionGroup.and().where("\u0130D", "=", "old").build())).getMessage());
        FormSqlRenderSupport.WriteFields fields = support().writeFields(form, Map.of(" i\u0307d ", "value"), 0L);
        assertSame(form.fields().getFirst(), fields.fields().getFirst());
        assertArrayEquals(new Object[]{"value"}, fields.parameters());
    }

    @Test
    void singleAndMultipleFieldsKeepCanonicalNamesAndInputOrder() {
        FormDataSqlRenderer renderer = renderer();
        assertEquals(List.of("Ada"), renderer.insert(form(), Map.of("NAME", "Ada")).parameters());
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("NAME", null);
        values.put("ID", 2L);
        assertArrayEquals(new Object[]{null, 2L}, Flux.from(renderer.insertBatch(form(), List.of(values)).rows())
                .blockFirst());
    }

    private static void requireUninstrumentedVm() {
        assumeTrue(ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
                .noneMatch(argument -> argument.startsWith("-javaagent:")),
                "allocation limits must run without bytecode instrumentation; run this test without -Pquality");
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
    }

    private static FormSqlRenderSupport support() {
        RdbDialect dialect = RdbDialect.postgresql();
        return new FormSqlRenderSupport(SqlRenderer.builder().addDefaultTerms().build(),
                dialect.json(), dialect.name(), true, dialect.schema()::identifier,
                StructuralPlanCaches.create(OrmCachePolicy.safeDefaults()), java.util.Map.of(),
                dialect.capabilities(), dialect.schema()::relationIdentifier);
    }

    private static DynamicForm form() {
        return DynamicForm.builder("items", "items")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR")).build();
    }
}
