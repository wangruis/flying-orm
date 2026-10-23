package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.plan.StructuralPlanCaches;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class FormBatchInsertPlanReuseTest {
    @Test
    void cursorMatchesCurrentColumnsWithoutConfusingOrderNullOrExpressions() {
        RdbDialect dialect = RdbDialect.postgresql();
        FormSqlRenderSupport support = new FormSqlRenderSupport(SqlRenderer.builder().addDefaultTerms().build(),
                dialect.json(), dialect.name(), true, dialect.schema()::identifier,
                StructuralPlanCaches.create(OrmCachePolicy.safeDefaults()), Map.of(),
                dialect.capabilities(), dialect.schema()::relationIdentifier);
        Map<String, Object> first = row(1L, "one");
        BatchInsertPlan plan = new FormBatchSqlRenderer(support, dialect.upsert()).insertPlan(form("VARCHAR"), first);
        BatchInsertPlan.Cursor cursor = new BatchInsertPlan.Cursor(first, plan, true);
        Map<String, Object> reordered = new LinkedHashMap<>();
        reordered.put("name", null);
        reordered.put("id", 2L);
        assertTrue(cursor.matches(reordered));
        assertArrayEquals(new Object[]{2L, null}, plan.parameters(reordered, 1));
        assertFalse(cursor.matches(Map.of("id", 2L)));
        assertFalse(cursor.matches(Map.of("id", 2L, "other", "two")));
        assertFalse(cursor.matches(Map.of("id", 2L, "name", com.flying.orm.core.expression.ValueExpression.value("two"))));
        assertTrue(cursor.matches(row(3L, "three")));
        assertArrayEquals(new Object[]{3L, "three"}, plan.parameters(row(3L, "three"), 2));
    }

    @Test
    void cachedStatementStillEncodesEveryCurrentRow() {
        AtomicInteger writes = new AtomicInteger();
        ValueCodec codec = new ValueCodec() {
            @Override
            public boolean supports(Class<?> type) { return type == String.class; }

            @Override
            public Object write(Object value) { return value + "-" + writes.incrementAndGet(); }

            @Override
            public Object read(Object value, Class<?> type) { return value; }
        };
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms()
                .valueCodecs(ValueCodecRegistry.standard().withFirst(codec)).build(), RdbDialect.postgresql());
        DynamicForm form = form("VARCHAR");
        BatchWriteRequest first = renderer.insertBatch(form, List.of(row(1L, "one"), row(2L, "two")));
        BatchWriteRequest second = renderer.insertBatch(form, List.of(row(3L, "three")));
        assertSame(first.statement(), second.statement());
        assertEquals(3, writes.get());
        List<Object[]> firstRows = Flux.from(first.rows()).collectList().block();
        assertArrayEquals(new Object[]{1L, "one-1"}, firstRows.getFirst());
        assertArrayEquals(new Object[]{2L, "two-2"}, firstRows.get(1));
        assertArrayEquals(new Object[]{3L, "three-3"}, Flux.from(second.rows()).blockFirst());
    }

    @Test
    void singleAndBatchInsertsShareStructureButNeverRowValues() {
        FormDataSqlRenderer renderer = renderer(StructuralPlanCaches.create(OrmCachePolicy.safeDefaults()));
        DynamicForm form = form("VARCHAR");
        BatchWriteRequest first = renderer.insertBatch(form, List.of(row(1L, "one")));
        BatchWriteRequest second = renderer.insertBatch(form, List.of(row(2L, "two"), row(3L, "three")));
        assertSame(first.statement(), second.statement());
        assertSame(first.statement(), renderer.insert(form, row(4L, "four")).statement());
        List<Object[]> firstRows = Flux.from(first.rows()).collectList().block();
        List<Object[]> secondRows = Flux.from(second.rows()).collectList().block();
        assertArrayEquals(new Object[]{1L, "one"}, firstRows.getFirst());
        assertArrayEquals(new Object[]{2L, "two"}, secondRows.getFirst());
        assertArrayEquals(new Object[]{3L, "three"}, secondRows.get(1));
        assertNotSame(firstRows.getFirst(), secondRows.getFirst());
    }

    @Test
    void batchPlansFollowExistingTableInvalidation() {
        StructuralPlanCaches caches = StructuralPlanCaches.create(OrmCachePolicy.safeDefaults());
        FormDataSqlRenderer renderer = renderer(caches);
        DynamicForm form = form("VARCHAR");
        BatchWriteRequest first = renderer.insertBatch(form, List.of(row(1L, "one")));
        assertSame(first.statement(), renderer.insertBatch(form, List.of(row(2L, "two"))).statement());
        caches.invalidate("batch_people");
        assertNotSame(first.statement(), renderer.insertBatch(form, List.of(row(3L, "three"))).statement());
    }

    @Test
    void disabledCacheDoesNotRetainBatchStatements() {
        StructuralPlanCaches caches = StructuralPlanCaches.create(OrmCachePolicy.builder()
                .sqlPlans(CacheRegionPolicy.disabled()).build());
        FormDataSqlRenderer renderer = renderer(caches);
        DynamicForm form = form("VARCHAR");
        BatchWriteRequest first = renderer.insertBatch(form, List.of(row(1L, "one")));
        BatchWriteRequest second = renderer.insertBatch(form, List.of(row(2L, "two")));
        assertNotSame(first.statement(), second.statement());
        assertEquals(first.sql(), second.sql());
    }

    @Test
    void differentColumnShapesAndPhysicalTypesDoNotSharePlans() {
        FormDataSqlRenderer renderer = renderer(StructuralPlanCaches.create(OrmCachePolicy.safeDefaults()));
        DynamicForm text = form("VARCHAR");
        BatchWriteRequest full = renderer.insertBatch(text, List.of(row(1L, "one")));
        BatchWriteRequest idOnly = renderer.insertBatch(text, List.of(Map.of("id", 2L)));
        BatchWriteRequest json = renderer.insertBatch(form("JSON"), List.of(row(3L, "\"three\"")));
        assertNotSame(full.statement(), idOnly.statement());
        assertNotSame(full.statement(), json.statement());
        assertEquals(1, idOnly.parameterCount());
        assertEquals(2, json.parameterCount());
    }

    private static FormDataSqlRenderer renderer(StructuralPlanCaches caches) {
        return FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql(),
                caches, CacheRegionPolicy.sqlPlanDefaults(), ProtectedFieldRuntime.withoutKeys(), List.of());
    }

    private static DynamicForm form(String type) {
        return DynamicForm.builder("batch_people", "batch_people")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", type)).build();
    }

    private static Map<String, Object> row(long id, String name) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", id);
        row.put("name", name);
        return row;
    }
}
