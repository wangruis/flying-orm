package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorPageResult;
import com.flying.orm.core.page.CursorSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CursorPageOwnershipConvergenceTest {

    private static final int LARGE_CURSOR_CHARS = 64 * 1_024;

    private static final int ALLOCATION_ITERATIONS = 20;

    private static final long MIN_AVOIDED_DUPLICATE_ALLOCATION =
            LARGE_CURSOR_CHARS * Character.BYTES - 4_096L;

    @Test
    void normalizationCopiesADeepCursorOnlyOnce() {
        java.lang.management.ThreadMXBean standardBean = ManagementFactory.getThreadMXBean();
        assumeTrue(standardBean instanceof com.sun.management.ThreadMXBean,
                   "per-thread allocation counters are unavailable");
        com.sun.management.ThreadMXBean allocationBean = (com.sun.management.ThreadMXBean) standardBean;
        assumeTrue(allocationBean.isThreadAllocatedMemorySupported(),
                   "per-thread allocation counters are unsupported");
        if (!allocationBean.isThreadAllocatedMemoryEnabled()) {
            allocationBean.setThreadAllocatedMemoryEnabled(true);
        }
        assumeTrue(allocationBean.isThreadAllocatedMemoryEnabled(),
                   "per-thread allocation counters could not be enabled");
        DynamicForm form = DynamicForm.builder("text_events", "text_events")
                                      .addField(DynamicField.primaryKey("event_id", "BIGINT"))
                                      .addField(DynamicField.of("label", "VARCHAR").withNullable(false))
                                      .build();
        char[] cursorText = new char[LARGE_CURSOR_CHARS];
        Arrays.fill(cursorText, 'x');
        String expectedText = new String(cursorText);
        CursorPageQuery page = CursorPageQuery.after(
                20,
                List.of(cursorText, 42L),
                CursorSort.asc("label"));
        cursorText[0] = 'y';
        List<CursorSort> normalizedSorts = List.of(
                CursorSort.asc("label"), CursorSort.asc("event_id"));
        Supplier<Object> carrierPath = () -> CursorPageNormalizer.normalize(form, page);
        Supplier<Object> reconstructedPublicPath = () -> new CursorPageQuery(
                page.size(), normalizedSorts, page.cursor());
        for (int iteration = 0; iteration < 100; iteration++) {
            carrierPath.get();
            reconstructedPublicPath.get();
        }

        long carrierAllocation = allocatedBytes(allocationBean, carrierPath);
        long reconstructedAllocation = allocatedBytes(allocationBean, reconstructedPublicPath);

        assertTrue(reconstructedAllocation - carrierAllocation >= MIN_AVOIDED_DUPLICATE_ALLOCATION,
                   () -> "trusted cursor carrier did not avoid the duplicate public snapshot: carrier="
                           + carrierAllocation + ", reconstructed=" + reconstructedAllocation);
        CursorPageNormalizer.NormalizedCursorPage normalized = CursorPageNormalizer.normalize(form, page);
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        var query = renderer.protection().prepareQuery(
                form, form, ConditionGroup.and().build(), DataScope.none());
        SqlRequest request = renderer.protection().select(query, normalized);
        String firstLabel = assertInstanceOf(String.class, request.parameters().get(0));
        String repeatedLabel = assertInstanceOf(String.class, request.parameters().get(1));

        assertEquals(expectedText, firstLabel);
        assertEquals(expectedText, repeatedLabel);
        assertEquals(42L, request.parameters().get(2));
    }

    @Test
    void cursorRenderingAndResultsKeepExplicitProductionSemantics() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.primaryKey("event_id", "BIGINT"))
                                      .addField(DynamicField.of("sequence", "BIGINT").withNullable(false))
                                      .build();
        CursorPageQuery page = CursorPageQuery.after(
                2, List.of(10L, 5L), CursorSort.asc("sequence"));
        CursorPageNormalizer.NormalizedCursorPage normalized = CursorPageNormalizer.normalize(form, page);
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());

        SqlRequest request = renderer.select(form, ConditionGroup.and().build(), page);

        assertEquals("select \"event_id\", \"sequence\" from \"events\" "
                             + "where (\"sequence\" > ? or (\"sequence\" = ? and \"event_id\" > ?)) "
                             + "order by \"sequence\" asc, \"event_id\" asc limit ? offset ?",
                     request.sql());
        assertEquals(List.of(10L, 10L, 5L, 3, 0L), request.parameters());

        List<DynamicRow> rows = List.of(
                DynamicRow.copyOf(Map.of("event_id", 6L, "sequence", 11L)),
                DynamicRow.copyOf(Map.of("event_id", 7L, "sequence", 12L)),
                DynamicRow.copyOf(Map.of("event_id", 8L, "sequence", 13L)));
        CursorPageResult<DynamicRow> result = FormCursorResults.from(rows, normalized);

        assertEquals(rows.subList(0, 2), result.rows());
        assertTrue(result.hasMore());
        assertEquals(List.of(12L, 7L), result.nextCursor());
    }

    private static long allocatedBytes(com.sun.management.ThreadMXBean allocationBean,
                                       Supplier<Object> operation) {
        Object[] results = new Object[ALLOCATION_ITERATIONS];
        long threadId = Thread.currentThread().threadId();
        long before = allocationBean.getThreadAllocatedBytes(threadId);
        for (int iteration = 0; iteration < ALLOCATION_ITERATIONS; iteration++) {
            results[iteration] = operation.get();
        }
        long allocated = allocationBean.getThreadAllocatedBytes(threadId) - before;
        assertNotNull(results[ALLOCATION_ITERATIONS - 1]);
        return allocated / ALLOCATION_ITERATIONS;
    }
}
