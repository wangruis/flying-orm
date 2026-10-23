package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetPageResult;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import java.util.function.Function;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class CursorPageOwnershipConvergenceTest {

    @Test
    void sharedSeekRenderingKeepsSqlAndCodecParameterOrderTogether() {
        DynamicForm form = DynamicForm.builder("events", "events")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("score", "INTEGER").withNullable(false)).build();
        KeysetPageNormalizer.NormalizedKeysetPage page = KeysetPageNormalizer.normalize(form,
                KeysetPageQuery.after(2, com.flying.orm.core.page.CursorPosition.of(List.of(7, 3L)), KeysetSort.desc("score", com.flying.orm.core.page.NullOrder.LAST)));
        List<String> encoded = new ArrayList<>();
        var predicate = FormKeysetSqlRenderer.cursorPredicate(page, field -> "t." + field,
                (field, value) -> {
                    encoded.add(field);
                    return value;
                });
        assertEquals("(t.score < ? OR (t.score = ? AND t.id < ?))", predicate.sql());
        assertEquals(List.of(7, 7, 3L), predicate.parameters());
        assertEquals(List.of("score", "score", "id"), encoded);
        assertEquals(predicate.sql(), FormKeysetSqlRenderer.cursorSql(page, field -> "t." + field));
        List<Object> parameters = new ArrayList<>();
        FormKeysetSqlRenderer.appendCursorParameters(page, (field, value) -> value, parameters);
        assertEquals(predicate.parameters(), parameters);
        var first = KeysetPageNormalizer.normalize(form, KeysetPageQuery.first(2, KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST)));
        assertEquals("", FormKeysetSqlRenderer.cursorPredicate(first,
                field -> { throw new AssertionError("first page has no predicate"); },
                (field, value) -> { throw new AssertionError("first page has no cursor bindings"); }).sql());
    }

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
        KeysetPageQuery page = KeysetPageQuery.after(
                20,
                com.flying.orm.core.page.CursorPosition.of(List.of(cursorText, 42L)),
                KeysetSort.asc("label", com.flying.orm.core.page.NullOrder.LAST));
        cursorText[0] = 'y';
        List<KeysetSort> normalizedSorts = List.of(
                KeysetSort.asc("label", com.flying.orm.core.page.NullOrder.LAST), KeysetSort.asc("event_id", com.flying.orm.core.page.NullOrder.LAST));
        Supplier<Object> carrierPath = () -> KeysetPageNormalizer.normalize(form, page);
        Supplier<Object> reconstructedPublicPath = () -> new KeysetPageQuery(
                page.size(), normalizedSorts, com.flying.orm.core.page.CursorPosition.of(page.position().values()));
        for (int iteration = 0; iteration < 100; iteration++) {
            carrierPath.get();
            reconstructedPublicPath.get();
        }

        long carrierAllocation = allocatedBytes(allocationBean, carrierPath);
        long reconstructedAllocation = allocatedBytes(allocationBean, reconstructedPublicPath);

        assertTrue(reconstructedAllocation - carrierAllocation >= MIN_AVOIDED_DUPLICATE_ALLOCATION,
                   () -> "trusted cursor carrier did not avoid the duplicate public snapshot: carrier="
                           + carrierAllocation + ", reconstructed=" + reconstructedAllocation);
        KeysetPageNormalizer.NormalizedKeysetPage normalized = KeysetPageNormalizer.normalize(form, page);
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        var query = renderer.protection().prepareQuery(
                form, ConditionGroup.and().build(), DataScope.none(), List.of("event_id", "label"));
        SqlRequest request = renderer.queryRenderer.selectKeyset(
                query.physicalForm(), query.where(), HiddenProjectionLayout.of(query.visibleFields(), normalized), normalized);
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
        KeysetPageQuery page = KeysetPageQuery.after(
                2, com.flying.orm.core.page.CursorPosition.of(List.of(10L, 5L)), KeysetSort.asc("sequence", com.flying.orm.core.page.NullOrder.LAST));
        KeysetPageNormalizer.NormalizedKeysetPage normalized = KeysetPageNormalizer.normalize(form, page);
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());

        SqlRequest request = renderer.select(form, ConditionGroup.and().build(), page);

        assertEquals("select \"event_id\", \"sequence\" from \"events\" "
                             + "where (\"sequence\" > ? OR (\"sequence\" = ? AND \"event_id\" > ?)) "
                             + "order by \"sequence\" asc, \"event_id\" asc limit ? offset ?",
                     request.sql());
        assertEquals(List.of(10L, 10L, 5L, 3, 0L), request.parameters());

        List<DynamicRow> rows = List.of(
                DynamicRow.copyOf(Map.of("event_id", 6L, "sequence", 11L)),
                DynamicRow.copyOf(Map.of("event_id", 7L, "sequence", 12L)),
                DynamicRow.copyOf(Map.of("event_id", 8L, "sequence", 13L)));
        KeysetPageResult<DynamicRow> result = finish(normalized, rows, Function.identity());

        assertEquals(rows.subList(0, 2), result.rows());
        assertTrue(result.hasMore());
        assertEquals(List.of(12L, 7L), result.nextPosition().values());
    }

    @Test
    void normalizedPageOwnsAnImmutableSliceAndCaseInsensitiveCursor() {
        KeysetPageNormalizer.NormalizedKeysetPage page = singleKeyPage();
        List<DynamicRow> rows = new ArrayList<>(List.of(
                DynamicRow.copyOf(Map.of("ID", 1L)), DynamicRow.copyOf(Map.of("ID", 2L))));
        KeysetPageResult<DynamicRow> result = finish(page, rows, Function.identity());
        rows.clear();
        assertEquals(1, result.rows().size());
        assertEquals(List.of(1L), result.nextPosition().values());
        assertTrue(result.hasMore());
        assertThrows(UnsupportedOperationException.class, () -> result.rows().clear());
    }

    @Test
    void finalAndEmptyPagesDoNotRequireAnotherCursor() {
        KeysetPageNormalizer.NormalizedKeysetPage page = singleKeyPage();
        for (List<DynamicRow> rows : List.of(List.<DynamicRow>of(),
                List.of(DynamicRow.copyOf(Map.of("id", 1L))))) {
            KeysetPageResult<DynamicRow> result = finish(page, rows, Function.identity());
            assertEquals(rows, result.rows());
            assertFalse(result.hasMore());
            assertTrue(result.nextPosition().values().isEmpty());
        }
    }

    @Test
    void nextPositionRejectsMissingColumnsAndNonnullRequestRejectsNullValues() {
        KeysetPageNormalizer.NormalizedKeysetPage page = singleKeyPage();
        DynamicRow extra = DynamicRow.copyOf(Map.of("id", 2L));
        assertThrows(IllegalArgumentException.class,
                () -> finish(page, List.of(DynamicRow.copyOf(Map.of("other", 1L)), extra), Function.identity()));
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("ID", null);
        KeysetPageResult<DynamicRow> result = finish(page,
                List.of(DynamicRow.copyOf(values), extra), Function.identity());
        assertEquals(java.util.Collections.singletonList(null), result.nextPosition().values());
        DynamicForm form = DynamicForm.builder("events", "events")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        assertThrows(IllegalArgumentException.class, () -> KeysetPageNormalizer.normalize(form,
                KeysetPageQuery.after(1, result.nextPosition(),
                        KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST))));
    }

    private static KeysetPageNormalizer.NormalizedKeysetPage singleKeyPage() {
        DynamicForm form = DynamicForm.builder("events", "events")
                .addField(DynamicField.primaryKey("id", "BIGINT")).build();
        return KeysetPageNormalizer.normalize(form, KeysetPageQuery.first(1, KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST)));
    }

    @Test
    void finishesTypedRowsWithoutMappingTheLookaheadOrChangingTheCursor() {
        KeysetPageNormalizer.NormalizedKeysetPage page = singleKeyPage();
        List<Long> mappedIds = new ArrayList<>();
        KeysetPageResult<String> result = finish(page, List.of(
                DynamicRow.copyOf(Map.of("ID", 1L)), DynamicRow.copyOf(Map.of("ID", 2L))), row -> {
            mappedIds.add((Long) row.get("ID"));
            return "visible";
        });
        assertEquals(List.of("visible"), result.rows());
        assertEquals(List.of(1L), mappedIds);
        assertEquals(List.of(1L), result.nextPosition().values());
        assertTrue(result.hasMore());
        assertEquals(List.of(), finish(page, List.of(), row -> { throw new AssertionError(); }).rows());
    }

    @Test
    void snapshotsTheCursorBeforeTheResultMapperCanMutateItsValues() {
        byte[] id = {1};
        KeysetPageResult<String> result = finish(singleKeyPage(), List.of(
                DynamicRow.copyOf(Map.of("id", id)), DynamicRow.copyOf(Map.of("id", new byte[]{2}))), row -> {
            ((byte[]) row.get("id"))[0] = 9;
            return "mapped";
        });
        org.junit.jupiter.api.Assertions.assertArrayEquals(new byte[]{1}, (byte[]) result.nextPosition().values().getFirst());
    }

    private static <T> KeysetPageResult<T> finish(KeysetPageNormalizer.NormalizedKeysetPage page,
                                                 List<DynamicRow> rows, Function<DynamicRow, T> publisher) {
        return HiddenProjectionLayout.of(page.sorts().stream().map(KeysetSort::field).toList(), page)
                .finish(rows, page.size(), publisher);
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
