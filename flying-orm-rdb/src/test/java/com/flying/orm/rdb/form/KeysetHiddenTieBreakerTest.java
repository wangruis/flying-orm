package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetPageResult;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.NullOrder;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.time.OffsetTime;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;

class KeysetHiddenTieBreakerTest {

    @Test
    void cursorOnlyColumnsAreAliasedAndRemovedBeforePublishingTheBusinessRow() {
        DynamicForm form = DynamicForm.builder("events", "events")
                .addField(DynamicField.of("payload", "VARCHAR"))
                .addField(DynamicField.of("created_at", "TIMESTAMP").withNullable(false))
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .build();
        KeysetPageNormalizer.NormalizedKeysetPage page = KeysetPageNormalizer.normalize(
                form,
                KeysetPageQuery.first(
                        20, KeysetSort.desc("created_at", NullOrder.LAST)));
        HiddenProjectionLayout layout = HiddenProjectionLayout.of(List.of("payload"), page);

        List<HiddenProjectionLayout.Projection> hidden = layout.selections().stream()
                .filter(HiddenProjectionLayout.Projection::hidden)
                .toList();
        Map<String, Object> physical = new LinkedHashMap<>();
        physical.put("payload", "business");
        physical.put(hidden.get(0).label(), "2026-09-03T00:00:00");
        physical.put(hidden.get(1).label(), 9L);
        DynamicRow physicalRow = DynamicRow.copyOf(physical);

        assertEquals(List.of("2026-09-03T00:00:00", 9L),
                     layout.nextPosition(physicalRow).values());
        DynamicRow visible = layout.visibleRow(physicalRow);
        assertEquals(Map.of("payload", "business"), visible);
        assertFalse(visible.containsKey(hidden.get(0).label()));
        assertFalse(visible.containsKey(hidden.get(1).label()));
    }

    @Test
    void noHiddenColumnsReturnsTheOriginalRowWithoutAllocation() {
        DynamicForm form = DynamicForm.builder("events", "events")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .build();
        KeysetPageNormalizer.NormalizedKeysetPage page = KeysetPageNormalizer.normalize(
                form,
                KeysetPageQuery.first(20, KeysetSort.asc("id", NullOrder.FIRST)));
        HiddenProjectionLayout layout = HiddenProjectionLayout.of(List.of("id"), page);
        DynamicRow row = DynamicRow.copyOf(Map.of("id", 1L));

        assertSame(row, layout.visibleRow(row));
        assertSame(row, layout.logicalRowForDecoding(row));
        assertSame(row, layout.physicalRowAfterDecoding(row));
        assertEquals(List.of(1L), layout.nextPosition(row).values());
    }

    @Test
    void duplicateCallerSortDoesNotReclassifyTheAppendedTieBreaker() {
        DynamicForm form = DynamicForm.builder("events", "events")
                .addField(DynamicField.of("created_at", "TIMESTAMP").withNullable(false))
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .build();
        KeysetSort sort = KeysetSort.asc("created_at", NullOrder.LAST);

        KeysetPageNormalizer.NormalizedKeysetPage page = KeysetPageNormalizer.normalize(
                form, KeysetPageQuery.first(20, sort, sort));

        assertEquals(1, page.callerSortCount());
        assertEquals(List.of("id"), page.hiddenTieBreakers());
    }

    @Test
    void syncAndReactiveFacadesPublishTheSameVisibleRowsAndTypedPosition() {
        DynamicForm form = DynamicForm.builder("events", "events")
                .addField(DynamicField.of("payload", "VARCHAR"))
                .addField(DynamicField.of("created_at", "TIME(6) WITH TIME ZONE").withNullable(false))
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .build();
        QuerySpec query = QuerySpec.of(form, ConditionGroup.and().build())
                                   .withProjection(List.of("payload"), List.of());
        KeysetPageQuery page = KeysetPageQuery.first(
                2, KeysetSort.asc("created_at", NullOrder.LAST));
        List<DynamicRow> physicalRows = List.of(
                physical("one", "00:00:00+08:00", 1L),
                physical("two", "00:01:00+08:00", 2L),
                physical("three", "00:02:00+08:00", 3L));
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());

        KeysetPageResult<DynamicRow> sync = SyncFormClient.create(
                syncExecutor(physicalRows), syncBatchExecutor(), renderer).keysetPage(query, page);
        KeysetPageResult<DynamicRow> reactive = ReactiveFormClient.create(
                reactiveExecutor(physicalRows), renderer).keysetPage(query, page).block();

        assertEquals(List.of(Map.of("payload", "one"), Map.of("payload", "two")), sync.rows());
        assertEquals(sync, reactive);
        assertEquals(List.of(OffsetTime.parse("00:01:00+08:00"), 2L),
                     sync.nextPosition().values());
        assertFalse(sync.rows().getFirst().containsKey("__fo_ks_0"));
        assertFalse(sync.rows().getFirst().containsKey("__fo_ks_1"));
    }

    @Test
    void typedMappingKeepsTheExactKeysetPosition() {
        KeysetPageResult<DynamicRow> source = new KeysetPageResult<>(
                List.of(DynamicRow.copyOf(Map.of("id", 7L))),
                com.flying.orm.core.page.CursorPosition.of(List.of(7L)), true);

        KeysetPageResult<Long> mapped = FormResultMappingSupport.mapKeysetPage(
                source, row -> (Long) row.get("id"));

        assertEquals(List.of(7L), mapped.rows());
        assertSame(source.nextPosition(), mapped.nextPosition());
    }

    private static DynamicRow physical(String payload, String createdAt, long id) {
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("payload", payload);
        values.put("__fo_ks_0", createdAt);
        values.put("__fo_ks_1", id);
        return DynamicRow.copyOf(values);
    }

    private static SyncSqlExecutor syncExecutor(List<DynamicRow> rows) {
        return (SyncSqlExecutor) Proxy.newProxyInstance(
                SyncSqlExecutor.class.getClassLoader(), new Class<?>[]{SyncSqlExecutor.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("query")) {
                        return rows;
                    }
                    throw new UnsupportedOperationException(method.toString());
                });
    }

    private static SyncBatchExecutor syncBatchExecutor() {
        return (SyncBatchExecutor) Proxy.newProxyInstance(
                SyncBatchExecutor.class.getClassLoader(), new Class<?>[]{SyncBatchExecutor.class},
                (proxy, method, arguments) -> {
                    throw new UnsupportedOperationException(method.toString());
                });
    }

    private static ReactiveSqlExecutor reactiveExecutor(List<DynamicRow> rows) {
        return new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(com.flying.orm.core.sql.render.SqlRequest request) {
                return Flux.fromIterable(rows);
            }

            @Override
            public Mono<Long> rowsUpdated(com.flying.orm.core.sql.render.SqlRequest request) {
                return Mono.error(new UnsupportedOperationException());
            }
        };
    }
}
