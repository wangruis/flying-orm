package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import com.flying.orm.rdb.dialect.PaginationDialect;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.plan.StructuralPlanCaches;
import com.flying.orm.rdb.lock.ReadLock;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FormQueryRendererConvergenceTest {
    private static SqlRequest keyset(FormQuerySqlRenderer renderer, DynamicForm form,
                                     ConditionGroup where, KeysetPageQuery page) {
        return keyset(renderer, form, where, form.fields().stream().map(DynamicField::name).toList(),
                KeysetPageNormalizer.normalize(form, page));
    }

    private static SqlRequest keyset(FormQuerySqlRenderer renderer, DynamicForm form,
                                     ConditionGroup where, List<String> fields,
                                     KeysetPageNormalizer.NormalizedKeysetPage page) {
        return renderer.selectKeyset(form, where, HiddenProjectionLayout.of(fields, page), page);
    }

    private static final ConditionGroup EMPTY = ConditionGroup.and().build();

    @Test
    void fiveDialectsKeepOffsetAndProjectedSqlAndParameterOrder() {
        for (RdbDialect dialect : dialects()) {
            for (boolean cacheEnabled : List.of(true, false)) {
                OrmCachePolicy policy = cacheEnabled ? OrmCachePolicy.safeDefaults()
                        : OrmCachePolicy.builder().sqlPlans(CacheRegionPolicy.disabled()).build();
                Fixture fixture = fixture(dialect, policy, dialect.pagination());
                String id = dialect.schema().identifier("id");
                String title = dialect.schema().identifier("title");
                String table = dialect.schema().identifier("events");
                String hidden = dialect.schema().identifier("hidden_index");
                String base = "select " + id + ", " + title + ", " + hidden + " from " + table;
                PageQuery page = PageQuery.of(2, 3, PageSort.asc("id"));
                SqlRequest offset = fixture.renderer.select(fixture.form, EMPTY,
                        null,
                        List.of(),
                        page.sorts(), page,
                        false, null, null);
                SqlRequest physical = fixture.renderer.select(fixture.form, EMPTY,
                        List.of("title", "id"),
                        List.of(),
                        page.sorts(), page,
                        false, null, null);
                String tail = dialect.name().equals("oracle") || dialect.name().equals("sqlserver")
                        ? " offset ? rows fetch next ? rows only" : " limit ? offset ?";
                List<Object> parameters = dialect.name().equals("oracle") || dialect.name().equals("sqlserver")
                        ? List.of(3L, 3) : List.of(3, 3L);
                assertEquals(base + " order by " + id + " asc" + tail, offset.sql(), dialect.name());
                assertEquals(parameters, offset.parameters(), dialect.name());
                assertEquals("select " + title + ", " + id + " from " + table
                        + " order by " + id + " asc" + tail, physical.sql(), dialect.name());
                assertEquals(parameters, physical.parameters(), dialect.name());

                SqlRequest projected = fixture.renderer.select(fixture.form, EMPTY,
                        List.of("title", "id"),
                        List.of("title"),
                        List.of(PageSort.desc("id")), null,
                        false, null, null);
                assertEquals("select " + title + ", " + id + " from " + table
                        + " group by " + title + " order by " + id + " desc", projected.sql(), dialect.name());
                assertEquals(List.of(), projected.parameters(), dialect.name());

                SqlRequest locked = fixture.renderer.select(fixture.form, EMPTY,
                        List.of("id"),
                        List.of(),
                        List.of(), null, false,
                        dialect.lockingReadDialect(), ReadLock.updateNowait());
                String lockedSql = dialect.name().equals("sqlserver")
                        ? "select " + id + " from " + table + " WITH (UPDLOCK, ROWLOCK, NOWAIT)"
                        : "select " + id + " from " + table + " FOR UPDATE NOWAIT";
                assertEquals(lockedSql, locked.sql(), dialect.name());
                assertEquals(List.of(), locked.parameters(), dialect.name());

                SqlRequest cursorFirst = keyset(fixture.renderer, fixture.form, EMPTY, KeysetPageQuery.first(2, KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST)));
                assertEquals(base + " order by " + id + " asc" + tail,
                        cursorFirst.sql(), dialect.name());
                assertEquals(dialect.name().equals("oracle") || dialect.name().equals("sqlserver")
                        ? List.of(0L, 3) : List.of(3, 0L), cursorFirst.parameters(), dialect.name());
                SqlRequest cursorAfter = keyset(fixture.renderer, fixture.form, EMPTY, List.of("title"), KeysetPageNormalizer.normalize(fixture.form, KeysetPageQuery.after(2, com.flying.orm.core.page.CursorPosition.of(List.of(7L)), KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST))));
                assertEquals("select " + title + ", " + id + " as " + dialect.schema().identifier("__fo_ks_0")
                        + " from " + table + " where " + id
                        + " > ? order by " + id + " asc" + tail, cursorAfter.sql(), dialect.name());
                assertEquals(dialect.name().equals("oracle") || dialect.name().equals("sqlserver")
                        ? List.of(7L, 0L, 3) : List.of(7L, 3, 0L),
                        cursorAfter.parameters(), dialect.name());

                List<SqlRequest> first = List.of(offset, physical, projected, locked,
                                                 cursorFirst, cursorAfter);
                assertEquals(cacheEnabled ? 6 : 0, fixture.caches.sqlSnapshot().missCount(),
                             dialect.name());
                List<SqlRequest> repeated = List.of(
                        fixture.renderer.select(fixture.form, EMPTY, null, List.of(), page.sorts(), page, false, null, null),
                        fixture.renderer.select(fixture.form, EMPTY,
                                List.of("title", "id"),
                                List.of(),
                                page.sorts(), page,
                                false, null, null),
                        fixture.renderer.select(fixture.form, EMPTY,
                                List.of("title", "id"),
                                List.of("title"),
                                List.of(PageSort.desc("id")), null,
                                false, null, null),
                        fixture.renderer.select(fixture.form, EMPTY,
                                List.of("id"),
                                List.of(),
                                List.of(), null, false,
                                dialect.lockingReadDialect(), ReadLock.updateNowait()),
                        keyset(fixture.renderer, fixture.form, EMPTY, KeysetPageQuery.first(2, KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST))),
                        keyset(fixture.renderer, fixture.form, EMPTY, List.of("title"), KeysetPageNormalizer.normalize(fixture.form, KeysetPageQuery.after(2, com.flying.orm.core.page.CursorPosition.of(List.of(7L)), KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST)))));
                for (int index = 0; index < first.size(); index++) {
                    assertEquals(first.get(index).sql(), repeated.get(index).sql(), dialect.name());
                    assertEquals(first.get(index).parameters(), repeated.get(index).parameters(), dialect.name());
            }
            assertEquals(cacheEnabled ? 6 : 0, fixture.caches.sqlSnapshot().hitCount(),
                         dialect.name());
            assertEquals(cacheEnabled ? 6 : 0, fixture.caches.sqlSnapshot().missCount(),
                         dialect.name());
            }
        }
    }

    @Test
    void equivalentProjectionsShareSqlPlansButLockingRemainsDistinct() {
        Fixture fixture = fixture(RdbDialect.postgresql(), OrmCachePolicy.safeDefaults(),
                                  RdbDialect.postgresql().pagination());
        PageQuery page = PageQuery.of(1, 2, PageSort.asc("id"));
        SqlRequest ordinary = fixture.renderer.select(fixture.form, EMPTY,
                null,
                List.of(),
                page.sorts(), page,
                false, null, null);
        assertEquals(1, fixture.caches.sqlSnapshot().missCount());
        assertEquals(ordinary.sql(), fixture.renderer.select(fixture.form, EMPTY,
                null,
                List.of(),
                page.sorts(), page,
                false, null, null).sql());
        assertEquals(1, fixture.caches.sqlSnapshot().hitCount());
        SqlRequest physicalAll = fixture.renderer.select(fixture.form, EMPTY,
                List.of("id", "title", "hidden_index"),
                List.of(),
                page.sorts(), page,
                false, null, null);
        assertEquals(ordinary.sql(), physicalAll.sql());
        assertEquals(1, fixture.caches.sqlSnapshot().missCount());
        assertEquals(physicalAll.sql(), fixture.renderer.select(fixture.form, EMPTY,
                List.of("id", "title", "hidden_index"),
                List.of(),
                page.sorts(), page,
                false, null, null).sql());
        assertEquals(3, fixture.caches.sqlSnapshot().hitCount());
        fixture.renderer.select(fixture.form, EMPTY, List.of("id"), List.of(), List.of(), null, false, null, null);
        fixture.renderer.select(fixture.form, EMPTY,
                List.of("id"),
                List.of(),
                List.of(), null, false,
                RdbDialect.postgresql().lockingReadDialect(), ReadLock.update());
        assertEquals(3, fixture.caches.sqlSnapshot().missCount());
        KeysetPageQuery cursor = KeysetPageQuery.first(2, KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST));
        SqlRequest cursorAll = keyset(fixture.renderer, fixture.form, EMPTY, cursor);
        assertEquals(cursorAll.sql(), keyset(fixture.renderer, fixture.form, EMPTY, List.of("id", "title", "hidden_index"), KeysetPageNormalizer.normalize(fixture.form, cursor)).sql());
        assertEquals(4, fixture.caches.sqlSnapshot().missCount());
    }

    @Test
    void physicalFieldSnapshotIsNarrowReorderedAndNeverIncludesHiddenColumns() {
        Fixture fixture = fixture(RdbDialect.postgresql(), OrmCachePolicy.safeDefaults(),
                                  RdbDialect.postgresql().pagination());
        List<String> visible = new ArrayList<>(List.of("title", "id"));
        SqlRequest request = fixture.renderer.select(fixture.form, EMPTY, visible,
                List.of(),
                PageQuery.of(1, 2, PageSort.asc("id")).sorts(),
                PageQuery.of(1, 2, PageSort.asc("id")),
                false, null, null);
        visible.set(0, "hidden_index");
        assertTrue(request.sql().startsWith("select \"title\", \"id\" from \"events\""));
        assertFalse(request.sql().contains("hidden_index"));
        SqlRequest cursor = keyset(fixture.renderer, fixture.form, EMPTY, List.of("title"), KeysetPageNormalizer.normalize(fixture.form, KeysetPageQuery.first(2, KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST))));
        assertTrue(cursor.sql().startsWith("select \"title\", \"id\" as \"__fo_ks_0\" from \"events\""));
        assertFalse(cursor.sql().contains("hidden_index"));
    }

    @Test
    void keysetKeepsFirstAfterAscendingDescendingAndExtraRowBudget() {
        Fixture fixture = fixture(RdbDialect.postgresql(), OrmCachePolicy.safeDefaults(),
                                  RdbDialect.postgresql().pagination());
        SqlRequest first = keyset(fixture.renderer, fixture.form, EMPTY, KeysetPageQuery.first(2, KeysetSort.desc("title", com.flying.orm.core.page.NullOrder.LAST)));
        assertEquals("select \"id\", \"title\", \"hidden_index\" from \"events\" "
                + "order by \"title\" desc, \"id\" desc limit ? offset ?", first.sql());
        assertEquals(List.of(3, 0L), first.parameters());

        SqlRequest after = keyset(fixture.renderer, fixture.form, ConditionGroup.and().where("id", ">", 1L).build(), KeysetPageQuery.after(2, com.flying.orm.core.page.CursorPosition.of(List.of("m", 7L)), KeysetSort.asc("title", com.flying.orm.core.page.NullOrder.LAST), KeysetSort.desc("id", com.flying.orm.core.page.NullOrder.LAST)));
        assertEquals("select \"id\", \"title\", \"hidden_index\" from \"events\" "
                + "where (\"id\" > ?) and ((\"title\" > ? OR (\"title\" = ? AND \"id\" < ?))) "
                + "order by \"title\" asc, \"id\" desc limit ? offset ?", after.sql());
        assertEquals(List.of(1L, "m", "m", 7L, 3, 0L), after.parameters());
        SqlRequest protectedAfter = keyset(fixture.renderer, fixture.form, EMPTY, List.of("title"), KeysetPageNormalizer.normalize(fixture.form, KeysetPageQuery.after(2, com.flying.orm.core.page.CursorPosition.of(List.of("m", 7L)), KeysetSort.asc("title", com.flying.orm.core.page.NullOrder.LAST), KeysetSort.desc("id", com.flying.orm.core.page.NullOrder.LAST))));
        assertEquals("select \"title\", \"id\" as \"__fo_ks_0\" from \"events\" "
                + "where (\"title\" > ? OR (\"title\" = ? AND \"id\" < ?)) "
                + "order by \"title\" asc, \"id\" desc limit ? offset ?", protectedAfter.sql());
        assertEquals(List.of("m", "m", 7L, 3, 0L), protectedAfter.parameters());
    }

    @Test
    void disablingSqlPlansCompilesEachRequestAndPaginationCallbacksStayLazy() {
        RdbDialect dialect = RdbDialect.postgresql();
        OrmCachePolicy disabled = OrmCachePolicy.builder()
                .sqlPlans(CacheRegionPolicy.disabled()).build();
        CountingPagination pagination = new CountingPagination(dialect.pagination());
        Fixture fixture = fixture(dialect, disabled, pagination);
        PageQuery page = PageQuery.of(1, 2, PageSort.asc("id"));
        fixture.renderer.select(fixture.form, EMPTY, null, List.of(), page.sorts(), page, false, null, null);
        fixture.renderer.select(fixture.form, EMPTY, null, List.of(), page.sorts(), page, false, null, null);
        assertEquals(2, pagination.parameters.get());
        assertEquals(2, pagination.paginate.get());
        assertEquals(0, fixture.caches.sqlSnapshot().requestCount());

        CountingPagination cachedPagination = new CountingPagination(dialect.pagination());
        Fixture cached = fixture(dialect, OrmCachePolicy.safeDefaults(), cachedPagination);
        cached.renderer.select(cached.form, EMPTY, null, List.of(), page.sorts(), page, false, null, null);
        cached.renderer.select(cached.form, EMPTY, null, List.of(), page.sorts(), page, false, null, null);
        cached.renderer.select(cached.form, EMPTY, List.of("id"), List.of(), page.sorts(), page, false, null, null);
        cached.renderer.select(cached.form, EMPTY, List.of("id"), List.of(), page.sorts(), page, false, null, null);
        keyset(cached.renderer, cached.form, EMPTY, KeysetPageQuery.first(2, KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST)));
        keyset(cached.renderer, cached.form, EMPTY, KeysetPageQuery.first(2, KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST)));
        assertEquals(6, cachedPagination.parameters.get());
        assertEquals(3, cachedPagination.paginate.get());
        assertEquals(3, cached.caches.sqlSnapshot().missCount());
        assertEquals(3, cached.caches.sqlSnapshot().hitCount());
    }

    @Test
    void extensionTermAndValueCodecCallbacksKeepTheirPerRequestCount() {
        RdbDialect dialect = RdbDialect.postgresql();
        AtomicInteger termCalls = new AtomicInteger();
        CountingStringCodec codec = new CountingStringCodec();
        SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms()
                .addTerm(SqlTermHandler.of("fixed", (term, context) -> {
                    termCalls.incrementAndGet();
                    return SqlFragment.of("1 = 1");
                }))
                .valueCodecs(ValueCodecRegistry.standard().withFirst(codec)).build()
                .withIdentifierRenderer(dialect.schema()::identifier);
        StructuralPlanCaches caches = StructuralPlanCaches.create(OrmCachePolicy.safeDefaults());
        CountingPagination pagination = new CountingPagination(dialect.pagination());
        FormSqlRenderSupport support = new FormSqlRenderSupport(conditions, dialect.json(),
                dialect.name(), true, dialect.schema()::identifier, caches, Map.of(),
                dialect.capabilities(), dialect.schema()::relationIdentifier);
        FormQuerySqlRenderer renderer = new FormQuerySqlRenderer(support, pagination);
        DynamicForm form = fixture(dialect, OrmCachePolicy.safeDefaults(),
                                   dialect.pagination()).form;
        ConditionGroup extension = conditions.conditions().where("id", "fixed", 0).build();
        SqlRequest firstTerm = renderer.select(form, extension,
                null,
                List.of(),
                PageQuery.of(1, 2, PageSort.asc("id")).sorts(),
                PageQuery.of(1, 2, PageSort.asc("id")),
                false, null, null);
        SqlRequest secondTerm = renderer.select(form, extension,
                null,
                List.of(),
                PageQuery.of(1, 2, PageSort.asc("id")).sorts(),
                PageQuery.of(1, 2, PageSort.asc("id")),
                false, null, null);
        assertEquals(firstTerm.sql(), secondTerm.sql());
        assertEquals(List.of(2, 0L), firstTerm.parameters());
        assertEquals(2, termCalls.get());
        assertEquals(2, pagination.parameters.get());
        assertEquals(2, pagination.paginate.get());

        ConditionGroup where = ConditionGroup.and().where("title", "=", "a").build();
        SqlRequest cursor = keyset(renderer, form, where, KeysetPageQuery.after(2, com.flying.orm.core.page.CursorPosition.of(List.of("b", 7L)), KeysetSort.asc("title", com.flying.orm.core.page.NullOrder.LAST)));
        SqlRequest cursorAgain = keyset(renderer, form, where, KeysetPageQuery.after(2, com.flying.orm.core.page.CursorPosition.of(List.of("b", 7L)), KeysetSort.asc("title", com.flying.orm.core.page.NullOrder.LAST)));
        assertEquals(cursor.sql(), cursorAgain.sql());
        assertEquals(List.of("encoded:a", "encoded:b", "encoded:b", 7L, 3, 0L),
                     cursor.parameters());
        assertEquals(cursor.parameters(), cursorAgain.parameters());
        assertEquals(6, codec.writes.get());
        assertEquals(4, pagination.parameters.get());
        assertEquals(3, pagination.paginate.get());
    }

    @Test
    void normalizedSelectionValidatesModelFieldsAndLockDialect() {
        Fixture fixture = fixture(RdbDialect.postgresql(), OrmCachePolicy.safeDefaults(),
                                  RdbDialect.postgresql().pagination());
        assertThrows(NullPointerException.class, () -> fixture.renderer.select(
                null, EMPTY, null, List.of(), List.of(), null, false, null, null));
        assertThrows(IllegalArgumentException.class, () -> fixture.renderer.select(
                fixture.form, EMPTY, List.of(), List.of(), List.of(), null, false, null, null));
        assertThrows(IllegalArgumentException.class, () -> fixture.renderer.select(
                fixture.form, EMPTY, List.of("id"), List.of(), List.of(PageSort.asc("missing")),
                null, false, null, null));
        assertThrows(NullPointerException.class, () -> fixture.renderer.select(
                fixture.form, EMPTY, List.of("id"), List.of(), List.of(), null, false, null, ReadLock.update()));
        assertNotEquals("", fixture.renderer.select(fixture.form, EMPTY,
                List.of("id"), List.of(), List.of(), null, false, null, null).sql());
    }

    private static List<RdbDialect> dialects() {
        return List.of(RdbDialect.h2(), RdbDialect.mysql(), RdbDialect.postgresql(),
                       RdbDialect.oracle(), RdbDialect.sqlServer());
    }

    private static Fixture fixture(RdbDialect dialect, OrmCachePolicy policy,
                                   PaginationDialect pagination) {
        StructuralPlanCaches caches = StructuralPlanCaches.create(policy);
        SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms().build()
                .withIdentifierRenderer(dialect.schema()::identifier);
        FormSqlRenderSupport support = new FormSqlRenderSupport(conditions, dialect.json(),
                dialect.name(), true, dialect.schema()::identifier, caches, Map.of(),
                dialect.capabilities(), dialect.schema()::relationIdentifier);
        DynamicForm form = DynamicForm.builder("events", "events")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("title", "VARCHAR").withNullable(false))
                .addField(DynamicField.of("hidden_index", "VARCHAR").withNullable(false))
                .build();
        return new Fixture(new FormQuerySqlRenderer(support, pagination), form, caches);
    }

    private record Fixture(FormQuerySqlRenderer renderer, DynamicForm form,
                           StructuralPlanCaches caches) {
    }

    private static final class CountingPagination implements PaginationDialect {
        private final PaginationDialect delegate;
        private final AtomicInteger parameters = new AtomicInteger();
        private final AtomicInteger paginate = new AtomicInteger();

        private CountingPagination(PaginationDialect delegate) {
            this.delegate = delegate;
        }

        @Override
        public SqlRequest paginate(String sql, List<Object> values, PageQuery page) {
            paginate.incrementAndGet();
            return delegate.paginate(sql, values, page);
        }

        @Override
        public List<Object> paginationParameters(List<Object> values, PageQuery page) {
            parameters.incrementAndGet();
            return delegate.paginationParameters(values, page);
        }
    }

    private static final class CountingStringCodec implements ValueCodec {
        private final AtomicInteger writes = new AtomicInteger();

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == String.class;
        }

        @Override
        public Object write(Object value) {
            writes.incrementAndGet();
            return "encoded:" + value;
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            return value;
        }
    }
}
