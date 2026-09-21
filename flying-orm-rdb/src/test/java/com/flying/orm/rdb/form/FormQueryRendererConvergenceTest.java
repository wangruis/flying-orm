package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorSort;
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
                SqlRequest offset = fixture.renderer.select(fixture.form, EMPTY, page);
                SqlRequest physical = fixture.renderer.selectPhysical(
                        fixture.form, List.of("title", "id"), EMPTY, page);
                String tail = dialect.name().equals("oracle") || dialect.name().equals("sqlserver")
                        ? " offset ? rows fetch next ? rows only" : " limit ? offset ?";
                List<Object> parameters = dialect.name().equals("oracle") || dialect.name().equals("sqlserver")
                        ? List.of(3L, 3) : List.of(3, 3L);
                assertEquals(base + " order by " + id + " asc" + tail, offset.sql(), dialect.name());
                assertEquals(parameters, offset.parameters(), dialect.name());
                assertEquals("select " + title + ", " + id + " from " + table
                        + " order by " + id + " asc" + tail, physical.sql(), dialect.name());
                assertEquals(parameters, physical.parameters(), dialect.name());

                SqlRequest projected = fixture.renderer.selectProjected(
                        fixture.form, EMPTY, List.of("title", "id"), List.of("title"),
                        List.of(PageSort.desc("id")));
                assertEquals("select " + title + ", " + id + " from " + table
                        + " group by " + title + " order by " + id + " desc", projected.sql(), dialect.name());
                assertEquals(List.of(), projected.parameters(), dialect.name());

                SqlRequest locked = fixture.renderer.selectProjectedLocking(
                        fixture.form, EMPTY, List.of("id"), List.of(), List.of(),
                        dialect.lockingReadDialect(), ReadLock.updateNowait());
                String lockedSql = dialect.name().equals("sqlserver")
                        ? "select " + id + " from " + table + " WITH (UPDLOCK, ROWLOCK, NOWAIT)"
                        : "select " + id + " from " + table + " FOR UPDATE NOWAIT";
                assertEquals(lockedSql, locked.sql(), dialect.name());
                assertEquals(List.of(), locked.parameters(), dialect.name());

                SqlRequest cursorFirst = fixture.renderer.select(fixture.form, EMPTY,
                        CursorPageQuery.first(2, CursorSort.asc("id")));
                assertEquals(base + " order by " + id + " asc" + tail,
                        cursorFirst.sql(), dialect.name());
                assertEquals(dialect.name().equals("oracle") || dialect.name().equals("sqlserver")
                        ? List.of(0L, 3) : List.of(3, 0L), cursorFirst.parameters(), dialect.name());
                SqlRequest cursorAfter = fixture.renderer.selectPhysical(fixture.form,
                        List.of("title"), EMPTY,
                        CursorPageQuery.after(2, List.of(7L), CursorSort.asc("id")));
                assertEquals("select " + title + " from " + table + " where (" + id
                        + " > ?) order by " + id + " asc" + tail, cursorAfter.sql(), dialect.name());
                assertEquals(dialect.name().equals("oracle") || dialect.name().equals("sqlserver")
                        ? List.of(7L, 0L, 3) : List.of(7L, 3, 0L),
                        cursorAfter.parameters(), dialect.name());

                List<SqlRequest> first = List.of(offset, physical, projected, locked,
                                                 cursorFirst, cursorAfter);
                assertEquals(cacheEnabled ? 6 : 0, fixture.caches.sqlSnapshot().missCount(),
                             dialect.name());
                List<SqlRequest> repeated = List.of(
                        fixture.renderer.select(fixture.form, EMPTY, page),
                        fixture.renderer.selectPhysical(fixture.form, List.of("title", "id"), EMPTY, page),
                        fixture.renderer.selectProjected(fixture.form, EMPTY,
                                List.of("title", "id"), List.of("title"), List.of(PageSort.desc("id"))),
                        fixture.renderer.selectProjectedLocking(fixture.form, EMPTY,
                                List.of("id"), List.of(), List.of(),
                                dialect.lockingReadDialect(), ReadLock.updateNowait()),
                        fixture.renderer.select(fixture.form, EMPTY,
                                CursorPageQuery.first(2, CursorSort.asc("id"))),
                        fixture.renderer.selectPhysical(fixture.form, List.of("title"), EMPTY,
                                CursorPageQuery.after(2, List.of(7L), CursorSort.asc("id"))));
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
    void sqlCacheSeparatesOrdinaryPhysicalAndLockingShapesOnColdAndHotRequests() {
        Fixture fixture = fixture(RdbDialect.postgresql(), OrmCachePolicy.safeDefaults(),
                                  RdbDialect.postgresql().pagination());
        PageQuery page = PageQuery.of(1, 2, PageSort.asc("id"));
        SqlRequest ordinary = fixture.renderer.select(fixture.form, EMPTY, page);
        assertEquals(1, fixture.caches.sqlSnapshot().missCount());
        assertEquals(ordinary.sql(), fixture.renderer.select(fixture.form, EMPTY, page).sql());
        assertEquals(1, fixture.caches.sqlSnapshot().hitCount());
        SqlRequest physicalAll = fixture.renderer.selectPhysical(
                fixture.form, List.of("id", "title", "hidden_index"), EMPTY, page);
        assertEquals(ordinary.sql(), physicalAll.sql());
        assertEquals(2, fixture.caches.sqlSnapshot().missCount());
        assertEquals(physicalAll.sql(), fixture.renderer.selectPhysical(
                fixture.form, List.of("id", "title", "hidden_index"), EMPTY, page).sql());
        assertEquals(2, fixture.caches.sqlSnapshot().hitCount());
        fixture.renderer.selectProjected(fixture.form, EMPTY, List.of("id"), List.of(), List.of());
        fixture.renderer.selectProjectedLocking(fixture.form, EMPTY, List.of("id"), List.of(),
                List.of(), RdbDialect.postgresql().lockingReadDialect(), ReadLock.update());
        assertEquals(4, fixture.caches.sqlSnapshot().missCount());
    }

    @Test
    void physicalFieldSnapshotIsNarrowReorderedAndNeverIncludesHiddenColumns() {
        Fixture fixture = fixture(RdbDialect.postgresql(), OrmCachePolicy.safeDefaults(),
                                  RdbDialect.postgresql().pagination());
        List<String> visible = new ArrayList<>(List.of("title", "id"));
        SqlRequest request = fixture.renderer.selectPhysical(
                fixture.form, visible, EMPTY, PageQuery.of(1, 2, PageSort.asc("id")));
        visible.set(0, "hidden_index");
        assertTrue(request.sql().startsWith("select \"title\", \"id\" from \"events\""));
        assertFalse(request.sql().contains("hidden_index"));
        SqlRequest cursor = fixture.renderer.selectPhysical(fixture.form, List.of("title"), EMPTY,
                CursorPageQuery.first(2, CursorSort.asc("id")));
        assertTrue(cursor.sql().startsWith("select \"title\" from \"events\""));
        assertFalse(cursor.sql().contains("hidden_index"));
    }

    @Test
    void legacyCursorKeepsFirstAfterAscendingDescendingAndExtraRowBudget() {
        Fixture fixture = fixture(RdbDialect.postgresql(), OrmCachePolicy.safeDefaults(),
                                  RdbDialect.postgresql().pagination());
        SqlRequest first = fixture.renderer.select(fixture.form, EMPTY,
                CursorPageQuery.first(2, CursorSort.desc("title")));
        assertEquals("select \"id\", \"title\", \"hidden_index\" from \"events\" "
                + "order by \"title\" desc, \"id\" desc limit ? offset ?", first.sql());
        assertEquals(List.of(3, 0L), first.parameters());

        SqlRequest after = fixture.renderer.select(fixture.form,
                ConditionGroup.and().where("id", ">", 1L).build(),
                CursorPageQuery.after(2, List.of("m", 7L), CursorSort.asc("title"),
                        CursorSort.desc("id")));
        assertEquals("select \"id\", \"title\", \"hidden_index\" from \"events\" "
                + "where (\"id\" > ?) and (\"title\" > ? or (\"title\" = ? and \"id\" < ?)) "
                + "order by \"title\" asc, \"id\" desc limit ? offset ?", after.sql());
        assertEquals(List.of(1L, "m", "m", 7L, 3, 0L), after.parameters());
        SqlRequest protectedAfter = fixture.renderer.selectPhysical(fixture.form, List.of("title"),
                EMPTY, CursorPageQuery.after(2, List.of("m", 7L),
                        CursorSort.asc("title"), CursorSort.desc("id")));
        assertEquals("select \"title\" from \"events\" "
                + "where (\"title\" > ? or (\"title\" = ? and \"id\" < ?)) "
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
        fixture.renderer.select(fixture.form, EMPTY, page);
        fixture.renderer.select(fixture.form, EMPTY, page);
        assertEquals(2, pagination.parameters.get());
        assertEquals(2, pagination.paginate.get());
        assertEquals(0, fixture.caches.sqlSnapshot().requestCount());

        CountingPagination cachedPagination = new CountingPagination(dialect.pagination());
        Fixture cached = fixture(dialect, OrmCachePolicy.safeDefaults(), cachedPagination);
        cached.renderer.select(cached.form, EMPTY, page);
        cached.renderer.select(cached.form, EMPTY, page);
        cached.renderer.selectPhysical(cached.form, List.of("id"), EMPTY, page);
        cached.renderer.selectPhysical(cached.form, List.of("id"), EMPTY, page);
        cached.renderer.select(cached.form, EMPTY,
                CursorPageQuery.first(2, CursorSort.asc("id")));
        cached.renderer.select(cached.form, EMPTY,
                CursorPageQuery.first(2, CursorSort.asc("id")));
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
                PageQuery.of(1, 2, PageSort.asc("id")));
        SqlRequest secondTerm = renderer.select(form, extension,
                PageQuery.of(1, 2, PageSort.asc("id")));
        assertEquals(firstTerm.sql(), secondTerm.sql());
        assertEquals(List.of(2, 0L), firstTerm.parameters());
        assertEquals(2, termCalls.get());
        assertEquals(2, pagination.parameters.get());
        assertEquals(2, pagination.paginate.get());

        ConditionGroup where = ConditionGroup.and().where("title", "=", "a").build();
        SqlRequest cursor = renderer.select(form, where,
                CursorPageQuery.after(2, List.of("b", 7L), CursorSort.asc("title")));
        SqlRequest cursorAgain = renderer.select(form, where,
                CursorPageQuery.after(2, List.of("b", 7L), CursorSort.asc("title")));
        assertEquals(cursor.sql(), cursorAgain.sql());
        assertEquals(List.of("encoded:a", "encoded:b", "encoded:b", 7L, 3, 0L),
                     cursor.parameters());
        assertEquals(cursor.parameters(), cursorAgain.parameters());
        assertEquals(6, codec.writes.get());
        assertEquals(4, pagination.parameters.get());
        assertEquals(3, pagination.paginate.get());
    }

    @Test
    void invalidInputsKeepTheirValidationOrderIncludingNullProjectedLock() {
        Fixture fixture = fixture(RdbDialect.postgresql(), OrmCachePolicy.safeDefaults(),
                                  RdbDialect.postgresql().pagination());
        assertEquals("dynamic form must not be null", assertThrows(NullPointerException.class,
                () -> fixture.renderer.select(null, EMPTY, (PageQuery) null)).getMessage());
        assertEquals("visible fields must not be null", assertThrows(NullPointerException.class,
                () -> fixture.renderer.selectPhysical(fixture.form, null, EMPTY,
                        (PageQuery) null)).getMessage());
        assertEquals("query projections must not be null", assertThrows(NullPointerException.class,
                () -> fixture.renderer.selectProjectedLocking(fixture.form, EMPTY, null, null, null,
                        null, null)).getMessage());
        assertEquals("projected query must select at least one entity field",
                assertThrows(IllegalArgumentException.class,
                        () -> fixture.renderer.selectProjectedLocking(fixture.form, EMPTY,
                                List.of(), List.of(), List.of(), null, null)).getMessage());
        assertEquals("dynamic field does not exist", assertThrows(IllegalArgumentException.class,
                () -> fixture.renderer.selectProjectedLocking(fixture.form, EMPTY,
                        List.of("id"), List.of(), List.of(PageSort.asc("missing")),
                        null, null)).getMessage());
        assertEquals("locking read dialect must not be null", assertThrows(NullPointerException.class,
                () -> fixture.renderer.selectProjectedLocking(fixture.form, EMPTY,
                        List.of("id"), List.of(), List.of(), null, null)).getMessage());
        assertEquals("read lock must not be null", assertThrows(NullPointerException.class,
                () -> fixture.renderer.selectProjectedLocking(fixture.form, EMPTY,
                        List.of("id"), List.of(), List.of(),
                        RdbDialect.postgresql().lockingReadDialect(), null)).getMessage());
        assertNotEquals("", fixture.renderer.selectProjected(fixture.form, EMPTY,
                List.of("id"), List.of(), List.of()).sql());
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
