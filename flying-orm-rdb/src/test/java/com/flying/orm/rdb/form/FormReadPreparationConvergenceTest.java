package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.NullOrder;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.lock.ReadLock;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Locale;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FormReadPreparationConvergenceTest {

    @Test
    void computedKeysetResolvesDecodingOnlyAfterHiddenFieldsAreKnown() throws ReflectiveOperationException {
        try (Fixture fixture = new Fixture()) {
            AtomicInteger resolutions = new AtomicInteger();
            FormReadPlanner planner = countingPlanner(fixture, resolutions);
            QuerySpec spec = QuerySpec.builder(fixture.form)
                    .expression("next_id", ValueExpression.field("id").add(ValueExpression.value(1L)))
                    .build();
            FormReadPlanner.PlannedKeysetPage plan = planner.keysetPage(spec,
                    KeysetPageQuery.first(2, KeysetSort.asc("id", NullOrder.LAST)));
            assertEquals(List.of("next_id"), plan.outputFields());
            assertTrue(plan.layout().hasHiddenSelections());
            assertEquals(1, resolutions.get(), "computed output must be resolved only in the final decoding plan");
        }
    }

    @Test
    void computedContainsResolvesDecodingOnceAndKeepsVerificationFields() throws ReflectiveOperationException {
        try (Fixture fixture = new Fixture()) {
            AtomicInteger resolutions = new AtomicInteger();
            FormReadPlanner planner = countingPlanner(fixture, resolutions);
            QuerySpec spec = QuerySpec.of(fixture.form, ConditionGroup.and()
                    .add(ProtectedConditions.contains("secret", "alpha")).build())
                    .withExpression("next_id", ValueExpression.field("id").add(ValueExpression.value(1L)))
                    .withProjection(List.of("next_id"), List.of());
            FormReadPlanner.PlannedQuery rows = planner.select(spec);
            assertEquals(List.of("next_id"), rows.outputFields());
            assertTrue(rows.resultDecodingPlan().requiresProtectionCpu());
            assertEquals(1, resolutions.get(), "verification fields must not cause the output plan to be rebuilt");
            resolutions.set(0);
            FormReadPlanner.PlannedKeysetPage page = planner.keysetPage(spec,
                    KeysetPageQuery.first(2, KeysetSort.asc("id", NullOrder.LAST)));
            assertEquals(List.of("next_id"), page.outputFields());
            assertTrue(page.resultDecodingPlan().requiresProtectionCpu());
            assertEquals(1, resolutions.get());
        }
    }

    private static FormReadPlanner countingPlanner(Fixture fixture, AtomicInteger resolutions)
            throws ReflectiveOperationException {
        Map<DynamicField, EntityTypeMappingRegistry.Mapping> mappings = new IdentityHashMap<>() {
            @Override
            public EntityTypeMappingRegistry.Mapping get(Object field) {
                if (field instanceof DynamicField dynamic && dynamic.name().equals("next_id")) {
                    resolutions.incrementAndGet();
                }
                return super.get(field);
            }
        };
        FormDataSqlRenderer renderer = fixture.renderer;
        // 在配置所有者内部计数；调用方 Map 已被快照，不能再用它作为读取探针。
        java.lang.reflect.Field codecs = FormSqlRenderSupport.class.getDeclaredField("customFieldCodecs");
        codecs.setAccessible(true);
        codecs.set(renderer.support, mappings);
        return new FormReadPlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited());
    }

    @Test
    void offsetPageUsesTheSamePreparedReadAsOrdinaryAndLockingQueries() {
        try (Fixture fixture = new Fixture()) {
            QuerySpec spec = QuerySpec.of(fixture.form, ConditionGroup.and().where("id", ">", 0L).build())
                    .withProjection(List.of("id"), List.of())
                    .withScope(DataScope.tenant("tenant_id", 9L));
            PageQuery page = PageQuery.of(2, 3, PageSort.desc("id"));
            org.junit.jupiter.api.Assertions.assertAll(
                    () -> org.junit.jupiter.api.Assertions.assertInstanceOf(FormReadPlanner.PlannedQuery.class,
                            fixture.planner.reads.page(spec, page)),
                    () -> org.junit.jupiter.api.Assertions.assertInstanceOf(FormReadPlanner.PlannedQuery.class,
                            fixture.planner.reads.pageGoverned(spec, page,
                                    com.flying.orm.core.scope.FieldUsePolicy.unrestricted(),
                                    com.flying.orm.core.condition.QueryShapeLimits.defaults()).plan()));
        }
    }

    @Test
    void pageChangesOnlyTheWindowAndAddsTheCountRequest() {
        DynamicForm form = DynamicForm.builder("events", "events")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("tenant_id", "BIGINT")).build();
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), dialect);
            FormReadPlanner planner = new FormReadPlanner(renderer,
                    new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                    SqlExecutionOptions.unlimited());
            for (boolean grouped : List.of(false, true)) {
                QuerySpec spec = QuerySpec.of(form, ConditionGroup.and().where("id", ">", 7L).build())
                        .withScope(DataScope.tenant("tenant_id", 9L))
                        .withProjection(List.of("id"), grouped ? List.of("id") : List.of())
                        .withSorts(List.of(PageSort.desc("id")));
                FormReadPlanner.PlannedQuery selected = planner.select(spec);
                PageQuery requested = PageQuery.of(2, 3, PageSort.asc("id"));
                FormReadPlanner.PlannedQuery page = planner.page(spec, requested);
                SqlRequest expected = renderer.paginate(selected.request(), requested);
                assertEquals(expected.sql(), page.request().sql(), dialect.name());
                assertEquals(expected.parameters(), page.request().parameters(), dialect.name());
                assertEquals(spec.sorts(), page.page().sorts(), "query sorts take precedence over page sorts");
                assertEquals(selected.outputFields(), page.outputFields());
                assertNull(selected.countRequest(), "ordinary reads must not generate an unused count");
                assertNull(selected.page());
                SqlRequest count = planner.count(spec, null, null, false).request();
                assertEquals(count.sql(), page.countRequest().sql(), dialect.name());
                assertEquals(count.parameters(), page.countRequest().parameters(), dialect.name());
                FormReadPlanner.PlannedQuery suppliedSort = planner.page(spec.withSorts(List.of()), requested);
                assertEquals(requested.sorts(), suppliedSort.page().sorts());
            }
        }
    }

    @Test
    void rowProducingPlansShareTheirResultContractWithoutAnotherCarrier() {
        Class<?>[] shapes = {FormReadPlanner.PlannedQuery.class,
                FormReadPlanner.PlannedKeysetPage.class};
        assertTrue(java.util.Arrays.stream(shapes[0].getInterfaces()).anyMatch(contract ->
                        java.util.Arrays.stream(shapes).allMatch(contract::isAssignableFrom)),
                "query, offset page and cursor page must expose the same row-result contract");
        for (Class<?> shape : shapes) {
            assertTrue(shape.isRecord(), "the existing immutable plan is the result carrier");
        }
    }

    @Test
    void lockingReadsPublishTheOrdinaryReadPlanWithoutAnUnusedSecondPlan() {
        try (Fixture fixture = new Fixture()) {
            QuerySpec query = QuerySpec.of(fixture.form, ConditionGroup.and().where("id", ">", 0L).build())
                    .withProjection(List.of("id"), List.of())
                    .withScope(DataScope.tenant("tenant_id", 9L));
            QuerySpec locking = query.withLock(ReadLock.update());
            KeysetPageQuery page = KeysetPageQuery.first(2, KeysetSort.asc("id", NullOrder.LAST));
            com.flying.orm.core.scope.FieldUsePolicy policy = com.flying.orm.core.scope.FieldUsePolicy.unrestricted();
            com.flying.orm.core.condition.QueryShapeLimits limits = com.flying.orm.core.condition.QueryShapeLimits.defaults();
            org.junit.jupiter.api.Assertions.assertAll(
                    () -> org.junit.jupiter.api.Assertions.assertInstanceOf(FormReadPlanner.PlannedQuery.class,
                            fixture.planner.reads.lockingRead(locking)),
                    () -> org.junit.jupiter.api.Assertions.assertInstanceOf(FormReadPlanner.PlannedQuery.class,
                            fixture.planner.reads.lockingReadGoverned(locking, policy, limits).plan()),
                    () -> org.junit.jupiter.api.Assertions.assertInstanceOf(FormReadPlanner.PlannedKeysetPage.class,
                            fixture.planner.reads.keysetPage(locking, page)),
                    () -> org.junit.jupiter.api.Assertions.assertInstanceOf(FormReadPlanner.PlannedKeysetPage.class,
                            fixture.planner.reads.keysetPageGoverned(locking, page, policy, limits).plan()));
            SqlRequest ordinary = fixture.planner.reads.keysetPage(query, page).request();
            SqlRequest locked = fixture.planner.reads.keysetPage(locking, page).request();
            SqlRequest governed = fixture.planner.reads.keysetPageGoverned(locking, page, policy, limits).plan().request();
            assertEquals(ordinary.sql() + " FOR UPDATE", locked.sql());
            assertEquals(ordinary.parameters(), locked.parameters());
            assertEquals(locked.sql(), governed.sql());
            assertEquals(locked.parameters(), governed.parameters());
        }
    }

    @Test
    void syncDynamicReadShapesReuseTheExecutorsMappedResultTerminal() {
        DynamicForm form = DynamicForm.builder("mapped_reads", "mapped_reads")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("payload", "JSON")).build();
        QuerySpec spec = QuerySpec.of(form, ConditionGroup.and().build());
        List<DynamicRow> source = List.of(
                DynamicRow.copyOf(Map.of("id", 1L, "payload", "{\"value\":true}")),
                DynamicRow.copyOf(Map.of("id", 2L, "payload", "null")));
        AtomicInteger mappedCalls = new AtomicInteger();
        SyncSqlExecutor executor = new com.flying.orm.rdb.support.ListQueryFixture() {
            public List<DynamicRow> query(SqlRequest request) {
                return request.sql().toLowerCase(Locale.ROOT).contains("count(")
                        ? List.of(DynamicRow.copyOf(Map.of("total", 2L))) : source;
            }
            public <T> List<T> queryMapped(SqlRequest request,
                    SqlExecutionOptions options, RowMapper<T> mapper, int limit) {
                mappedCalls.incrementAndGet();
                assertEquals(0, limit);
                return com.flying.orm.rdb.support.ListQueryFixture.super.queryMapped(request, options, mapper, limit);
            }
            public long rowsUpdated(SqlRequest request) {
                throw new AssertionError("read must not write");
            }
            public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
                throw new AssertionError("read must not write");
            }
        };
        SyncBatchExecutor batch = (SyncBatchExecutor) Proxy.newProxyInstance(getClass().getClassLoader(),
                        new Class<?>[]{SyncBatchExecutor.class},
                        (proxy, method, args) -> { throw new AssertionError("read must not batch-write"); });
        SyncFormClient client = SyncFormClient.create(executor, batch, FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql()));
        List<List<DynamicRow>> results = List.of(
                client.select(spec), client.page(spec, PageQuery.of(1, 2)).rows(),
                client.keysetPage(spec, KeysetPageQuery.first(2, KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST))).rows(),
                client.keysetPage(spec, KeysetPageQuery.first(2, KeysetSort.asc("id", NullOrder.LAST))).rows());
        for (List<DynamicRow> rows : results) {
            assertEquals(Map.of("value", true), rows.getFirst().get("payload"));
            assertNull(rows.getLast().get("payload"));
            assertEquals(List.of(1L, 2L), rows.stream().map(row -> row.get("id")).toList());
            assertThrows(UnsupportedOperationException.class, () -> rows.clear());
        }
        assertEquals("{\"value\":true}", source.getFirst().get("payload"));
        assertEquals("null", source.getLast().get("payload"));
        assertEquals(4, mappedCalls.get());
        DynamicForm plain = DynamicForm.builder("plain", "plain")
                .addField(DynamicField.of("payload", "VARCHAR")).build();
        org.junit.jupiter.api.Assertions.assertSame(source,
                client.select(QuerySpec.of(plain, ConditionGroup.and().build())),
                "a query without decoding must retain the existing direct result path");
        assertEquals(4, mappedCalls.get());
    }

    @Test
    void lockingReadPreservesTheOrdinaryProjectionScopeAndProtectedParameters() {
        try (Fixture fixture = new Fixture()) {
            QuerySpec spec = QuerySpec.of(fixture.form, ConditionGroup.and()
                    .add(ProtectedConditions.exact("secret", "alphabet")).build())
                    .withScope(DataScope.tenant("tenant_id", 9L))
                    .withProjection(List.of("ID", "secret"), List.of())
                    .withSorts(List.of(PageSort.desc("ID")))
                    .withExecutionOptions(SqlExecutionOptions.maxRows(1234));
            FormReadPlanner.PlannedQuery ordinary = fixture.planner.reads.select(spec);
            FormReadPlanner.PlannedQuery locking = fixture.planner.reads.lockingRead(spec.withLock(ReadLock.update()));
            assertEquals(ordinary.request().sql() + " FOR UPDATE", locking.request().sql());
            assertArrayEquals(ordinary.request().parameters().toArray(), locking.request().parameters().toArray());
            assertEquals(ordinary.outputFields(), locking.outputFields());
            assertEquals(ordinary.options(), locking.options());
            assertEquals(ordinary.scope().tenantScope("tenant_id"), locking.scope().tenantScope("tenant_id"));
            assertFalse(locking.request().parameters().contains("alphabet"));
        }
    }

    @Test
    void lockingShapeRejectionsKeepTheirOrderAndMessages() {
        try (Fixture fixture = new Fixture()) {
            QuerySpec contains = QuerySpec.of(fixture.form, ConditionGroup.and()
                    .add(ProtectedConditions.contains("secret", "alpha")).build());
            assertEquals("locking read does not support protected contains queries",
                    assertThrows(UnsupportedOperationException.class, () -> fixture.planner.reads.lockingRead(contains.withLock(ReadLock.update()))).getMessage());
            QuerySpec grouped = contains.withProjection(List.of("missing"), List.of("missing"));
            assertEquals("locking read does not support grouped queries",
                    assertThrows(UnsupportedOperationException.class, () -> fixture.planner.reads.lockingRead(grouped.withLock(ReadLock.update()))).getMessage());
        }
    }

    @Test
    void ordinaryProtectedSearchRetainsScopeAndPageShapes() {
        try (Fixture fixture = new Fixture()) {
            ConditionGroup where = ConditionGroup.and()
                    .add(ProtectedConditions.exact("secret", "alphabet"))
                    .add(ProtectedConditions.suffix("secret", "bet")).build();
            QuerySpec spec = QuerySpec.of(fixture.form, where)
                    .withScope(DataScope.where(ConditionGroup.and().where("tenant_id", "=", 9L).build()));
            var select = fixture.planner.reads.select(spec);
            var page = fixture.planner.reads.page(spec, new PageQuery(1, 2, List.of()));
            var cursor = fixture.planner.reads.keysetPage(spec, KeysetPageQuery.first(2, KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST)));
            assertNull(select.containsQuery());
            assertNotNull(page.countRequest());
            assertTrue(select.request().parameters().contains(9L));
            assertTrue(cursor.request().parameters().contains(3));
            assertFalse(select.request().parameters().contains("alphabet"));
            assertFalse(select.request().parameters().contains("bet"));
        }
    }

    @Test
    void containsOmitsOffsetCountAndPreservesCallerExecutionLimits() {
        try (Fixture fixture = new Fixture()) {
            QuerySpec spec = QuerySpec.of(fixture.form, ConditionGroup.and()
                    .add(ProtectedConditions.contains("secret", "alpha")).build())
                    .withExecutionOptions(SqlExecutionOptions.maxRows(2000).withMaxResultBytes(8192));
            var select = fixture.planner.reads.select(spec);
            var page = fixture.planner.reads.page(spec, new PageQuery(1, 2, List.of()));
            assertNotNull(select.containsQuery());
            assertNull(page.countRequest());
            assertNotNull(page.containsQuery());
            var cursor = fixture.planner.reads.keysetPage(spec, KeysetPageQuery.first(2, KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST)));
            assertFalse(select.request().sql().contains(" limit "));
            assertFalse(page.request().sql().contains(" limit "));
            assertFalse(cursor.request().sql().contains(" limit "));
            assertEquals(spec.executionOptions().orElseThrow(), select.options());
            assertEquals(select.options(), page.options());
            assertEquals(select.options(), cursor.options());
        }
    }

    @Test
    void containsStructureErrorsPrecedeInvalidOrdinaryEncryptedComparison() {
        try (Fixture fixture = new Fixture()) {
            ConditionGroup nested = ConditionGroup.and().where("secret", "=", "invalid")
                    .add(ConditionGroup.or().add(ProtectedConditions.contains("secret", "alpha")).build())
                    .build();
            assertEquals("protected contains search requires a top-level AND condition",
                    assertThrows(IllegalArgumentException.class,
                            () -> fixture.planner.reads.select(QuerySpec.of(fixture.form, nested))).getMessage());
            ConditionGroup multiple = ConditionGroup.and().where("secret", "=", "invalid")
                    .add(ProtectedConditions.contains("secret", "alpha"))
                    .add(ProtectedConditions.contains("secret", "beta")).build();
            assertEquals("protected contains search supports one field per query",
                    assertThrows(IllegalArgumentException.class,
                            () -> fixture.planner.reads.select(QuerySpec.of(fixture.form, multiple))).getMessage());
        }
    }

    private static final class Fixture implements AutoCloseable {
        private final DynamicForm form = DynamicForm.builder("read-preparation", "read_preparation")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .addField(DynamicField.of("tenant_id", "BIGINT"))
                .encrypted("secret", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.EXACT, EncryptedSearchMode.SUFFIX,
                                EncryptedSearchMode.CONTAINS).suffixLengths(3).build()).build();
        private final ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]));
        private final FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql())
                .withProtectedFields(runtime);
        private final FormOperationPlanner planner = new FormOperationPlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited());

        @Override
        public void close() {
            runtime.close();
        }
    }
}
