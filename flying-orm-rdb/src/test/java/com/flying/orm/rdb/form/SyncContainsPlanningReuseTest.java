package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.protection.MaskingPolicyRegistry;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.protection.ProtectedFormLayout;
import com.flying.orm.rdb.protection.ProtectedValueNormalizerRegistry;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.time.OffsetTime;
import java.util.function.BiConsumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

class SyncContainsPlanningReuseTest {

    @Test
    void containsCountConsumesCandidatesWithoutCollectingThem() {
        DynamicForm form = DynamicForm.builder("contains_users", "tenant.contains_users")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .encrypted("secret", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.CONTAINS).build()).build();
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            List<DynamicRow> candidates = List.of(encryptedRow(runtime, form, 1L, "alphabet"),
                    encryptedRow(runtime, form, 2L, "nothing"),
                    encryptedRow(runtime, form, 3L, "alphabet soup"));
            AtomicInteger queries = new AtomicInteger();
            IllegalStateException failure = new IllegalStateException("candidate stream failed");
            SyncSqlExecutor executor = new com.flying.orm.rdb.support.ListQueryFixture() {
                public List<DynamicRow> query(SqlRequest request) {
                    throw new AssertionError("CONTAINS count must not collect candidate rows");
                }
                public long queryEach(SqlRequest request, SqlExecutionOptions options,
                                      java.util.function.Consumer<? super DynamicRow> consumer) {
                    assertTrue(request.sql().startsWith("select fop_business.\"secret\" as \"secret\" from "));
                    org.junit.jupiter.api.Assertions.assertFalse(request.sql().contains("\"tenant.__fop_c_"), request.sql());
                    assertTrue(request.sql().contains("\"tenant\".\"__fop_c_"), request.sql());
                    int invocation = queries.getAndIncrement();
                    if (invocation != 1) candidates.forEach(consumer);
                    if (invocation == 2) throw failure;
                    return invocation == 0 ? candidates.size() : 0L;
                }
                public long rowsUpdated(SqlRequest request) { throw new AssertionError("unexpected write"); }
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
                    throw new AssertionError("unexpected write");
                }
            };
            SyncFormClient client = SyncFormClient.create(executor,
                    (request, completed) -> { throw new AssertionError("unexpected batch"); },
                    FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(),
                            RdbDialect.postgresql()).withProtectedFields(runtime));
            QuerySpec spec = QuerySpec.of(form, ConditionGroup.and()
                    .add(ProtectedConditions.contains("secret", "pha")).build());
            assertEquals(2L, client.count(spec), "false-positive candidates must not be counted");
            assertEquals(0L, client.count(spec), "empty result must not reuse the previous counter");
            org.junit.jupiter.api.Assertions.assertSame(failure,
                    assertThrows(IllegalStateException.class, () -> client.count(spec)),
                    "stream failure must propagate instead of returning a partial count");
            assertEquals(3, queries.get());
        }
    }

    @Test
    void containsReadShapesStreamCandidatesWithoutPublishingVerificationFields() {
        DynamicForm form = DynamicForm.builder("contains_users", "contains_users")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .encrypted("secret", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.CONTAINS).build()).build();
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            CountingExecutor executor = new CountingExecutor(List.of(
                    encryptedRow(runtime, form, 1L, "alphabet"),
                    encryptedRow(runtime, form, 2L, "nothing"),
                    encryptedRow(runtime, form, 3L, "alphabet soup")));
            SyncFormClient client = SyncFormClient.create(executor,
                    (request, completed) -> { throw new AssertionError("unexpected batch"); },
                    FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(),
                            RdbDialect.postgresql()).withProtectedFields(runtime));
            QuerySpec spec = QuerySpec.of(form, ConditionGroup.and()
                    .add(ProtectedConditions.contains("secret", "pha")).build())
                    .withProjection(List.of("id"), List.of());
            List<DynamicRow> selected = client.select(spec);
            com.flying.orm.core.page.PageResult<DynamicRow> page = client.page(spec, PageQuery.of(2, 1));
            com.flying.orm.core.page.KeysetPageResult<DynamicRow> cursor = client.keysetPage(
                    spec, KeysetPageQuery.first(1, KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST)));
            DynamicRow first = client.first(spec);
            assertEquals(1L, first.get("id"));
            assertEquals(1, first.columnCount());
            assertThrows(IllegalStateException.class, () -> client.selectOne(spec, Result.class));
            assertEquals(List.of(1L, 3L), selected.stream().map(row -> row.get("id")).toList());
            assertEquals(2L, page.total());
            assertEquals(3L, page.rows().getFirst().get("id"));
            assertEquals(1L, cursor.rows().getFirst().get("id"));
            for (List<DynamicRow> rows : List.of(selected, page.rows(), cursor.rows())) {
                assertTrue(rows.stream().allMatch(row -> row.columnCount() == 1 && !row.containsKey("secret")));
                assertThrows(UnsupportedOperationException.class, rows::clear);
            }
            assertEquals(5, executor.queries);
            assertEquals(5, executor.streamingQueries,
                    "all read shapes must stream candidate verification instead of collecting decrypted lists");
            assertEquals(0, executor.mappedQueries);
            assertTrue(executor.rows.getFirst().get("secret") instanceof byte[]);
        }
    }

    @Test
    void containsCountReadsOnlyTheSearchFieldAndSkipsUnrelatedDecoding() {
        DynamicForm form = DynamicForm.builder("contains_users", "contains_users")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .addField(DynamicField.of("payload", "JSON"))
                .encrypted("secret", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.CONTAINS).build()).build();
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql()).withProtectedFields(runtime);
            Object ciphertext = encryptedRow(runtime, form, 1L, "alphabet").get("secret");
            java.util.function.Function<SqlRequest, List<DynamicRow>> rows = request -> {
                String projection = request.sql().substring(0, request.sql().indexOf(" from "));
                Map<String, Object> values = projection.contains("\"payload\"")
                        ? Map.of("id", 1L, "secret", ciphertext, "payload", "{invalid-json")
                        : Map.of("secret", ciphertext);
                return List.of(DynamicRow.copyOf(values));
            };
            SyncFormClient sync = SyncFormClient.create(new com.flying.orm.rdb.support.ListQueryFixture() {
                public List<DynamicRow> query(SqlRequest request) { return rows.apply(request); }
                public long rowsUpdated(SqlRequest request) { throw new AssertionError("unexpected write"); }
                public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
                    throw new AssertionError("unexpected write");
                }
            }, (request, completed) -> { throw new AssertionError("unexpected batch"); }, renderer);
            ReactiveFormClient reactive = ReactiveFormClient.create(new com.flying.orm.rdb.reactive.ReactiveSqlExecutor() {
                public reactor.core.publisher.Flux<DynamicRow> query(SqlRequest request) {
                    return reactor.core.publisher.Flux.defer(() -> reactor.core.publisher.Flux.fromIterable(rows.apply(request)));
                }
                public reactor.core.publisher.Mono<Long> rowsUpdated(SqlRequest request) { throw new AssertionError("unexpected write"); }
            }, renderer);
            QuerySpec spec = QuerySpec.of(form, ConditionGroup.and()
                    .add(ProtectedConditions.contains("secret", "pha")).build());
            org.junit.jupiter.api.Assertions.assertAll(
                    () -> assertEquals(1L, sync.count(spec)),
                    () -> assertEquals(1L, reactive.count(spec).block()));
        }
    }

    @Test
    void containsCountDoesNotRequirePrimaryKeyOrdering() {
        DynamicForm form = DynamicForm.builder("contains_times", "contains_times")
                .addField(DynamicField.primaryKey("id", "OFFSET_TIME"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .encrypted("secret", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.CONTAINS).build()).build();
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            for (RdbDialect dialect : List.of(RdbDialect.mysql(), RdbDialect.oracle(), RdbDialect.sqlServer(),
                    RdbDialect.postgresql())) {
                FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                        SqlRenderer.builder().addDefaultTerms().build(), dialect).withProtectedFields(runtime);
                CountingExecutor executor = new CountingExecutor(List.of());
                SyncFormClient sync = SyncFormClient.create(executor,
                        (request, completed) -> { throw new AssertionError("unexpected batch"); }, renderer);
                QuerySpec spec = QuerySpec.of(form, ConditionGroup.and()
                        .add(ProtectedConditions.contains("secret", "pha")).build());
                assertEquals(0L, sync.count(spec));
                assertTrue(!executor.lastRequest.sql().contains("order by"), executor.lastRequest.sql());
            }
        }
    }

    @Test
    void rejectsTextBackedOffsetTimeContainsOrdering() {
        checkTemporalOrdering(false, (operations, spec) -> operations.select(
                spec.withSorts(List.of(PageSort.asc("remote_time")))));
    }

    @Test
    void rejectsTextBackedOffsetTimeContainsOffsetPage() {
        checkTemporalOrdering(false, (operations, spec) -> operations.page(
                spec, PageQuery.of(1, 10, PageSort.asc("remote_time"))));
    }

    @Test
    void rejectsTextBackedOffsetTimeContainsCursorPage() {
        checkTemporalOrdering(false, (operations, spec) -> operations.keysetPage(
                spec, KeysetPageQuery.first(10, KeysetSort.asc("remote_time", com.flying.orm.core.page.NullOrder.LAST))));
        checkTemporalOrdering(false, (operations, spec) -> operations.keysetPage(spec,
                KeysetPageQuery.after(10, com.flying.orm.core.page.CursorPosition.of(List.of(OffsetTime.parse("10:00:00+14:00"), 1L)),
                        KeysetSort.asc("remote_time", com.flying.orm.core.page.NullOrder.LAST))));
    }

    @Test
    void rejectsTextBackedOffsetTimeImplicitContainsPrimaryKeyOrdering() {
        checkTemporalOrdering(true, SyncFormOperations::select);
        checkTemporalOrdering(true, (operations, spec) -> operations.page(spec, PageQuery.of(1, 10)));
        checkTemporalOrdering(true, (operations, spec) -> operations.keysetPage(
                spec, KeysetPageQuery.first(10, KeysetSort.asc("sequence", com.flying.orm.core.page.NullOrder.LAST))));
    }

    private static void checkTemporalOrdering(boolean temporalPrimaryKey,
                                             BiConsumer<SyncFormOperations, QuerySpec> action) {
        DynamicForm form = DynamicForm.builder("contains_times", "contains_times")
                .addField(DynamicField.primaryKey("id", temporalPrimaryKey ? "OFFSET_TIME" : "BIGINT"))
                .addField(DynamicField.of("remote_time", "OFFSET_TIME").withNullable(false))
                .addField(DynamicField.of("sequence", "BIGINT").withNullable(false))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .encrypted("secret", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.CONTAINS).build()).build();
        for (RdbDialect dialect : List.of(RdbDialect.mysql(), RdbDialect.oracle(), RdbDialect.sqlServer(),
                RdbDialect.h2(), RdbDialect.postgresql())) {
            try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                    ProtectedFieldKeyRing.single("v1", new byte[32]));
                 EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
                FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                        SqlRenderer.builder().addDefaultTerms().build(), dialect).withProtectedFields(runtime);
                CountingExecutor executor = new CountingExecutor(List.of());
                SyncFormOperations operations = new SyncFormOperations(executor, new FormConfiguration(renderer, StructuredConditionResolver.defaults(), SqlExecutionOptions.unlimited(), models).withDataScope(DataScope.none()));
                QuerySpec spec = QuerySpec.of(form, ConditionGroup.and()
                        .add(ProtectedConditions.contains("secret", "pha")).build());
                if (dialect.name().equals("h2") || dialect.name().equals("postgresql")) {
                    assertDoesNotThrow(() -> action.accept(operations, spec));
                } else {
                    IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                            () -> action.accept(operations, spec));
                    assertTrue(failure.getMessage().contains("OFFSET_TIME"), failure.getMessage());
                    assertEquals(0, executor.queries, "invalid ordering must fail before querying candidates");
                }
            }
        }
    }

    @Test
    void typedListPreparesTheSameContainsQueryOnce() {
        checkPlanning(false);
    }

    @Test
    void typedOnePreparesTheSameContainsQueryOnceAndVerifiesFalsePositivesFirst() {
        checkPlanning(true);
    }

    @Test
    void firstVerifiesCandidatesBeforeTakingOneAndKeepsSyncReactiveParity() {
        DynamicForm form = DynamicForm.builder("contains_users", "contains_users")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .encrypted("secret", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.CONTAINS).build()).build();
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql()).withProtectedFields(runtime);
            List<DynamicRow> rows = List.of(encryptedRow(runtime, form, 1L, "no match"),
                    encryptedRow(runtime, form, 2L, "alphabet"), encryptedRow(runtime, form, 3L, "elephant"));
            CountingExecutor executor = new CountingExecutor(rows);
            SyncFormClient sync = SyncFormClient.create(executor,
                    (request, completed) -> { throw new AssertionError("unexpected batch"); }, renderer);
            java.util.concurrent.atomic.AtomicReference<SqlRequest> reactiveRequest = new java.util.concurrent.atomic.AtomicReference<>();
            ReactiveFormClient reactive = ReactiveFormClient.create(new com.flying.orm.rdb.reactive.ReactiveSqlExecutor() {
                public reactor.core.publisher.Flux<DynamicRow> query(SqlRequest request) {
                    reactiveRequest.set(request);
                    return reactor.core.publisher.Flux.fromIterable(rows);
                }
                public reactor.core.publisher.Mono<Long> rowsUpdated(SqlRequest request) { throw new AssertionError("unexpected write"); }
            }, renderer);
            QuerySpec spec = QuerySpec.of(form, ConditionGroup.and().add(ProtectedConditions.contains("secret", "pha")).build());
            for (boolean governed : List.of(false, true)) {
                SyncFormClient syncClient = governed ? sync.withFieldUsePolicy(com.flying.orm.core.scope.FieldUsePolicy.unrestricted()) : sync;
                ReactiveFormClient reactiveClient = governed ? reactive.withFieldUsePolicy(com.flying.orm.core.scope.FieldUsePolicy.unrestricted()) : reactive;
                assertEquals(new Result(2L, "alphabet"), syncClient.first(spec, Result.class));
                assertEquals(new Result(2L, "alphabet"), reactiveClient.first(spec, Result.class).block());
                assertEquals(2L, syncClient.first(spec).get("id"));
                assertEquals(2L, reactiveClient.first(spec).block().get("id"));
                java.util.ArrayList<Result> consumed = new java.util.ArrayList<>();
                assertEquals(2L, syncClient.forEach(spec, Result.class, consumed::add));
                assertEquals(List.of(new Result(2L, "alphabet"), new Result(3L, "elephant")), consumed);
                consumed.clear();
                assertEquals(1L, syncClient.forEachWhile(spec, Result.class, row -> {
                    consumed.add(row);
                    return false;
                }));
                assertEquals(List.of(new Result(2L, "alphabet")), consumed);
                // The candidate protocol must remain identical to a regular verified read.
                SqlRequest first = reactiveRequest.get();
                reactiveClient.select(spec).collectList().block();
                assertEquals(reactiveRequest.get().sql(), first.sql());
                org.junit.jupiter.api.Assertions.assertArrayEquals(
                        reactiveRequest.get().parameters().toArray(), first.parameters().toArray());
                int beforeCount = executor.queries;
                assertEquals(2L, syncClient.count(spec));
                assertEquals(beforeCount + 1, executor.queries, "count verifies candidates in a single query");
                assertEquals(2L, reactiveClient.count(spec).block());
                SqlRequest count = reactiveRequest.get();
                assertTrue(count.sql().startsWith("select fop_business.\"secret\" as \"secret\" from "), count.sql());
                assertTrue(!count.sql().contains("order by"), count.sql());
                org.junit.jupiter.api.Assertions.assertArrayEquals(first.parameters().toArray(), count.parameters().toArray());
            }
            com.flying.orm.core.scope.FieldUsePolicy filterOnly = com.flying.orm.core.scope.FieldUsePolicy.builder()
                    .allow("secret", com.flying.orm.core.scope.FieldUse.FILTER).build();
            assertEquals(2L, sync.withFieldUsePolicy(filterOnly).count(spec));
            assertEquals(2L, reactive.withFieldUsePolicy(filterOnly).count(spec).block());
            QuerySpec grouped = spec.withProjection(List.of("id"), List.of("id"));
            assertThrows(IllegalArgumentException.class, () -> sync.count(grouped));
            assertThrows(IllegalArgumentException.class, () -> reactive.count(grouped).block());
        }
    }

    private static void checkPlanning(boolean one) {
        AtomicInteger normalizations = new AtomicInteger();
        ProtectedValueNormalizerRegistry normalizers = ProtectedValueNormalizerRegistry.standard()
                .with("counted", value -> {
                    normalizations.incrementAndGet();
                    return value;
                });
        DynamicForm form = DynamicForm.builder("contains_users", "contains_users")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .encrypted("secret", EncryptedFieldDefinition.builder()
                        .normalizer("counted").searchModes(EncryptedSearchMode.CONTAINS).build())
                .build();
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]), normalizers, MaskingPolicyRegistry.standard());
             EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql())
                    .withProtectedFields(runtime);
            CountingExecutor executor = new CountingExecutor(List.of(
                    encryptedRow(runtime, form, 1L, "goodbye"),
                    encryptedRow(runtime, form, 2L, "nothing"),
                    encryptedRow(runtime, form, 3L, "alphabet soup")));
            SyncFormOperations operations = new SyncFormOperations(executor, new FormConfiguration(renderer, StructuredConditionResolver.defaults(), SqlExecutionOptions.unlimited(), models).withDataScope(DataScope.none()));
            QuerySpec spec = QuerySpec.of(form, ConditionGroup.and()
                    .add(ProtectedConditions.contains("secret", "pha")).build());

            normalizations.set(0);
            assertEquals(List.of(3L), operations.select(spec).stream().map(row -> row.get("id")).toList());
            int onePlanNormalizations = normalizations.get();
            assertTrue(onePlanNormalizations > 0);
            normalizations.set(0);
            executor.queries = 0;

            if (one) {
                assertEquals(new Result(3L, "alphabet soup"), operations.selectOne(spec, Result.class));
            } else {
                assertEquals(List.of(new Result(3L, "alphabet soup")), operations.select(spec, Result.class));
            }
            assertEquals(1, executor.queries);
            assertEquals(onePlanNormalizations, normalizations.get(),
                    "typed mapping must consume the existing plan, without repeating contains normalization");
        }
    }

    private static DynamicRow encryptedRow(ProtectedFieldRuntime runtime, DynamicForm form, long id, String secret) {
        return DynamicRow.copyOf(runtime.writeOperation(form, ProtectedFormLayout.physical(form),
                DataScope.none(), ValueCodecRegistry.standard()).prepare(Map.of("id", id, "secret", secret)).ownedValues());
    }

    record Result(Long id, String secret) { }

    private static final class CountingExecutor implements com.flying.orm.rdb.support.ListQueryFixture {
        private final List<DynamicRow> rows;
        private int queries;
        private int mappedQueries;
        private int streamingQueries;
        private SqlRequest lastRequest;

        private CountingExecutor(List<DynamicRow> rows) {
            this.rows = rows;
        }

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            queries++;
            lastRequest = request;
            return rows;
        }

        @Override
        public long queryEach(SqlRequest request, SqlExecutionOptions options,
                              java.util.function.Consumer<? super DynamicRow> consumer) {
            streamingQueries++;
            return com.flying.orm.rdb.support.ListQueryFixture.super.queryEach(request, options, consumer);
        }

        @Override
        public <T> List<T> queryMapped(SqlRequest request, SqlExecutionOptions options,
                com.flying.orm.rdb.mapping.RowMapper<T> mapper, int rowLimit) {
            mappedQueries++;
            assertEquals(0, rowLimit, "CONTAINS must verify all candidates before pagination");
            return com.flying.orm.rdb.support.ListQueryFixture.super.queryMapped(request, options, mapper, rowLimit);
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new AssertionError("unexpected write");
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
            throw new AssertionError("unexpected write");
        }
    }
}
