package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.protection.ProtectedConditions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.protection.ProtectedFormLayout;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ContainsProjectionLayoutTest {

    @Test
    void reactiveContainsPublishesVerifiedRowsWithoutWaitingForQueryCompletion() {
        try (Fixture fixture = new Fixture()) {
            ProtectedFieldRuntime.WriteOperation write = fixture.runtime.writeOperation(
                    fixture.form, ProtectedFormLayout.physical(fixture.form), DataScope.none(), ValueCodecRegistry.standard());
            DynamicRow miss = DynamicRow.copyOf(write.prepare(
                    Map.of("id", 1L, "secret", "goodbye", "note", "miss")).ownedValues());
            DynamicRow match = DynamicRow.copyOf(write.prepare(
                    Map.of("id", 2L, "secret", "alphabet", "note", "match")).ownedValues());
            AtomicBoolean cancelled = new AtomicBoolean();
            ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
                public Flux<DynamicRow> query(SqlRequest request) {
                    return Flux.just(miss, match).concatWith(Flux.never()).doOnCancel(() -> cancelled.set(true));
                }
                public Mono<Long> rowsUpdated(SqlRequest request) {
                    return Mono.error(new AssertionError("read must not write"));
                }
            };
            ReactiveFormClient client = ReactiveFormClient.create(executor, fixture.renderer);
            try {
                DynamicRow result = Objects.requireNonNull(client.select(QuerySpec.of(fixture.form, fixture.where)
                        .withProjection(List.of("note", "id"), List.of())).next().block(Duration.ofSeconds(3)));
                assertEquals(List.of("note", "id"), new ArrayList<>(result.keySet()));
                assertEquals(2L, result.get("id"));
                assertEquals("match", result.get("note"));
                assertTrue(cancelled.get(), "taking the first verified row must cancel the unfinished query");
            } finally {
                client.entityModels().close();
            }
        }
    }

    @Test
    void reactiveFinalRowsKeepFilteringOrderAndSubscriptionLocalBudget() {
        try (Fixture fixture = new Fixture()) {
            List<DynamicRow> candidates = List.of(row(1L, "alphabet", "first"),
                    row(2L, "goodbye", "miss"), row(3L, "alphabet soup", "third"));
            long matchedBytes = com.flying.orm.rdb.batch.BatchMemoryBudget.estimateRowBytes(candidates.getFirst())
                    + com.flying.orm.rdb.batch.BatchMemoryBudget.estimateRowBytes(candidates.getLast());
            Mono<List<DynamicRow>> result = fixture.support.finish(fixture.form, fixture.query,
                    Flux.fromIterable(candidates), List.of("note", "id"), SensitiveDisplayMode.FULL,
                    com.flying.orm.rdb.execution.SqlExecutionOptions.unlimited().withMaxResultBytes(matchedBytes)).collectList();
            for (int subscription = 0; subscription < 2; subscription++) {
                List<DynamicRow> rows = Objects.requireNonNull(result.block(Duration.ofSeconds(3)));
                assertEquals(List.of(1L, 3L), rows.stream().map(value -> value.get("id")).toList());
                assertEquals(List.of("first", "third"), rows.stream().map(value -> value.get("note")).toList());
                rows.forEach(value -> assertEquals(List.of("note", "id"), new ArrayList<>(value.keySet())));
                assertEquals(1, layoutCount(rows));
            }
        }
    }

    @TestFactory
    Stream<DynamicTest> reactiveContainsFinishesEachCandidateBeforeSourceCompletion() {
        return Stream.of("select", "page", "cursor").map(shape -> DynamicTest.dynamicTest(shape, () -> {
            try (Fixture fixture = new Fixture()) {
                Object encrypted = fixture.runtime.writeOperation(fixture.form, ProtectedFormLayout.physical(fixture.form),
                        DataScope.none(), ValueCodecRegistry.standard())
                        .prepare(Map.of("id", 1L, "secret", "alphabet")).ownedValues().get("secret");
                DynamicRow missingProjection = DynamicRow.copyOf(Map.of("id", 1L, "secret", encrypted));
                AtomicBoolean cancelled = new AtomicBoolean();
                ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
                    public Flux<DynamicRow> query(SqlRequest request) {
                        return Flux.just(missingProjection).concatWith(Flux.never())
                                .doOnCancel(() -> cancelled.set(true));
                    }
                    public Mono<Long> rowsUpdated(SqlRequest request) {
                        return Mono.error(new AssertionError("read must not write"));
                    }
                };
                ReactiveFormClient client = ReactiveFormClient.create(executor, fixture.renderer);
                try {
                    QuerySpec spec = QuerySpec.of(fixture.form, fixture.where)
                            .withProjection(List.of("note", "id"), List.of());
                    Mono<?> result = switch (shape) {
                        case "select" -> client.select(spec).collectList();
                        case "page" -> client.page(spec, PageQuery.of(1, 2));
                        default -> client.keysetPage(spec, KeysetPageQuery.first(2, KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST)));
                    };
                    CompletableFuture<?> future = result.toFuture();
                    try {
                        ExecutionException failure = assertThrows(ExecutionException.class,
                                () -> future.get(3, TimeUnit.SECONDS));
                        assertEquals("protected contains result is missing a projected field",
                                failure.getCause().getMessage());
                        assertTrue(cancelled.get());
                    } finally {
                        future.cancel(true);
                    }
                } finally {
                    client.entityModels().close();
                }
            }
        }));
    }

    @TestFactory
    Stream<DynamicTest> reactiveContainsDecodesWithoutWaitingForAllRawRows() {
        return Stream.of("select", "page", "cursor").map(shape -> DynamicTest.dynamicTest(shape, () -> {
            try (Fixture fixture = new Fixture()) {
                DynamicRow invalid = DynamicRow.copyOf(Map.of("id", 1L, "secret", new byte[0], "note", "invalid"));
                RuntimeException expected = assertThrows(RuntimeException.class,
                        () -> fixture.renderer.protection().decryptOperation(fixture.form, DataScope.none()).transform(invalid));
                AtomicBoolean cancelled = new AtomicBoolean();
                ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
                    public Flux<DynamicRow> query(SqlRequest request) {
                        return Flux.just(invalid).concatWith(Flux.never()).doOnCancel(() -> cancelled.set(true));
                    }
                    public Mono<Long> rowsUpdated(SqlRequest request) {
                        return Mono.error(new AssertionError("read must not write"));
                    }
                };
                ReactiveFormClient client = ReactiveFormClient.create(executor, fixture.renderer);
                try {
                    QuerySpec spec = QuerySpec.of(fixture.form, fixture.where);
                    Mono<?> result = switch (shape) {
                        case "select" -> client.select(spec).collectList();
                        case "page" -> client.page(spec, PageQuery.of(1, 2));
                        default -> client.keysetPage(spec, KeysetPageQuery.first(2, KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST)));
                    };
                    CompletableFuture<?> future = result.toFuture();
                    try {
                        ExecutionException failure = assertThrows(ExecutionException.class,
                                () -> future.get(3, TimeUnit.SECONDS));
                        assertEquals(expected.getClass(), failure.getCause().getClass());
                        assertTrue(cancelled.get(), "decoding failure must cancel the unfinished source");
                    } finally {
                        future.cancel(true);
                    }
                } finally {
                    client.entityModels().close();
                }
            }
        }));
    }

    @Test
    void groupedOffsetPageStillRejectsContainsAndEncryptedGroups() {
        try (Fixture fixture = new Fixture()) {
            FormOperationPlanner planner = new FormOperationPlanner(fixture.renderer,
                    new FormScopeSupport(fixture.renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                    com.flying.orm.rdb.execution.SqlExecutionOptions.unlimited());
            QuerySpec contains = QuerySpec.of(fixture.form, fixture.where)
                    .withProjection(List.of("note"), List.of("note"));
            IllegalArgumentException search = assertThrows(IllegalArgumentException.class,
                    () -> planner.reads.page(contains, com.flying.orm.core.page.PageQuery.of(1, 2)));
            assertEquals("protected contains search does not support grouped queries", search.getMessage());
            QuerySpec encrypted = QuerySpec.of(fixture.form, ConditionGroup.and().build())
                    .withProjection(List.of("secret"), List.of("secret"));
            IllegalArgumentException grouping = assertThrows(IllegalArgumentException.class,
                    () -> planner.reads.page(encrypted, com.flying.orm.core.page.PageQuery.of(1, 2)));
            assertEquals("encrypted field must not be used for query grouping", grouping.getMessage());
        }
    }

    @TestFactory
    Stream<DynamicTest> reactiveContainsPreservesReadShapePublicationOnTerminalFailure() {
        return Stream.of("select", "page", "cursor").map(shape -> DynamicTest.dynamicTest(shape, () -> {
            try (Fixture fixture = new Fixture()) {
                Object encrypted = fixture.runtime.writeOperation(fixture.form, ProtectedFormLayout.physical(fixture.form),
                        DataScope.none(), ValueCodecRegistry.standard())
                        .prepare(Map.of("id", 1L, "secret", "alphabet", "note", "seed")).ownedValues().get("secret");
                DynamicRow raw = DynamicRow.copyOf(Map.of("id", 1L, "secret", encrypted, "note", "seed"));
                IllegalStateException failure = new IllegalStateException("source cleanup failed");
                ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
                    public Flux<DynamicRow> query(SqlRequest request) {
                        return Flux.just(raw).concatWith(Mono.error(failure));
                    }
                    public Mono<Long> rowsUpdated(SqlRequest request) {
                        return Mono.error(new AssertionError("read must not write"));
                    }
                };
                ReactiveFormClient client = ReactiveFormClient.create(executor, fixture.renderer);
                try {
                    QuerySpec spec = QuerySpec.of(fixture.form, fixture.where);
                    AtomicInteger published = new AtomicInteger();
                    Mono<?> result = switch (shape) {
                        case "select" -> client.select(spec).doOnNext(row -> published.incrementAndGet()).then();
                        case "page" -> client.page(spec, PageQuery.of(1, 2)).doOnNext(page -> published.incrementAndGet());
                        default -> client.keysetPage(spec, KeysetPageQuery.first(2, KeysetSort.asc("id", com.flying.orm.core.page.NullOrder.LAST)))
                                .doOnNext(page -> published.incrementAndGet());
                    };
                    assertSame(failure, assertThrows(IllegalStateException.class,
                            () -> result.block(Duration.ofSeconds(3))));
                    assertEquals(shape.equals("select") ? 1 : 0, published.get(),
                            "row streams may emit before failure; a page needs successful completion");
                } finally {
                    client.entityModels().close();
                }
            }
        }));
    }

    @Test
    void narrowProjectionReusesOneLayoutAndActualEntityBinding() {
        assertSharedProjection(List.of("id", "note"));
    }

    @Test
    void reorderedProjectionReusesOneLayoutAndActualEntityBinding() {
        assertSharedProjection(List.of("note", "secret", "id"));
    }

    private static void assertSharedProjection(List<String> projection) {
        try (Fixture fixture = new Fixture()) {
            DynamicRow first = row(1L, "alphabet", "first");
            DynamicRow second = first.withValues(Map.of(0, 2L, 2, "second"));
            DynamicRow third = first.withValues(Map.of(0, 3L, 2, "third"));
            List<DynamicRow> source = List.of(first, second, third);
            assertEquals(1, layoutCount(source));

            List<DynamicRow> result = fixture.finish(source, projection);

            assertEquals(List.of(1L, 2L, 3L), result.stream().map(value -> value.get("id")).toList());
            assertEquals(List.of("first", "second", "third"),
                    result.stream().map(value -> value.get("note")).toList());
            result.forEach(value -> assertEquals(projection, new ArrayList<>(value.keySet())));
            RowMapper<Projection> mapper = RowMapper.of(Projection.class);
            List<Projection> mapped = result.stream().map(mapper::map).toList();
            assertEquals(List.of(1L, 2L, 3L), mapped.stream().map(Projection::id).toList());
            assertEquals(1, actualBindingCount(result, mapper),
                    "one fixed result projection must reuse its actual MappingPlan BoundWriter");
            assertEquals(1, layoutCount(result), "projection metadata must not be rebuilt for each row");
        }
    }

    @Test
    void heterogeneousSourceOrdersPreserveNamedValuesAndNullSlots() {
        try (Fixture fixture = new Fixture()) {
            DynamicRow first = row(1L, "alphabet", "first");
            LinkedHashMap<String, Object> reordered = new LinkedHashMap<>();
            reordered.put("note", null);
            reordered.put("secret", "alphabet soup");
            reordered.put("id", 2L);
            reordered.put("driver_extra", "must remain hidden");
            List<DynamicRow> result = fixture.finish(
                    List.of(first, DynamicRow.copyOf(reordered)), List.of("note", "id"));

            assertEquals(List.of("note", "id"), new ArrayList<>(result.getFirst().keySet()));
            assertEquals("first", result.getFirst().get("note"));
            assertEquals(1L, result.getFirst().get("id"));
            assertNull(result.getLast().get("note"), "every output slot must be replaced, including null");
            assertEquals(2L, result.getLast().get("id"));
            assertEquals(1, layoutCount(result));
            DynamicRow changed = result.getLast().withValues(Map.of(0, "changed"));
            assertEquals("changed", changed.get("note"));
            assertNull(result.getLast().get("note"));
            assertEquals("first", result.getFirst().get("note"));
            assertEquals("first", first.get("note"));
        }
    }

    @Test
    void missingProjectedColumnStillFailsAfterAValidFirstRow() {
        try (Fixture fixture = new Fixture()) {
            IllegalStateException failure = assertThrows(IllegalStateException.class,
                    () -> fixture.finish(List.of(row(1L, "alphabet", "first"),
                                    DynamicRow.copyOf(Map.of("id", 2L, "secret", "alphabet"))),
                            List.of("note", "id")));
            assertEquals("protected contains result is missing a projected field", failure.getMessage());
        }
    }

    @Test
    void fullOrderedProjectionReturnsOriginalRowsAndEmptyMatchesStayEmpty() {
        try (Fixture fixture = new Fixture()) {
            DynamicRow first = row(1L, "alphabet", null);
            DynamicRow second = row(2L, "alphabet soup", "second");
            List<DynamicRow> result = fixture.finish(List.of(first, second), List.of("id", "secret", "note"));
            assertSame(first, result.getFirst());
            assertSame(second, result.getLast());
            assertTrue(fixture.finish(List.of(), List.of("id")).isEmpty());
            assertTrue(fixture.finish(List.of(row(3L, "goodbye", "miss")), List.of("id")).isEmpty());
        }
    }

    @Test
    void maskingAndPlaintextVerificationPrecedeTheSharedProjection() {
        try (Fixture fixture = new Fixture()) {
            List<DynamicRow> result = List.of(row(1L, "alphabet", "first"), row(2L, "goodbye", "false positive"),
                            row(3L, "alphabet soup", "third")).stream()
                    .map(fixture.support.rowFinisher(fixture.form, fixture.query,
                            List.of("secret", "id"), SensitiveDisplayMode.MASKED,
                            com.flying.orm.rdb.execution.SqlExecutionOptions.unlimited()))
                    .filter(java.util.Objects::nonNull).toList();
            assertEquals(List.of(1L, 3L), result.stream().map(value -> value.get("id")).toList());
            assertEquals(List.of("********", "*************"),
                    result.stream().map(value -> value.get("secret")).toList());
            result.forEach(value -> assertEquals(List.of("secret", "id"), new ArrayList<>(value.keySet())));
            assertEquals(1, layoutCount(result));
        }
    }

    @Test
    void reactivePublicQueryKeepsProjectionStateInsideEachSubscription() {
        try (Fixture fixture = new Fixture()) {
            Object encrypted = fixture.runtime.writeOperation(fixture.form, ProtectedFormLayout.physical(fixture.form),
                    DataScope.none(), ValueCodecRegistry.standard())
                    .prepare(Map.of("id", 1L, "secret", "alphabet", "note", "seed")).ownedValues().get("secret");
            LinkedHashMap<String, Object> raw = new LinkedHashMap<>();
            raw.put("id", 1L);
            raw.put("secret", encrypted);
            raw.put("note", "seed");
            DynamicRow source = DynamicRow.copyOf(raw);
            AtomicInteger subscriptions = new AtomicInteger();
            ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
                @Override
                public Flux<DynamicRow> query(SqlRequest request) {
                    return Flux.defer(() -> {
                        long base = subscriptions.incrementAndGet() * 10L;
                        return Flux.just(source.withValues(Map.of(0, base + 1, 2, "first-" + base)),
                                source.withValues(Map.of(0, base + 2, 2, "second-" + base)));
                    });
                }

                @Override
                public Mono<Long> rowsUpdated(SqlRequest request) {
                    return Mono.error(new AssertionError("query test must not write"));
                }

                @Override
                public Mono<BatchExecutionEvidence> writeBatch(BatchWriteRequest request, java.util.function.LongFunction<? extends org.reactivestreams.Publisher<Void>> rowCompleted) {
                    return Mono.error(new AssertionError("query test must not write batches"));
                }
            };
            ReactiveFormClient client = ReactiveFormClient.create(executor, fixture.renderer);
            try {
                Flux<DynamicRow> query = client.select(QuerySpec.of(fixture.form, fixture.where)
                        .withProjection(List.of("note", "id"), List.of()));
                List<DynamicRow> first = Objects.requireNonNull(query.collectList().block(Duration.ofSeconds(5)));
                List<DynamicRow> second = Objects.requireNonNull(query.collectList().block(Duration.ofSeconds(5)));

                assertEquals(List.of(11L, 12L), first.stream().map(value -> value.get("id")).toList());
                assertEquals(List.of(21L, 22L), second.stream().map(value -> value.get("id")).toList());
                assertEquals("first-10", first.getFirst().get("note"));
                assertEquals("first-20", second.getFirst().get("note"));
                assertEquals(1, layoutCount(first));
                assertEquals(1, layoutCount(second));
                Object marker = new Object();
                assertNotSame(first.getFirst().mappingBinding(marker, Object::new),
                        second.getFirst().mappingBinding(marker, Object::new),
                        "projection bindings must not retain values or state across subscriptions");
            } finally {
                client.entityModels().close();
            }
        }
    }

    private static int actualBindingCount(List<DynamicRow> rows, RowMapper<?> mapper) {
        Set<Object> bindings = Collections.newSetFromMap(new IdentityHashMap<>());
        for (DynamicRow row : rows) {
            bindings.add(row.mappingBinding(mapper, () -> {
                throw new AssertionError("MappingPlan.map must already have installed its real BoundWriter");
            }));
        }
        return bindings.size();
    }

    private static int layoutCount(List<DynamicRow> rows) {
        Object marker = new Object();
        AtomicInteger factories = new AtomicInteger();
        rows.forEach(row -> row.mappingBinding(marker, () -> {
            factories.incrementAndGet();
            return new Object();
        }));
        return factories.get();
    }

    private static DynamicRow row(long id, String secret, String note) {
        LinkedHashMap<String, Object> values = new LinkedHashMap<>();
        values.put("id", id);
        values.put("secret", secret);
        values.put("note", note);
        return DynamicRow.copyOf(values);
    }

    record Projection(Long id, String secret, String note) { }

    private static final class Fixture implements AutoCloseable {
        private final DynamicForm form = DynamicForm.builder("contains-layout", "contains_layout")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .addField(DynamicField.of("note", "VARCHAR"))
                .encrypted("secret", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.CONTAINS).build())
                .masked("secret", MaskedFieldDefinition.builder("full").build())
                .build();
        private final ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]));
        private final FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql())
                .withProtectedFields(runtime);
        private final ProtectedContainsResultSupport support = new ProtectedContainsResultSupport(renderer);
        private final ConditionGroup where = ConditionGroup.and()
                .add(ProtectedConditions.contains("secret", "pha")).build();
        private final ProtectedFieldRuntime.PreparedContainsQuery query = renderer.protection()
                .prepareContainsQuery(form, form, where, DataScope.none()).orElseThrow();

        private List<DynamicRow> finish(List<DynamicRow> rows, List<String> fields) {
            return rows.stream().map(support.rowFinisher(form, query, fields, SensitiveDisplayMode.FULL,
                    com.flying.orm.rdb.execution.SqlExecutionOptions.unlimited()))
                    .filter(java.util.Objects::nonNull).toList();
        }

        @Override
        public void close() {
            runtime.close();
        }
    }
}
