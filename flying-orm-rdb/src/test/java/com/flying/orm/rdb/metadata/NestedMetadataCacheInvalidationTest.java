package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.ReactiveSchemaClient;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NestedMetadataCacheInvalidationTest {

    private static final List<Consumer<MetadataCacheInvalidator>> INVALIDATIONS = List.of(
            cache -> cache.invalidate("accounts"),
            cache -> cache.invalidate("public", "accounts"),
            cache -> cache.invalidate(RelationIdentity.of(null, "public", "accounts")),
            MetadataCacheInvalidator::invalidateAll,
            cache -> cache.invalidate("public.accounts"),
            cache -> cache.invalidate(RelationIdentity.of("catalog", "public", "accounts")));

    @TestFactory
    List<DynamicTest> nestedInvalidationRefreshesBothMetadataKinds() {
        List<DynamicTest> tests = new ArrayList<>();
        for (int mode = 0; mode < INVALIDATIONS.size(); mode++) {
            Consumer<MetadataCacheInvalidator> invalidate = INVALIDATIONS.get(mode);
            for (boolean enabled : List.of(true, false)) {
                tests.add(DynamicTest.dynamicTest("mode=" + mode + "/enabled=" + enabled, () -> {
                    MutableReader source = new MutableReader();
                    ReactiveFormMetadataCache inner = ReactiveFormMetadataReaders.cached(source);
                    CacheRegionPolicy policy = enabled
                            ? CacheRegionPolicy.metadataDefaults() : CacheRegionPolicy.disabled();
                    ReactiveFormMetadataCache outer = ReactiveFormMetadataReaders.cached(
                            inner, policy, MetadataCacheInvalidator.none());
                    assertColumns(outer, 1);
                    assertEquals(2, source.reads);
                    source.current = form(true);
                    invalidate.accept(outer);
                    assertColumns(outer, 2);
                    assertColumns(inner, 2);
                    assertEquals(4, source.reads, "each metadata kind must reload exactly once");
                }));
            }
        }
        return tests;
    }

    @TestFactory
    List<DynamicTest> schemaExecutionInvalidatesNestedCrudCaches() {
        List<DynamicTest> tests = new ArrayList<>();
        for (RdbDialect dialect : List.of(RdbDialect.mysql(), RdbDialect.postgresql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            tests.add(DynamicTest.dynamicTest(dialect.name(), () -> {
                MutableReader source = new MutableReader();
                ReactiveFormMetadataCache cache = ReactiveFormMetadataReaders.cached(
                        ReactiveFormMetadataReaders.cached(source));
                assertColumns(cache, 1);
                List<SqlRequest> executed = new ArrayList<>();
                ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
                    @Override
                    public Flux<DynamicRow> query(SqlRequest request) {
                        return Flux.error(new AssertionError("unexpected query"));
                    }

                    @Override
                    public Mono<Long> rowsUpdated(SqlRequest request) {
                        return Mono.fromSupplier(() -> {
                            executed.add(request);
                            source.current = form(true);
                            return 0L;
                        });
                    }
                };
                var result = ReactiveSchemaClient.create(executor, dialect)
                        .createOrAlterDetailed(form(true), List.of(), cache).block();
                assertNotNull(result);
                assertEquals(0L, result.rowsUpdated());
                assertEquals(1, executed.size());
                assertTrue(executed.getFirst().sql().contains("note"));
                assertColumns(cache, 2);
                assertEquals(5, source.reads, "two initial loads, one schema read, two reloads");
            }));
        }
        return tests;
    }

    @TestFactory
    List<DynamicTest> directlySharedDelegateAndDependentAreNotNotifiedTwice() {
        List<DynamicTest> tests = new ArrayList<>();
        for (int mode = 0; mode < INVALIDATIONS.size(); mode++) {
            Consumer<MetadataCacheInvalidator> invalidate = INVALIDATIONS.get(mode);
            tests.add(DynamicTest.dynamicTest("mode=" + mode, () -> {
                InvalidatingReader source = new InvalidatingReader(false);
                var cache = ReactiveFormMetadataReaders.cached(
                        source, CacheRegionPolicy.metadataDefaults(), source);
                invalidate.accept(cache);
                assertEquals(1, source.notifications);
            }));
        }
        return tests;
    }

    @TestFactory
    List<DynamicTest> failingTargetDoesNotPreventOtherInvalidation() {
        List<DynamicTest> tests = new ArrayList<>();
        for (int mode = 0; mode < INVALIDATIONS.size(); mode++) {
            Consumer<MetadataCacheInvalidator> invalidate = INVALIDATIONS.get(mode);
            for (boolean delegateFails : List.of(true, false)) {
                tests.add(DynamicTest.dynamicTest("mode=" + mode + "/delegateFails=" + delegateFails, () -> {
                    InvalidatingReader source = new InvalidatingReader(delegateFails);
                    InvalidatingReader dependent = new InvalidatingReader(!delegateFails);
                    var cache = ReactiveFormMetadataReaders.cached(
                            source, CacheRegionPolicy.metadataDefaults(), dependent);
                    assertColumns(cache, 1);
                    source.current = form(true);
                    RuntimeException failure = assertThrows(RuntimeException.class, () -> invalidate.accept(cache));
                    assertTrue(failure == (delegateFails ? source.failure : dependent.failure)
                            || failure.getCause() == (delegateFails ? source.failure : dependent.failure));
                    assertEquals(1, source.notifications);
                    assertEquals(1, dependent.notifications);
                    assertColumns(cache, 2);
                }));
            }
        }
        return tests;
    }

    private static void assertColumns(ReactiveFormMetadataReader reader, int count) {
        assertEquals(count, reader.readForm("accounts", "public", "accounts").block().fields().size());
        assertEquals(count, reader.readTable("public", "accounts").block().columns().size());
    }

    @TestFactory
    List<DynamicTest> readDuringDelegateInvalidationCannotRepopulateStaleOuterEntries() {
        List<DynamicTest> tests = new ArrayList<>();
        for (int mode = 0; mode < INVALIDATIONS.size(); mode++) {
            Consumer<MetadataCacheInvalidator> invalidate = INVALIDATIONS.get(mode);
            tests.add(DynamicTest.dynamicTest("mode=" + mode, () -> {
                MutableReader source = new MutableReader();
                var inner = ReactiveFormMetadataReaders.cached(source);
                CountDownLatch invalidating = new CountDownLatch(1);
                CountDownLatch proceed = new CountDownLatch(1);
                // Model an integration cache whose invalidation is concurrently in progress.
                InvalidatingReader delegate = new InvalidatingReader(false) {
                    @Override
                    public Mono<DynamicForm> readForm(String id, String table) {
                        return inner.readForm(id, "public", table);
                    }

                    @Override
                    public void invalidateAll() {
                        invalidating.countDown();
                        try {
                            assertTrue(proceed.await(5, TimeUnit.SECONDS));
                        } catch (InterruptedException failure) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(failure);
                        }
                        inner.invalidateAll();
                    }
                };
                var outer = ReactiveFormMetadataReaders.cached(delegate);
                assertColumns(outer, 1);
                source.current = form(true);
                try (var worker = Executors.newSingleThreadExecutor()) {
                    var pending = worker.submit(() -> invalidate.accept(outer));
                    try {
                        assertTrue(invalidating.await(5, TimeUnit.SECONDS));
                        assertColumns(outer, 1);
                    } finally {
                        proceed.countDown();
                    }
                    pending.get(5, TimeUnit.SECONDS);
                    assertColumns(outer, 2);
                }
            }));
        }
        return tests;
    }

    private static DynamicForm form(boolean note) {
        DynamicForm.Builder builder = DynamicForm.builder("accounts", "accounts")
                .addField(DynamicField.primaryKey("id", "INTEGER"));
        if (note) {
            builder.addField(DynamicField.of("note", "VARCHAR").withLength(100));
        }
        return builder.build();
    }

    private static class MutableReader implements ReactiveFormMetadataReader {
        DynamicForm current = form(false);
        int reads;

        @Override
        public Mono<DynamicForm> readForm(String id, String table) {
            return Mono.fromSupplier(() -> {
                reads++;
                return current;
            });
        }

        @Override
        public Mono<DynamicForm> readForm(String id, String schema, String table) {
            return readForm(id, table);
        }
    }

    private static class InvalidatingReader extends MutableReader implements MetadataCacheInvalidator {
        private final boolean fails;
        private final RuntimeException failure = new IllegalStateException("invalidation failed");
        private int notifications;

        private InvalidatingReader(boolean fails) {
            this.fails = fails;
        }

        @Override
        public void invalidate(String table) {
            invalidateAll();
        }

        @Override
        public void invalidateAll() {
            notifications++;
            if (fails) {
                throw failure;
            }
        }
    }
}
