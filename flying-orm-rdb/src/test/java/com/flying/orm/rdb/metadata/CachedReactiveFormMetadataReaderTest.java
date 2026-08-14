package com.flying.orm.rdb.metadata;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.ColumnMetadata;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.isolation.IsolationContext;
import com.flying.orm.rdb.isolation.IsolationContexts;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipant;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipationException;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import io.r2dbc.spi.Connection;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.lang.reflect.Proxy;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CachedReactiveFormMetadataReaderTest {

    /**
     * 直接使用 public factory 时，缓存必须先询问 custom participant，把已锁定的路由写入 Context 后再查键；
     * 否则两个物理库的同名表会共同命中 null partition。
     */
    @Test
    void directFactoryCachePartitionsMetadataByCustomTransactionRouteBeforeLookup() {
        AtomicReference<R2dbcTransactionContext> transaction = new AtomicReference<>();
        AtomicInteger participantCalls = new AtomicInteger();
        AtomicInteger metadataQueries = new AtomicInteger();
        ReactiveSqlExecutor executor = customTransactionExecutor(transaction, participantCalls, metadataQueries);
        ReactiveFormMetadataReader reader = ReactiveFormMetadataReaders.cached(
                ReactiveFormMetadataReaders.create(executor, RdbDialect.h2()));
        R2dbcTransactionContext primary = R2dbcTransactionContext.external(externalConnection(), "primary");
        R2dbcTransactionContext archive = R2dbcTransactionContext.external(externalConnection(), "archive");

        transaction.set(primary);
        DynamicForm primaryForm = reader.readForm("users", "Users").block();
        transaction.set(archive);
        DynamicForm archiveForm = reader.readForm("users", "Users").block();

        assertNotSame(primaryForm, archiveForm);
        assertEquals(2, metadataQueries.get());
        assertEquals(2, participantCalls.get());
    }

    /** 缓存命中前也必须拒绝已锁定事务路由与本次隔离路由的冲突。 */
    @Test
    void directFactoryCacheRejectsConflictingTransactionRouteBeforeCacheHit() {
        AtomicReference<R2dbcTransactionContext> transaction = new AtomicReference<>();
        AtomicInteger participantCalls = new AtomicInteger();
        AtomicInteger metadataQueries = new AtomicInteger();
        ReactiveSqlExecutor executor = customTransactionExecutor(transaction, participantCalls, metadataQueries);
        ReactiveFormMetadataReader reader = ReactiveFormMetadataReaders.cached(
                ReactiveFormMetadataReaders.create(executor, RdbDialect.h2()));
        IsolationContext archive = IsolationContext.database("tenant", "archive");

        IsolationContexts.with(reader.readForm("users", "Users"), archive).block();
        transaction.set(R2dbcTransactionContext.external(externalConnection(), "primary"));

        StepVerifier.create(IsolationContexts.with(reader.readForm("users", "Users"), archive))
                    .expectErrorSatisfies(error -> {
                        R2dbcTransactionParticipationException rejected = assertInstanceOf(
                                R2dbcTransactionParticipationException.class, error);
                        assertEquals(R2dbcTransactionParticipationException.Reason.ROUTING_IDENTITY_CHANGED,
                                     rejected.reason());
                    })
                    .verify();
        assertEquals(1, metadataQueries.get());
        assertEquals(2, participantCalls.get());
    }

    /** 已有 raw Context 时不应再次委托 custom participant，避免 bootstrap 内外包装重复解析。 */
    @Test
    void directFactoryCacheUsesRawTransactionContextBeforeCustomParticipant() {
        AtomicReference<R2dbcTransactionContext> transaction = new AtomicReference<>();
        AtomicInteger participantCalls = new AtomicInteger();
        AtomicInteger metadataQueries = new AtomicInteger();
        ReactiveSqlExecutor executor = customTransactionExecutor(transaction, participantCalls, metadataQueries);
        ReactiveFormMetadataReader reader = ReactiveFormMetadataReaders.cached(
                ReactiveFormMetadataReaders.create(executor, RdbDialect.h2()));
        R2dbcTransactionContext primary = R2dbcTransactionContext.external(externalConnection(), "primary");

        reader.readForm("users", "Users")
              .contextWrite(context -> R2dbcTransactionParticipant.bind(context, primary))
              .block();

        assertEquals(1, metadataQueries.get());
        assertEquals(0, participantCalls.get());
    }

    /** public unsupported-reader factory 也不能把调用方提供的无界方言文本复制到异步异常。 */
    @Test
    void unsupportedReaderDoesNotEchoCallerDialectName() {
        String dialectName = "review-secret-dialect-" + "x".repeat(8192);

        StepVerifier.create(ReactiveFormMetadataReaders.unsupported(dialectName).readForm("users", "Users"))
                    .expectErrorSatisfies(error -> {
                        assertInstanceOf(UnsupportedOperationException.class, error);
                        assertFalse(error.getMessage().contains(dialectName));
                    })
                    .verify();
    }

    @Test
    void doesNotEchoMalformedRuntimeTableNameDuringInvalidation() {
        String table = "tenant.users.extra--must-not-leak";
        MetadataCacheInvalidator invalidator = (MetadataCacheInvalidator) ReactiveFormMetadataReaders.cached(
                new CountingMetadataReader());

        IllegalArgumentException error = assertThrows(IllegalArgumentException.class,
                () -> invalidator.invalidate(table));

        assertFalse(error.getMessage().contains(table));
    }

    @Test
    void doesNotEchoMalformedRuntimeTableNameWhileBuildingMetadataCacheKey() {
        String table = "tenant.users.extra--must-not-leak";
        ReactiveFormMetadataReader reader = ReactiveFormMetadataReaders.cached(new CountingMetadataReader());

        IllegalArgumentException formError = assertThrows(IllegalArgumentException.class,
                () -> reader.readForm("users", table));
        IllegalArgumentException tableError = assertThrows(IllegalArgumentException.class,
                () -> reader.readTable(table));

        assertFalse(formError.getMessage().contains(table));
        assertFalse(tableError.getMessage().contains(table));
    }

    @Test
    void cachesFormMetadataUntilTableIsInvalidated() {
        CountingMetadataReader delegate = new CountingMetadataReader();
        ReactiveFormMetadataReader reader = CachedReactiveFormMetadataReader.create(
                delegate,
                new CacheRegionPolicy(true, 128, 16, Duration.ofMinutes(5), true));

        DynamicForm first = reader.readForm("users", "Users").block();
        DynamicForm second = reader.readForm("users", "Users").block();

        assertSame(first, second);
        assertEquals(1, delegate.formReads());

        ((MetadataCacheInvalidator) reader).invalidate("Users");

        DynamicForm afterInvalidation = reader.readForm("users", "Users").block();
        assertNotSame(first, afterInvalidation);
        assertEquals(2, delegate.formReads());
    }

    /** 动态路由库和会话 schema 都属于元数据身份，同名表不能跨上下文共用缓存。 */
    @Test
    void isolatesMetadataByDatabaseRouteAndContextSchema() {
        CountingMetadataReader delegate = new CountingMetadataReader();
        ReactiveFormMetadataReader reader = ReactiveFormMetadataReaders.cached(delegate);
        IsolationContext primary = IsolationContext.database("tenant", "primary");
        IsolationContext archive = IsolationContext.database("tenant", "archive");

        DynamicForm primaryForm = IsolationContexts.with(reader.readForm("users", "Users"), primary).block();
        DynamicForm archiveForm = IsolationContexts.with(reader.readForm("users", "Users"), archive).block();
        assertNotSame(primaryForm, archiveForm);
        assertSame(primaryForm, IsolationContexts.with(reader.readForm("users", "Users"), primary).block());
        assertEquals(2, delegate.formReads());

        IsolationContext publicSchema = IsolationContext.shared().withSchema("public");
        IsolationContext auditSchema = IsolationContext.shared().withSchema("audit");
        DynamicForm publicForm = IsolationContexts.with(reader.readForm("events", "Events"), publicSchema).block();
        DynamicForm auditForm = IsolationContexts.with(reader.readForm("events", "Events"), auditSchema).block();
        assertNotSame(publicForm, auditForm);
        assertSame(publicForm,
                   IsolationContexts.with(reader.readForm("events", "Events"), publicSchema).block());
        assertEquals(4, delegate.formReads());
    }

    /** 外部事务单独绑定路由时，同名表不能沿用未分区的元数据缓存项。 */
    @Test
    void isolatesMetadataByExternalTransactionRoutingIdentity() {
        CountingMetadataReader delegate = new CountingMetadataReader();
        ReactiveFormMetadataReader reader = ReactiveFormMetadataReaders.cached(delegate);
        R2dbcTransactionContext primary = R2dbcTransactionContext.external(externalConnection(), "primary");
        R2dbcTransactionContext archive = R2dbcTransactionContext.external(externalConnection(), "archive");

        DynamicForm primaryForm = reader.readForm("users", "Users")
                                         .contextWrite(context -> R2dbcTransactionParticipant.bind(context, primary))
                                         .block();
        DynamicForm archiveForm = reader.readForm("users", "Users")
                                         .contextWrite(context -> R2dbcTransactionParticipant.bind(context, archive))
                                         .block();

        assertNotSame(primaryForm, archiveForm);
        assertEquals(2, delegate.formReads());
    }

    /** 已锁定的外部事务与本次隔离路由冲突时，缓存命中前也必须拒绝。 */
    @Test
    void rejectsConflictingIsolationRouteBeforeMetadataCacheHit() {
        CountingMetadataReader delegate = new CountingMetadataReader();
        ReactiveFormMetadataReader reader = ReactiveFormMetadataReaders.cached(delegate);
        IsolationContext archive = IsolationContext.database("tenant", "archive");
        R2dbcTransactionContext primaryTransaction = R2dbcTransactionContext.external(externalConnection(), "primary");

        IsolationContexts.with(reader.readForm("users", "Users"), archive).block();

        StepVerifier.create(IsolationContexts.with(reader.readForm("users", "Users"), archive)
                                            .contextWrite(context -> R2dbcTransactionParticipant.bind(
                                                    context, primaryTransaction)))
                    .expectErrorSatisfies(error -> {
                        R2dbcTransactionParticipationException rejected = assertInstanceOf(
                                R2dbcTransactionParticipationException.class, error);
                        assertEquals(R2dbcTransactionParticipationException.Reason.ROUTING_IDENTITY_CHANGED,
                                     rejected.reason());
                    })
                    .verify();
        assertEquals(1, delegate.formReads());
    }

    @Test
    void cachesTableMetadataAndCanClearEverything() {
        CountingMetadataReader delegate = new CountingMetadataReader();
        ReactiveFormMetadataReader reader = ReactiveFormMetadataReaders.cached(delegate);

        TableMetadata first = reader.readTable("public", "Users").block();
        TableMetadata second = reader.readTable("public", "Users").block();

        assertSame(first, second);
        assertEquals(1, delegate.tableReads());

        ((MetadataCacheInvalidator) reader).invalidateAll();

        StepVerifier.create(reader.readTable("public", "Users"))
                    .expectNextMatches(afterClear -> afterClear != first)
                    .verifyComplete();
        assertEquals(2, delegate.tableReads());
    }

    @Test
    void invalidatesAllSchemasByTableOrOneSchemaByQualifiedTable() {
        CountingMetadataReader delegate = new CountingMetadataReader();
        ReactiveFormMetadataReader reader = ReactiveFormMetadataReaders.cached(delegate);
        MetadataCacheInvalidator invalidator = (MetadataCacheInvalidator) reader;

        TableMetadata publicFirst = reader.readTable("public", "Users").block();
        TableMetadata auditFirst = reader.readTable("audit", "Users").block();
        invalidator.invalidate("public.Users");

        assertNotSame(publicFirst, reader.readTable("public", "Users").block());
        assertSame(auditFirst, reader.readTable("audit", "Users").block());

        TableMetadata publicSecond = reader.readTable("public", "Users").block();
        TableMetadata auditSecond = reader.readTable("audit", "Users").block();
        invalidator.invalidate("Users");

        assertNotSame(publicSecond, reader.readTable("public", "Users").block());
        assertNotSame(auditSecond, reader.readTable("audit", "Users").block());
    }

    @Test
    void exposesCaffeineStatsForMetadataCache() {
        CountingMetadataReader delegate = new CountingMetadataReader();
        CachedReactiveFormMetadataReader reader = CachedReactiveFormMetadataReader.create(delegate);

        reader.readForm("users", "Users").block();
        reader.readForm("users", "Users").block();

        CacheStats stats = reader.formStats();
        assertEquals(1, stats.missCount());
        assertEquals(1, stats.hitCount());
        assertEquals(1, delegate.formReads());
    }

    @Test
    void exposesFrameworkIndependentSnapshotForBothCacheRegions() {
        CountingMetadataReader delegate = new CountingMetadataReader();
        CachedReactiveFormMetadataReader reader = CachedReactiveFormMetadataReader.create(delegate);

        reader.readForm("users", "Users").block();
        reader.readForm("users", "Users").block();
        reader.readTable("public", "Users").block();

        MetadataCacheSnapshot snapshot = reader.snapshot();
        assertEquals(1, snapshot.forms().entries());
        assertEquals(1, snapshot.forms().hitCount());
        assertEquals(1, snapshot.forms().missCount());
        assertEquals(1, snapshot.forms().loadSuccessCount());
        assertEquals(1, snapshot.tables().entries());
        assertEquals(2, snapshot.combined().entries());
        assertEquals(3, snapshot.combined().requestCount());
        assertEquals(2, snapshot.combined().loadSuccessCount());
    }

    @Test
    void snapshotCountsTheReactiveLoadResultInsteadOfMonoInsertion() {
        AtomicInteger attempts = new AtomicInteger();
        ReactiveFormMetadataReader delegate = new ReactiveFormMetadataReader() {
            @Override
            public Mono<DynamicForm> readForm(String formId, String table) {
                if (attempts.incrementAndGet() == 1) {
                    return Mono.error(new IllegalStateException("metadata unavailable"));
                }
                return Mono.just(CountingMetadataReader.form(formId, table, 2));
            }

            @Override
            public Mono<DynamicForm> readForm(String formId, String schema, String table) {
                return readForm(formId, schema + "." + table);
            }
        };
        CachedReactiveFormMetadataReader reader = CachedReactiveFormMetadataReader.create(delegate);

        StepVerifier.create(reader.readForm("users", "Users"))
                    .expectErrorMessage("metadata unavailable")
                    .verify();
        StepVerifier.create(reader.readForm("users", "Users"))
                    .expectNextCount(1)
                    .verifyComplete();

        MetadataCacheSnapshot.Region forms = reader.snapshot().forms();
        assertEquals(2, attempts.get());
        assertEquals(1, forms.loadFailureCount());
        assertEquals(1, forms.loadSuccessCount());
        assertEquals(2, forms.loadCount());
    }

    @Test
    void enforcesMaximumWeightBeforeTakingSnapshot() {
        CountingMetadataReader delegate = new CountingMetadataReader();
        CachedReactiveFormMetadataReader reader = CachedReactiveFormMetadataReader.create(
                delegate,
                new CacheRegionPolicy(true, 2, 2, Duration.ofMinutes(5), true));

        reader.readForm("users", "Users").block();
        reader.readForm("orders", "Orders").block();

        MetadataCacheSnapshot.Region forms = reader.snapshot().forms();
        assertTrue(forms.entries() <= 1);
        assertTrue(forms.evictionCount() >= 1);
    }

    @Test
    void enforcesOneSharedWeightedBoundaryAcrossFormsAndTables() {
        CountingMetadataReader delegate = new CountingMetadataReader();
        CacheRegionPolicy policy = new CacheRegionPolicy(true,
                                                         2,
                                                         2,
                                                         Duration.ofMinutes(5),
                                                         true);
        CachedReactiveFormMetadataReader reader = CachedReactiveFormMetadataReader.create(delegate, policy);

        reader.readForm("users", "Users").block();
        reader.readTable("public", "Users").block();

        assertTrue(reader.snapshot().combined().entries() <= 1);
    }

    @Test
    void bypassesDisabledCacheAndHonorsDisabledStatistics() {
        CountingMetadataReader delegate = new CountingMetadataReader();
        CachedReactiveFormMetadataReader reader = CachedReactiveFormMetadataReader.create(
                delegate,
                CacheRegionPolicy.disabled());

        reader.readForm("users", "Users").block();
        reader.readForm("users", "Users").block();

        assertEquals(2, delegate.formReads());
        assertEquals(0, reader.snapshot().combined().requestCount());
    }

    @Test
    void doesNotRecordStatsWhenPolicyDisablesThem() {
        CountingMetadataReader delegate = new CountingMetadataReader();
        CacheRegionPolicy policy = new CacheRegionPolicy(true,
                                                         32,
                                                         8,
                                                         Duration.ofMinutes(5),
                                                         false);
        CachedReactiveFormMetadataReader reader = CachedReactiveFormMetadataReader.create(delegate, policy);

        reader.readForm("users", "Users").block();
        reader.readForm("users", "Users").block();

        assertEquals(1, reader.snapshot().forms().entries());
        assertEquals(0, reader.snapshot().forms().requestCount());
        assertEquals(0, reader.snapshot().forms().loadCount());
    }

    @Test
    void staleFailureAfterInvalidationCannotRemoveTheNewCacheEntry() {
        Sinks.One<DynamicForm> staleLoad = Sinks.one();
        AtomicInteger attempts = new AtomicInteger();
        ReactiveFormMetadataReader delegate = new ReactiveFormMetadataReader() {
            @Override
            public Mono<DynamicForm> readForm(String formId, String table) {
                int attempt = attempts.incrementAndGet();
                return attempt == 1
                        ? staleLoad.asMono()
                        : Mono.just(CountingMetadataReader.form(formId, table, attempt));
            }

            @Override
            public Mono<DynamicForm> readForm(String formId, String schema, String table) {
                return readForm(formId, schema + "." + table);
            }
        };
        CachedReactiveFormMetadataReader reader = CachedReactiveFormMetadataReader.create(delegate);

        CompletableFuture<DynamicForm> staleResult = reader.readForm("users", "Users").toFuture();
        reader.invalidate("Users");
        DynamicForm fresh = reader.readForm("users", "Users").block();

        staleLoad.tryEmitError(new IllegalStateException("stale metadata failed"));
        assertThrows(CompletionException.class, staleResult::join);
        assertSame(fresh, reader.readForm("users", "Users").block());
        assertEquals(2, attempts.get());
    }

    @Test
    void sharesSingleLoadForConcurrentSameKeyReads() throws InterruptedException {
        CountingMetadataReader delegate = new CountingMetadataReader();
        CachedReactiveFormMetadataReader reader = CachedReactiveFormMetadataReader.create(delegate);
        CountDownLatch start = new CountDownLatch(1);
        List<DynamicForm> results = new CopyOnWriteArrayList<>();
        List<Thread> threads = new CopyOnWriteArrayList<>();

        for (int i = 0; i < 16; i++) {
            Thread thread = new Thread(() -> {
                try {
                    start.await();
                    results.add(reader.readForm("users", "Users").block());
                } catch (InterruptedException error) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(error);
                }
            });
            threads.add(thread);
            thread.start();
        }

        start.countDown();
        for (Thread thread : threads) {
            thread.join();
        }

        assertEquals(16, results.size());
        assertEquals(1, delegate.formReads());
    }

    /**
     * 只组装响应式调用时不能提前碰 delegate。真正订阅时才读取元数据和当前上下文。
     */
    @Test
    void defersDelegateInvocationUntilSubscription() {
        AtomicInteger invocations = new AtomicInteger();
        ReactiveFormMetadataReader delegate = new ReactiveFormMetadataReader() {
            @Override
            public Mono<DynamicForm> readForm(String formId, String table) {
                invocations.incrementAndGet();
                return Mono.just(CountingMetadataReader.form(formId, table, 1));
            }

            @Override
            public Mono<DynamicForm> readForm(String formId, String schema, String table) {
                invocations.incrementAndGet();
                return Mono.just(CountingMetadataReader.form(formId, schema + "." + table, 1));
            }
        };
        ReactiveFormMetadataReader reader = ReactiveFormMetadataReaders.cached(delegate);

        Mono<DynamicForm> operation = reader.readForm("users", "Users");
        assertEquals(0, invocations.get());

        StepVerifier.create(operation)
                    .expectNextCount(1)
                    .verifyComplete();
        assertEquals(1, invocations.get());
    }

    private static final class CountingMetadataReader implements ReactiveFormMetadataReader {

        private final AtomicInteger formReads = new AtomicInteger();

        private final AtomicInteger tableReads = new AtomicInteger();

        @Override
        public Mono<DynamicForm> readForm(String formId, String table) {
            return Mono.fromSupplier(() -> form(formId, table, formReads.incrementAndGet()));
        }

        @Override
        public Mono<DynamicForm> readForm(String formId, String schema, String table) {
            return Mono.fromSupplier(() -> form(formId, schema + "." + table, formReads.incrementAndGet()));
        }

        @Override
        public Mono<TableMetadata> readTable(String schema, String table) {
            return Mono.fromSupplier(() -> TableMetadata.builder(schema + "." + table)
                                                        .addColumn(ColumnMetadata.primaryKey(
                                                                "id_" + tableReads.incrementAndGet(),
                                                                "BIGINT"))
                                                        .build());
        }

        private static DynamicForm form(String formId, String table, int readIndex) {
            return DynamicForm.builder(formId, table)
                              .addField(DynamicField.primaryKey("id_" + readIndex, "BIGINT"))
                              .build();
        }

        private int formReads() {
            return formReads.get();
        }

        private int tableReads() {
            return tableReads.get();
        }
    }

    /** 元数据缓存测试不执行 SQL，只需要一个非空的外部事务连接事实。 */
    private static Connection externalConnection() {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                                                   new Class<?>[]{Connection.class},
                                                   (proxy, method, arguments) -> null);
    }

    private static ReactiveSqlExecutor customTransactionExecutor(
            AtomicReference<R2dbcTransactionContext> transaction,
            AtomicInteger participantCalls,
            AtomicInteger metadataQueries) {
        return new ReactiveSqlExecutor() {
            @Override
            public Mono<R2dbcTransactionContext> currentTransaction() {
                return Mono.defer(() -> {
                    participantCalls.incrementAndGet();
                    return Mono.justOrEmpty(transaction.get());
                });
            }

            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.defer(() -> {
                    metadataQueries.incrementAndGet();
                    return Flux.just(DynamicRow.copyOf(Map.of(
                            "COLUMN_NAME", "id", "DATA_TYPE", "bigint", "PRIMARY_KEY", true)));
                });
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.error(new UnsupportedOperationException("metadata reader must not update rows"));
            }
        };
    }
}
