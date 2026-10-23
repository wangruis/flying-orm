package com.flying.orm.rdb.repository;

import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.protection.FieldProtectionRegistry;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.id.IdGenerator;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.lifecycle.ReactiveEntityListener;
import com.flying.orm.rdb.mapping.EntityMetadata;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

class RepositoryBatchNoopLifecycleTest {

    @Test
    void explicitNoopAndEmptyCompositionDoNotEnableLifecycleWork() {
        for (ReactiveEntityListener<String> listener : java.util.Arrays.asList(
                null, ReactiveEntityListener.<String>none(), ReactiveEntityListener.<String>compose(),
                ReactiveEntityListener.compose(ReactiveEntityListener.<String>none()))) {
            EntityLifecycleDispatcher<String> dispatcher = new EntityLifecycleDispatcher<>(metadata(), listener);
            assertFalse(dispatcher.hasWork());
            Flux<String> rows = Flux.just("one", "two");
            assertSame(rows, dispatcher.postLoad(rows));
            SyncRepositoryLifecycleSupport<String> sync = new SyncRepositoryLifecycleSupport<>(
                    metadata(), listener);
            assertSame(rows, sync.beforeRows(rows, EntityLifecyclePhase.PRE_PERSIST));
            List<String> loaded = List.of("one", "two");
            assertSame(loaded, sync.postLoad(loaded));
            assertEquals(1L, sync.persist("one", () -> 1L));
            java.util.concurrent.atomic.AtomicInteger writes = new java.util.concurrent.atomic.AtomicInteger();
            Mono<Long> pending = dispatcher.persist("one", () -> {
                writes.incrementAndGet();
                return Mono.just(1L);
            });
            assertEquals(0, writes.get());
            assertEquals(1L, pending.block());
            assertEquals(1L, pending.block());
            assertEquals(2, writes.get());
        }
    }

    @Test
    void singleEffectiveListenerStillStartsOnlyOnSubscription() {
        List<String> events = new ArrayList<>();
        ReactiveEntityListener<String> listener = event -> {
            events.add(event.entity());
            return Mono.empty();
        };
        Publisher<Void> pending = ReactiveEntityListener.compose(ReactiveEntityListener.none(), listener,
                ReactiveEntityListener.none()).onEvent(new com.flying.orm.rdb.lifecycle.EntityLifecycleEvent<>(
                        metadata(), "one", EntityLifecyclePhase.PRE_PERSIST, null));
        assertEquals(List.of(), events);
        Mono.from(pending).block();
        Mono.from(pending).block();
        assertEquals(List.of("one", "one"), events);
    }

    @Test
    void effectiveListenersStillRunInOrderAfterSubscription() {
        List<String> events = new ArrayList<>();
        EntityLifecycleDispatcher<String> dispatcher = new EntityLifecycleDispatcher<>(metadata(),
                ReactiveEntityListener.compose(
                        event -> Mono.fromRunnable(() -> events.add("first:" + event.phase())),
                        ReactiveEntityListener.none(),
                        event -> Mono.fromRunnable(() -> events.add("second:" + event.phase()))));
        Mono<Long> pending = dispatcher.persist("one", () -> Mono.fromSupplier(() -> {
            events.add("write");
            return 1L;
        }));
        assertEquals(List.of(), events);
        assertEquals(1L, pending.block());
        assertEquals(List.of("first:PRE_PERSIST", "second:PRE_PERSIST", "write",
                "first:POST_PERSIST", "second:POST_PERSIST"), events);
    }

    @Test
    void syncPreCallbacksAndSourceConsumptionFollowActualDemand() {
        List<String> inputs = new ArrayList<>();
        List<String> callbacks = new ArrayList<>();
        List<String> outputs = new ArrayList<>();
        SyncRepositoryLifecycleSupport<String> lifecycle = new SyncRepositoryLifecycleSupport<>(
                metadata(), event -> Mono.fromRunnable(() -> callbacks.add(event.entity())));
        reactor.core.publisher.BaseSubscriber<String> subscriber = new reactor.core.publisher.BaseSubscriber<>() {
            protected void hookOnSubscribe(org.reactivestreams.Subscription subscription) { request(1); }
            protected void hookOnNext(String value) { outputs.add(value); }
        };
        Flux.from(lifecycle.beforeRows(Flux.just("one", "two").doOnNext(inputs::add),
                EntityLifecyclePhase.PRE_PERSIST)).subscribe(subscriber);
        assertEquals(List.of("one"), inputs);
        assertEquals(List.of("one"), callbacks);
        assertEquals(List.of("one"), outputs);
        subscriber.request(1);
        assertEquals(List.of("one", "two"), inputs);
        assertEquals(inputs, callbacks);
        assertEquals(inputs, outputs);
        subscriber.cancel();
    }

    @Test
    void cancellingAnAsyncPreCancelsBothTheCallbackAndTheSource() {
        java.util.concurrent.atomic.AtomicInteger sourceCancelled = new java.util.concurrent.atomic.AtomicInteger();
        java.util.concurrent.atomic.AtomicInteger preCancelled = new java.util.concurrent.atomic.AtomicInteger();
        SyncRepositoryLifecycleSupport<String> lifecycle = new SyncRepositoryLifecycleSupport<>(
                metadata(), event -> Mono.<Void>never().doOnCancel(preCancelled::incrementAndGet));
        reactor.core.publisher.BaseSubscriber<String> subscriber = new reactor.core.publisher.BaseSubscriber<>() {
            protected void hookOnSubscribe(org.reactivestreams.Subscription subscription) { request(1); }
            protected void hookOnNext(String value) { org.junit.jupiter.api.Assertions.fail("PRE has not completed"); }
        };
        Flux.from(lifecycle.beforeRows(Flux.just("one", "two").doOnCancel(sourceCancelled::incrementAndGet),
                EntityLifecyclePhase.PRE_PERSIST)).subscribe(subscriber);
        subscriber.cancel();
        subscriber.request(1);
        assertEquals(1, sourceCancelled.get());
        assertEquals(1, preCancelled.get());
    }

    @Test
    void reactiveNoopLifecycleUsesTheDirectFormBatchChain() {
        List<String> inputs = new ArrayList<>();
        com.flying.orm.rdb.form.ReactiveFormClient client = BatchEvidenceFixtures.batchFormClient(
                (request, completed) -> {
                    assertFalse(request.generatedKeys().required());
                    org.junit.jupiter.api.Assertions.assertNull(completed);
                    return Flux.from(request.rows()).collectList().map(rows -> {
                        assertEquals(List.of("one", "two"), rows.stream().map(row -> row[0]).toList());
                        return BatchEvidenceFixtures.successful(rows.size());
                    });
                });
        try (com.flying.orm.rdb.mapping.EntityModelRegistry models =
                     com.flying.orm.rdb.mapping.EntityModelRegistry.create(
                             com.flying.orm.rdb.cache.CacheRegionPolicy.disabled())) {
            EntityMetadata<NamedRow> metadata = models.metadata(NamedRow.class);
            ReactiveRepositoryBatchCoordinator<NamedRow> coordinator = new ReactiveRepositoryBatchCoordinator<>(
                    client, metadata.toDynamicForm(), metadata, models.entityValues(NamedRow.class),
                    new EntityLifecycleDispatcher<>(metadata, ReactiveEntityListener.none()),
                    RepositoryEntityIdSupport.create(metadata, models));
            Mono<BatchExecutionEvidence> pending = coordinator.write(
                    Flux.just(new NamedRow("one"), new NamedRow("two")).doOnNext(row -> inputs.add(row.value())),
                    BatchWriteOptions.defaults(), null, RepositoryBatchKind.INSERT, null);
            assertEquals(List.of(), inputs, "assembling the operation must not consume input");
            BatchExecutionEvidence result = pending.block(Duration.ofSeconds(1));
            assertEquals(com.flying.orm.rdb.batch.BatchExecutionState.SUCCESS, result.state());
            assertEquals(List.of("one", "two"), inputs);
        }
    }

    record NamedRow(String value) { }

    @Test
    void reactiveExplicitNullScopeFailsLazilyWithoutConsumingRows() {
        EntityMetadata<String> metadata = metadata();
        ReactiveRepositoryBatchCoordinator<String> coordinator = new ReactiveRepositoryBatchCoordinator<>(
                BatchEvidenceFixtures.unusedFormClient(), metadata.toDynamicForm(), metadata,
                EntityValues.createUncached(String.class, metadata), new EntityLifecycleDispatcher<>(metadata, null),
                RepositoryEntityIdSupport.create(metadata, IdGenerator.none()));
        java.util.concurrent.atomic.AtomicInteger subscriptions = new java.util.concurrent.atomic.AtomicInteger();
        Mono<BatchExecutionEvidence> pending = org.junit.jupiter.api.Assertions.assertDoesNotThrow(() ->
                coordinator.write(Flux.just("one").doOnSubscribe(ignored -> subscriptions.incrementAndGet()),
                        BatchWriteOptions.defaults(), null, RepositoryBatchKind.UPDATE, null));
        NullPointerException error = org.junit.jupiter.api.Assertions.assertThrows(NullPointerException.class,
                () -> pending.block(Duration.ofSeconds(1)));
        assertEquals("batch data scope must not be null", error.getMessage());
        assertEquals(0, subscriptions.get());
    }

    private static EntityMetadata<String> metadata() {
        return EntityMetadata.create(
                String.class,
                "noop_lifecycle",
                com.flying.orm.core.metadata.RelationIdentity.table("noop_lifecycle"),
                "noop_lifecycle",
                List.of(),
                null,
                TenantStrategy.NONE,
                FieldProtectionRegistry.builder().build(), java.util.Map.of());
    }
}
