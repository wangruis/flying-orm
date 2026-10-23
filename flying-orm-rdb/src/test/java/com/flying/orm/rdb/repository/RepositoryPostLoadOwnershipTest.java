package com.flying.orm.rdb.repository;

import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.protection.FieldProtectionRegistry;
import com.flying.orm.rdb.mapping.EntityMetadata;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.Disposable;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RepositoryPostLoadOwnershipTest {

    @Test
    void reactivePostLoadWithoutListenerReturnsOwnedPublishersAndPagesDirectly() {
        EntityLifecycleDispatcher<String> lifecycle =
                new EntityLifecycleDispatcher<>(metadata(), null);
        Flux<String> rows = Flux.just("one", "two");
        PageResult<String> page = new PageResult<>(List.of("one", "two"), 2, 1, 20);

        assertSame(rows, lifecycle.postLoad(rows));
        assertSame(page, lifecycle.postLoad(page).block());
    }

    @Test
    void reactivePostLoadWithListenerReturnsTheOriginalPageAfterOrderedCallbacks() {
        List<String> callbacks = new ArrayList<>();
        List<String> discarded = new ArrayList<>();
        EntityLifecycleDispatcher<String> lifecycle = new EntityLifecycleDispatcher<>(
                metadata(), event -> Mono.fromRunnable(() -> callbacks.add(event.entity())));
        PageResult<String> page = new PageResult<>(List.of("one", "two"), 9, 2, 2);
        Mono<PageResult<String>> loaded = lifecycle.postLoad(page).doOnDiscard(String.class, discarded::add);

        assertEquals(List.of(), callbacks);
        PageResult<String> actual = loaded.block();
        assertEquals(List.of("one", "two"), callbacks);
        assertEquals(List.of(), discarded);
        assertSame(page, actual);
    }

    @Test
    void laterCallbackFailureDoesNotDiscardEntitiesAlreadyOwnedByThePage() {
        List<String> callbacks = new ArrayList<>();
        List<String> discarded = new ArrayList<>();
        IllegalStateException failure = new IllegalStateException("second callback failed");
        EntityLifecycleDispatcher<String> lifecycle = new EntityLifecycleDispatcher<>(metadata(), event -> {
            callbacks.add(event.entity());
            return event.entity().equals("two") ? Mono.error(failure) : Mono.empty();
        });
        PageResult<String> page = new PageResult<>(List.of("one", "two"), 2, 1, 2);

        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> lifecycle.postLoad(page).doOnDiscard(String.class, discarded::add).block()));

        assertEquals(List.of("one", "two"), callbacks);
        assertEquals(List.of("one", "two"), page.rows());
        assertFalse(discarded.contains("one"), "completed page entities must not enter an abandoned duplicate list");
    }

    @Test
    void pageWaitsForTheLastCallbackAndRepeatsCallbacksForEachSubscription() {
        List<String> callbacks = new ArrayList<>();
        List<PageResult<String>> published = new ArrayList<>();
        reactor.core.publisher.Sinks.Empty<Void> completion = reactor.core.publisher.Sinks.empty();
        EntityLifecycleDispatcher<String> lifecycle = new EntityLifecycleDispatcher<>(metadata(), event -> {
            callbacks.add(event.entity());
            return event.entity().equals("two") ? completion.asMono() : Mono.empty();
        });
        PageResult<String> page = new PageResult<>(List.of("one", "two"), 2, 1, 2);
        Mono<PageResult<String>> loaded = lifecycle.postLoad(page);
        Disposable subscription = loaded.subscribe(published::add);
        try {
            assertEquals(List.of("one", "two"), callbacks);
            assertEquals(List.of(), published);
            assertEquals(reactor.core.publisher.Sinks.EmitResult.OK, completion.tryEmitEmpty());
            assertSame(page, published.getFirst());
            assertSame(page, loaded.block());
            assertEquals(List.of("one", "two", "one", "two"), callbacks);
        } finally {
            subscription.dispose();
        }
    }

    @Test
    void cancellationCancelsTheActiveCallbackWithoutDiscardingCompletedPageEntities() {
        List<String> callbacks = new ArrayList<>();
        List<String> discarded = new ArrayList<>();
        AtomicInteger cancellations = new AtomicInteger();
        EntityLifecycleDispatcher<String> lifecycle = new EntityLifecycleDispatcher<>(metadata(), event -> {
            callbacks.add(event.entity());
            return event.entity().equals("two")
                    ? Mono.<Void>never().doOnCancel(cancellations::incrementAndGet) : Mono.empty();
        });
        PageResult<String> page = new PageResult<>(List.of("one", "two", "three"), 3, 1, 3);

        List<PageResult<String>> published = new ArrayList<>();
        List<Throwable> errors = new ArrayList<>();
        Disposable subscription = lifecycle.postLoad(page).doOnDiscard(String.class, discarded::add)
                .subscribe(published::add, errors::add);
        try {
            assertEquals(List.of("one", "two"), callbacks);
            assertEquals(List.of(), published);
            assertEquals(List.of(), errors);
        } finally {
            subscription.dispose();
        }

        assertEquals(1, cancellations.get());
        assertFalse(discarded.contains("one"), "completed page entities remain owned by the original page");
        assertEquals(List.of("one", "two", "three"), page.rows());
    }

    @Test
    void syncPostLoadWithListenerIteratesAndReturnsOwnedList() {
        AtomicInteger callbacks = new AtomicInteger();
        SyncRepositoryLifecycleSupport<String> lifecycle = new SyncRepositoryLifecycleSupport<>(
                metadata(), event -> {
                    callbacks.incrementAndGet();
                    return Mono.empty();
                });
        List<String> rows = new ArrayList<>(List.of("one", "two"));

        List<String> actual = lifecycle.postLoad(rows);

        assertSame(rows, actual);
        assertEquals(2, callbacks.get());
    }

    private static EntityMetadata<String> metadata() {
        return EntityMetadata.create(String.class,
                                     "strings",
                                     com.flying.orm.core.metadata.RelationIdentity.table("strings"),
                                     "strings",
                                     List.of(),
                                     null,
                                     TenantStrategy.NONE,
                                     FieldProtectionRegistry.empty(), java.util.Map.of());
    }
}
