package com.flying.orm.rdb.repository;

import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.protection.FieldProtectionRegistry;
import com.flying.orm.rdb.mapping.EntityMetadata;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class RepositoryPostLoadOwnershipTest {

    @Test
    void reactivePostLoadWithoutListenerReturnsOwnedPublishersAndPagesDirectly() {
        ReactiveRepositoryLifecycleSupport<String> lifecycle =
                new ReactiveRepositoryLifecycleSupport<>(metadata(), null);
        Flux<String> rows = Flux.just("one", "two");
        PageResult<String> page = new PageResult<>(List.of("one", "two"), 2, 1, 20);

        assertSame(rows, lifecycle.postLoad(rows));
        assertSame(page, lifecycle.postLoad(page).block());
    }

    @Test
    void syncPostLoadWithListenerIteratesAndReturnsOwnedList() {
        AtomicInteger callbacks = new AtomicInteger();
        SyncRepositoryLifecycleSupport<String> lifecycle = new SyncRepositoryLifecycleSupport<>(
                metadata(), event -> {
                    callbacks.incrementAndGet();
                    return Mono.empty();
                }, new SyncRepositoryAwaiter(Duration.ofSeconds(1)));
        List<String> rows = new ArrayList<>(List.of("one", "two"));

        List<String> actual = lifecycle.postLoad(rows);

        assertSame(rows, actual);
        assertEquals(2, callbacks.get());
    }

    private static EntityMetadata<String> metadata() {
        return EntityMetadata.create(String.class,
                                     "strings",
                                     "strings",
                                     List.of(),
                                     null,
                                     TenantStrategy.NONE,
                                     FieldProtectionRegistry.empty());
    }
}
