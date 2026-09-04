package com.flying.orm.rdb.repository;

import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.protection.FieldProtectionRegistry;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.id.IdGenerator;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
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
    void reactiveNoopLifecyclePreservesDirectBatchDemand() {
        EntityMetadata<String> metadata = metadata();
        RepositoryEntityIdSupport<String> ids = RepositoryEntityIdSupport.create(metadata, IdGenerator.none());
        ReactiveRepositoryBatchCoordinator<String> coordinator = new ReactiveRepositoryBatchCoordinator<>(
                metadata,
                EntityValues.createUncached(String.class, metadata),
                new ReactiveRepositoryLifecycleSupport<>(metadata, null),
                ids);
        List<Long> requests = new ArrayList<>();
        BatchWriteResult result = coordinator.write(
                Flux.just("one", "two").doOnRequest(requests::add),
                String::toUpperCase,
                EntityLifecyclePhase.PRE_PERSIST,
                EntityLifecyclePhase.POST_PERSIST,
                BatchWriteOptions.defaults(),
                false,
                (rows, completion, generatedKeys) -> {
                    assertFalse(generatedKeys.required());
                    assertInstanceOf(Mono.class, completion.afterCompletion(BatchWriteResult.empty(
                            BatchWriteOptions.Mode.ATOMIC)));
                    return Flux.from(rows).collectList().map(mapped -> {
                        assertEquals(List.of("ONE", "TWO"), mapped);
                        return BatchWriteResult.from(
                                BatchWriteOptions.Mode.ATOMIC,
                                List.of(BatchChunkResult.committed(0, 0L, mapped.size(), mapped.size())));
                    });
                }).block(Duration.ofSeconds(1));

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(List.of(Long.MAX_VALUE), requests);
    }

    @Test
    void syncNoopPlanUsesDirectRowsWithoutLifecycleState() {
        assertSame(RepositoryBatchLifecyclePlan.NOOP,
                   RepositoryBatchLifecyclePlan.select(false, false));
        assertSame(RepositoryBatchLifecyclePlan.TRACKED,
                   RepositoryBatchLifecyclePlan.select(true, false));
        assertSame(RepositoryBatchLifecyclePlan.TRACKED,
                   RepositoryBatchLifecyclePlan.select(false, true));

        List<Long> requests = new ArrayList<>();
        Publisher<String> rows = new SyncRepositoryBatchRows<>(
                Flux.just("one", "two").doOnRequest(requests::add),
                String::toUpperCase);

        assertEquals(List.of("ONE", "TWO"), Flux.from(rows).collectList().block(Duration.ofSeconds(1)));
        assertEquals(List.of(Long.MAX_VALUE), requests);
    }

    private static EntityMetadata<String> metadata() {
        return EntityMetadata.create(
                String.class,
                "noop_lifecycle",
                "noop_lifecycle",
                List.of(),
                null,
                TenantStrategy.NONE,
                FieldProtectionRegistry.builder().build());
    }
}
