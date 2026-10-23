package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RepositoryBatchRetentionBudgetTest {

    @Test
    void finiteReactiveBudgetTraversesRetainedValuesOnlyOnce() {
        AtomicInteger estimates = new AtomicInteger();
        PayloadEntity entity = new PayloadEntity(7L, new byte[16], new byte[16]);
        entity.lifecycleNumber = new java.math.BigDecimal("123") {
            @Override
            public int precision() { estimates.incrementAndGet(); return super.precision(); }
        };
        AtomicInteger callbacks = new AtomicInteger();
        repository(callbacks).insertBatch(Flux.just(entity), options(1_600L)).block();
        assertEquals(1, callbacks.get());
        assertEquals(1, estimates.get());
    }

    @Test
    void defaultReactiveRetentionDoesNotReadNonWritingFieldsForAnUnusedBudget() {
        AtomicInteger callbacks = new AtomicInteger();
        PayloadEntity entity = new PayloadEntity(7L, new byte[16], new byte[16]);
        repository(callbacks).insertBatch(Flux.just(entity), BatchWriteOptions.defaults()).block();
        assertEquals(1, callbacks.get());
        assertEquals(0, entity.retainedReads);
    }

    @Test
    void syncRetentionEstimatesOnlyForAnExplicitBudget() {
        ReactiveFormClient client = ReactiveFormClient.create(successfulBatchExecutor(), renderer());
        com.flying.orm.rdb.mapping.EntityMetadata<PayloadEntity> metadata =
                client.entityModels().metadata(PayloadEntity.class);
        AtomicInteger callbacks = new AtomicInteger();
        SyncRepositoryLifecycleSupport<PayloadEntity> lifecycle = new SyncRepositoryLifecycleSupport<>(
                metadata, event -> Mono.fromRunnable(callbacks::incrementAndGet));
        for (long budget : new long[]{Long.MAX_VALUE, 1_600L}) {
            com.flying.orm.rdb.internal.mapping.EntityValues<PayloadEntity> values =
                    com.flying.orm.rdb.internal.mapping.EntityValues.createUncached(PayloadEntity.class, metadata);
            RepositoryBatchRetention<PayloadEntity> tracker = new RepositoryBatchRetention<>(
                    RepositoryEntityIdSupport.create(metadata, com.flying.orm.rdb.id.IdGenerator.none()), false, budget,
                    "lifecycleRetainedBytes", (entity, row) ->
                    com.flying.orm.rdb.batch.BatchMemoryBudget.estimateRowBytes(values.readRetainedValues(entity)) + 64L);
            PayloadEntity entity = new PayloadEntity(7L, new byte[1_024], new byte[1_024]);
            if (budget == Long.MAX_VALUE) {
                tracker.remember(0, entity, null);
                lifecycle.fire(EntityLifecyclePhase.POST_PERSIST, tracker.complete(0), 0L);
                assertEquals(0, entity.retainedReads);
                assertEquals(1, callbacks.get());
            } else {
                assertThrows(BatchMemoryLimitExceededException.class, () -> tracker.remember(0, entity, null));
                assertEquals(1, entity.retainedReads);
            }
            tracker.clear();
        }
    }

    @Test
    void reactiveInsertCountsSharedPayloadOnce() {
        AtomicInteger postPersistCalls = new AtomicInteger();
        ReactiveFormRepository<PayloadEntity> repository = repository(postPersistCalls);
        PayloadEntity entity = new PayloadEntity(7L, new byte[1_024], null);

        BatchExecutionEvidence result = repository.insertBatch(
                        Flux.just(entity), options(1_600L))
                .block(Duration.ofSeconds(2));

        assertEquals(com.flying.orm.rdb.batch.BatchExecutionState.SUCCESS, result.state());
        assertEquals(1, postPersistCalls.get());
    }

    @Test
    void reactiveInsertStillCountsNonWritingLifecyclePayload() {
        ReactiveFormRepository<PayloadEntity> repository = repository(new AtomicInteger());
        PayloadEntity entity = new PayloadEntity(7L, new byte[1_024], new byte[1_024]);

        assertThrows(BatchMemoryLimitExceededException.class,
                () -> repository.insertBatch(Flux.just(entity), options(1_600L))
                        .block(Duration.ofSeconds(2)));
    }

    private static ReactiveFormRepository<PayloadEntity> repository(AtomicInteger postPersistCalls) {
        ReactiveFormClient client = ReactiveFormClient.create(successfulBatchExecutor(), renderer());
        return ReactiveFormRepository.create(
                        client,
                        client.entityModels().metadata(PayloadEntity.class).toDynamicForm(),
                        PayloadEntity.class)
                .withListener(event -> {
                    if (event.phase() == EntityLifecyclePhase.POST_PERSIST) {
                        postPersistCalls.incrementAndGet();
                    }
                    return Mono.empty();
                });
    }

    private static BatchWriteOptions options(long maxBufferedBytes) {
        return BatchWriteOptions.of(1).withMemoryLimits(1, maxBufferedBytes);
    }

    private static ReactiveSqlExecutor successfulBatchExecutor() {
        return new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest request) {
                return Flux.error(new UnsupportedOperationException());
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest request) {
                return Mono.error(new UnsupportedOperationException());
            }

            @Override
            public Mono<BatchExecutionEvidence> writeBatch(BatchWriteRequest request, java.util.function.LongFunction<? extends org.reactivestreams.Publisher<Void>> rowCompleted) {
                return Flux.from(request.rows()).index()
                        .concatMap(row -> rowCompleted == null ? Mono.empty() : Mono.from(rowCompleted.apply(row.getT1())))
                        .then(Mono.just(BatchEvidenceFixtures.successful(1)));
            }
        };
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
    }

    @TableName("payload_entities")
    private static final class PayloadEntity {

        @TableId(type = IdType.INPUT)
        private final Long id;

        private final byte[] payload;

        @TableField(insertStrategy = FieldStrategy.NEVER, updateStrategy = FieldStrategy.NEVER)
        private final byte[] lifecycleOnly;

        @TableField(insertStrategy = FieldStrategy.NEVER, updateStrategy = FieldStrategy.NEVER)
        private java.math.BigDecimal lifecycleNumber;

        @TableField(exist = false)
        private int retainedReads;

        private PayloadEntity(Long id, byte[] payload, byte[] lifecycleOnly) {
            this.id = id;
            this.payload = payload;
            this.lifecycleOnly = lifecycleOnly;
        }

        public Long getId() {
            return id;
        }

        public byte[] getPayload() {
            return payload;
        }

        public byte[] getLifecycleOnly() {
            retainedReads++;
            return lifecycleOnly;
        }
    }
}
