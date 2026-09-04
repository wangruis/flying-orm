package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
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
    void reactiveInsertCountsSharedPayloadOnce() {
        AtomicInteger postPersistCalls = new AtomicInteger();
        ReactiveFormRepository<PayloadEntity> repository = repository(postPersistCalls);
        PayloadEntity entity = new PayloadEntity(7L, new byte[1_024], null);

        BatchWriteResult result = repository.insertBatch(
                        Flux.just(entity), options(1_600L))
                .block(Duration.ofSeconds(2));

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
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
        return BatchWriteOptions.atomic(1).withMemoryLimits(1, maxBufferedBytes, 1);
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
            public Mono<BatchWriteResult> writeBatch(BatchWriteRequest request) {
                return Flux.from(request.rows())
                        .then(Mono.just(BatchWriteResult.from(request.options().mode(),
                                java.util.List.of(BatchChunkResult.committed(0, 0L, 1, 1L)))));
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
            return lifecycleOnly;
        }
    }
}
