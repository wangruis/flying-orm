package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.lifecycle.CommittedEntityLifecycleException;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RepositoryIndependentBatchFailureLifecycleTest {

    @Test
    void syncAggregateHonorsCommittedChunksCarriedByFailure() {
        BatchWriteException failure = partialFailure();
        SyncFormClient client = SyncFormClient.create(new NoopSyncSqlExecutor(),
                new FailingSyncBatchExecutor(failure), renderer());
        List<Long> persisted = new ArrayList<>();
        SyncFormRepository<Person> repository = SyncFormRepository.create(
                client, client.entityModels().metadata(Person.class).toDynamicForm(), Person.class)
                .withListener(event -> {
                    if (event.phase() == EntityLifecyclePhase.POST_PERSIST) {
                        persisted.add(event.entity().getId());
                    }
                    return Mono.empty();
                });

        BatchWriteException thrown = assertThrows(BatchWriteException.class,
                () -> repository.insertBatch(Flux.just(person(1), person(2)), independentOptions()));

        assertSame(failure, thrown);
        assertEquals(List.of(1L), persisted);
    }

    @Test
    void syncChunkEntryHonorsCommittedChunksCarriedByFailure() {
        BatchWriteException failure = partialFailure();
        SyncFormClient client = SyncFormClient.create(new NoopSyncSqlExecutor(),
                new FailingSyncBatchExecutor(failure), renderer());
        List<Long> persisted = new ArrayList<>();
        SyncFormRepository<Person> repository = SyncFormRepository.create(
                client, client.entityModels().metadata(Person.class).toDynamicForm(), Person.class)
                .withListener(event -> {
                    if (event.phase() == EntityLifecyclePhase.POST_PERSIST) {
                        persisted.add(event.entity().getId());
                    }
                    return Mono.empty();
                });

        BatchWriteException thrown = assertThrows(BatchWriteException.class,
                () -> repository.insertBatchChunks(Flux.just(person(1), person(2)), independentOptions()));

        assertSame(failure, thrown);
        assertEquals(List.of(1L), persisted);
    }

    @Test
    void reactiveAggregateHonorsCommittedChunksCarriedByFailure() {
        BatchWriteException failure = partialFailure();
        ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
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
                return Flux.from(request.rows()).then(Mono.error(failure));
            }
        };
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        List<Long> persisted = new ArrayList<>();
        ReactiveFormRepository<Person> repository = ReactiveFormRepository.create(
                client, client.entityModels().metadata(Person.class).toDynamicForm(), Person.class)
                .withListener(event -> {
                    if (event.phase() == EntityLifecyclePhase.POST_PERSIST) {
                        persisted.add(event.entity().getId());
                    }
                    return Mono.empty();
                });

        BatchWriteException thrown = assertThrows(BatchWriteException.class,
                () -> repository.insertBatch(Flux.just(person(1), person(2)), independentOptions()).block());

        assertSame(failure, thrown);
        assertEquals(List.of(1L), persisted);
    }

    @Test
    void syncFailureStillFinishesEveryEntityInAnAlreadyCommittedChunk() {
        BatchWriteException failure = partialFailureAfterTwoCommittedRows();
        SyncFormClient client = SyncFormClient.create(new NoopSyncSqlExecutor(),
                new FailingSyncBatchExecutor(failure), renderer());
        List<Long> persisted = new ArrayList<>();
        SyncFormRepository<Person> repository = SyncFormRepository.create(
                client, client.entityModels().metadata(Person.class).toDynamicForm(), Person.class)
                .withListener(event -> {
                    if (event.phase() == EntityLifecyclePhase.POST_PERSIST) {
                        persisted.add(event.entity().getId());
                        if (event.entity().getId() == 1L) {
                            return Mono.error(new IllegalStateException("first post-persist failed"));
                        }
                    }
                    return Mono.empty();
                });

        BatchWriteException thrown = assertThrows(BatchWriteException.class,
                () -> repository.insertBatch(
                        Flux.just(person(1), person(2), person(3)), independentOptions()));

        assertSame(failure, thrown);
        assertEquals(List.of(1L, 2L), persisted);
    }

    @Test
    void reactiveFailureStillFinishesEveryEntityInAnAlreadyCommittedChunk() {
        BatchWriteException failure = partialFailureAfterTwoCommittedRows();
        ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
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
                return Flux.from(request.rows()).then(Mono.error(failure));
            }
        };
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        List<Long> persisted = new ArrayList<>();
        ReactiveFormRepository<Person> repository = ReactiveFormRepository.create(
                client, client.entityModels().metadata(Person.class).toDynamicForm(), Person.class)
                .withListener(event -> {
                    if (event.phase() == EntityLifecyclePhase.POST_PERSIST) {
                        persisted.add(event.entity().getId());
                        if (event.entity().getId() == 1L) {
                            return Mono.error(new IllegalStateException("first post-persist failed"));
                        }
                    }
                    return Mono.empty();
                });

        BatchWriteException thrown = assertThrows(BatchWriteException.class,
                () -> repository.insertBatch(
                        Flux.just(person(1), person(2), person(3)), independentOptions()).block());

        assertSame(failure, thrown);
        assertEquals(List.of(1L, 2L), persisted);
    }

    @Test
    void syncSuccessStillFinishesEveryCommittedEntityWhenOneCallbackFails() {
        BatchWriteResult result = committedTwoRows();
        SyncFormClient client = SyncFormClient.create(new NoopSyncSqlExecutor(),
                new SuccessfulSyncBatchExecutor(result), renderer());
        List<Long> persisted = new ArrayList<>();
        SyncFormRepository<Person> repository = SyncFormRepository.create(
                client, client.entityModels().metadata(Person.class).toDynamicForm(), Person.class)
                .withListener(failingFirstPostPersist(persisted));

        assertThrows(CommittedEntityLifecycleException.class,
                () -> repository.insertBatch(Flux.just(person(1), person(2)), independentOptions()));

        assertEquals(List.of(1L, 2L), persisted);
    }

    @Test
    void reactiveSuccessStillFinishesEveryCommittedEntityWhenOneCallbackFails() {
        BatchWriteResult result = committedTwoRows();
        ReactiveSqlExecutor executor = successfulReactiveBatchExecutor(result);
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        List<Long> persisted = new ArrayList<>();
        ReactiveFormRepository<Person> repository = ReactiveFormRepository.create(
                client, client.entityModels().metadata(Person.class).toDynamicForm(), Person.class)
                .withListener(failingFirstPostPersist(persisted));

        assertThrows(CommittedEntityLifecycleException.class,
                () -> repository.insertBatch(Flux.just(person(1), person(2)), independentOptions()).block());

        assertEquals(List.of(1L, 2L), persisted);
    }

    private static BatchWriteException partialFailure() {
        IllegalStateException cause = new IllegalStateException("second chunk failed");
        BatchWriteResult result = BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT, List.of(
                BatchChunkResult.committed(0, 0L, 1, 1L),
                BatchChunkResult.failed(1, 1L, 1, cause)));
        return new BatchWriteException("independent batch stopped", cause, result);
    }

    private static BatchWriteException partialFailureAfterTwoCommittedRows() {
        IllegalStateException cause = new IllegalStateException("third row failed");
        BatchWriteResult result = BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT, List.of(
                BatchChunkResult.committed(0, 0L, 2, 2L),
                BatchChunkResult.failed(1, 2L, 1, cause)));
        return new BatchWriteException("independent batch stopped", cause, result);
    }

    private static BatchWriteResult committedTwoRows() {
        return BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT,
                List.of(BatchChunkResult.committed(0, 0L, 2, 2L)));
    }

    private static com.flying.orm.rdb.lifecycle.ReactiveEntityListener<Person> failingFirstPostPersist(
            List<Long> persisted) {
        return event -> {
            if (event.phase() == EntityLifecyclePhase.POST_PERSIST) {
                persisted.add(event.entity().getId());
                if (event.entity().getId() == 1L) {
                    return Mono.error(new IllegalStateException("first post-persist failed"));
                }
            }
            return Mono.empty();
        };
    }

    private static ReactiveSqlExecutor successfulReactiveBatchExecutor(BatchWriteResult result) {
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
                return Flux.from(request.rows()).then(Mono.just(result));
            }
        };
    }

    private static BatchWriteOptions independentOptions() {
        return BatchWriteOptions.independent(1, 1);
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
    }

    private static Person person(long id) {
        return new Person(id, "name-" + id);
    }

    private static final class FailingSyncBatchExecutor implements SyncBatchExecutor {

        private final BatchWriteException failure;

        private FailingSyncBatchExecutor(BatchWriteException failure) {
            this.failure = failure;
        }

        @Override
        public BatchWriteResult writeBatch(BatchWriteRequest request) {
            Flux.from(request.rows()).collectList().block();
            throw failure;
        }

        @Override
        public List<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
            Flux.from(request.rows()).collectList().block();
            throw failure;
        }
    }

    private static final class SuccessfulSyncBatchExecutor implements SyncBatchExecutor {

        private final BatchWriteResult result;

        private SuccessfulSyncBatchExecutor(BatchWriteResult result) {
            this.result = result;
        }

        @Override
        public BatchWriteResult writeBatch(BatchWriteRequest request) {
            Flux.from(request.rows()).collectList().block();
            return result;
        }

        @Override
        public List<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
            Flux.from(request.rows()).collectList().block();
            return result.chunks();
        }
    }

    private static final class NoopSyncSqlExecutor implements SyncSqlExecutor {

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            throw new UnsupportedOperationException();
        }
    }

    @TableName("batch_people")
    private static final class Person {

        @TableId(type = IdType.INPUT)
        private final Long id;

        private final String name;

        private Person(Long id, String name) {
            this.id = id;
            this.name = name;
        }

        public Long getId() {
            return id;
        }

        public String getName() {
            return name;
        }
    }
}
