package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.List;
import java.util.ArrayList;
import java.util.function.LongFunction;
import static org.junit.jupiter.api.Assertions.*;

class RepositoryIndependentBatchFailureLifecycleTest {
    @Test
    void syncDoesNotReplayPostFromSqlSuccessFactsAfterAuxiliaryFailure() {
        for (boolean callbackCompleted : new boolean[]{false, true}) {
            List<Long> posts = new ArrayList<>();
            BatchExecutionEvidence facts = BatchEvidenceFixtures.successful(2);
            BatchExecutionEvidenceException failure = new BatchExecutionEvidenceException(
                    "auxiliary relation failed", new IllegalStateException("side index failed"), facts);
            SyncBatchExecutor executor = (request, rowCompleted) -> {
                Flux.from(request.rows()).collectList().block();
                if (callbackCompleted) rowCompleted.accept(0L);
                throw failure;
            };
            var client = SyncFormClient.create(new NoopSyncSqlExecutor(), executor, renderer());
            var repository = SyncFormRepository.create(client,
                    client.entityModels().metadata(Person.class).toDynamicForm(), Person.class)
                    .withListener(event -> {
                        if (event.phase() == EntityLifecyclePhase.POST_PERSIST) posts.add(event.entity().id);
                        return Mono.empty();
                    });
            assertSame(failure, assertThrows(BatchExecutionEvidenceException.class,
                    () -> RepositoryBatchEvidenceAccess.sync(repository).insertEvidence(Flux.just(new Person(1L, "one"), new Person(2L, "two")),
                            BatchWriteOptions.of(2))));
            assertEquals(callbackCompleted ? List.of(1L) : List.of(), posts);
            assertEquals(2, facts.successfulCount(), "SQL evidence alone must not qualify POST");
        }
    }

    @Test
    void reactiveDoesNotReplayPostFromSqlSuccessFactsAfterAuxiliaryFailure() {
        for (boolean callbackCompleted : new boolean[]{false, true}) {
            List<Long> posts = new ArrayList<>();
            BatchExecutionEvidence facts = BatchEvidenceFixtures.successful(2);
            BatchExecutionEvidenceException failure = new BatchExecutionEvidenceException(
                    "auxiliary relation failed", new IllegalStateException("side index failed"), facts);
            ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
                public Flux<DynamicRow> query(SqlRequest request) { return Flux.error(new UnsupportedOperationException()); }
                public Mono<Long> rowsUpdated(SqlRequest request) { return Mono.error(new UnsupportedOperationException()); }
                public Mono<BatchExecutionEvidence> writeBatch(BatchWriteRequest request,
                        LongFunction<? extends Publisher<Void>> rowCompleted) {
                    return Flux.from(request.rows()).then(Mono.defer(() -> callbackCompleted
                            ? Mono.from(rowCompleted.apply(0L)) : Mono.empty())).then(Mono.error(failure));
                }
            };
            var client = ReactiveFormClient.create(executor, renderer());
            var repository = ReactiveFormRepository.create(client,
                    client.entityModels().metadata(Person.class).toDynamicForm(), Person.class)
                    .withListener(event -> {
                        if (event.phase() == EntityLifecyclePhase.POST_PERSIST) posts.add(event.entity().id);
                        return Mono.empty();
                    });
            assertSame(failure, assertThrows(BatchExecutionEvidenceException.class,
                    () -> RepositoryBatchEvidenceAccess.reactive(repository).insertEvidence(Flux.just(new Person(1L, "one"), new Person(2L, "two")),
                            BatchWriteOptions.of(2)).block()));
            assertEquals(callbackCompleted ? List.of(1L) : List.of(), posts);
            assertEquals(2, facts.successfulCount());
        }
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
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
