package com.flying.orm.rdb.repository;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.Version;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class RepositoryEntityWriterReadConvergenceTest {

    @Test
    void syncSingleAndBatchWritersHoldTheSameIdSupport() throws ReflectiveOperationException {
        SyncFormClient client = SyncFormClient.create(
                new CapturingSyncExecutor(), new UnusedSyncBatchExecutor(), renderer());
        SyncFormRepository<CountedEntity> repository = SyncFormRepository.create(
                client, client.entityModels().metadata(CountedEntity.class).toDynamicForm(), CountedEntity.class);

        Object singleIds = field(field(repository, "entityWriter"), "ids");
        Object batchIds = field(field(field(repository, "batchCoordinator"), "support"), "ids");
        assertSame(batchIds, singleIds);
    }

    @Test
    void reactiveSingleAndBatchWritersHoldTheSameIdSupport() throws ReflectiveOperationException {
        ReactiveFormClient client = ReactiveFormClient.create(new CapturingReactiveExecutor(), renderer());
        ReactiveFormRepository<CountedEntity> repository = ReactiveFormRepository.create(
                client, client.entityModels().metadata(CountedEntity.class).toDynamicForm(), CountedEntity.class);

        Object singleIds = field(field(repository, "entityWriter"), "ids");
        Object batchIds = field(field(field(repository, "batchOperations"), "coordinator"), "ids");
        assertSame(batchIds, singleIds);
    }

    private static Object field(Object owner, String name) throws ReflectiveOperationException {
        java.lang.reflect.Field field = owner.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(owner);
    }

    @Test
    void syncUpdateReadsOnlyTheWritableSnapshotWhenVersioningIsDisabled() {
        CapturingSyncExecutor executor = new CapturingSyncExecutor();
        SyncFormClient client = SyncFormClient.create(executor, new UnusedSyncBatchExecutor(), renderer());
        SyncFormRepository<CountedEntity> repository = SyncFormRepository.create(
                client, client.entityModels().metadata(CountedEntity.class).toDynamicForm(), CountedEntity.class);
        CountedEntity entity = new CountedEntity(7L, "next");

        assertEquals(1L, repository.update(entity, where()));

        assertEquals(1, entity.idReads, "readForUpdate retains its single existing field-read pass");
        assertEquals(1, entity.valueReads);
        assertEquals(2, executor.request.get().parameters().size());
    }

    @Test
    void reactiveUpdateReadsOnlyTheWritableSnapshotWhenVersioningIsDisabled() {
        CapturingReactiveExecutor executor = new CapturingReactiveExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        ReactiveFormRepository<CountedEntity> repository = ReactiveFormRepository.create(
                client, client.entityModels().metadata(CountedEntity.class).toDynamicForm(), CountedEntity.class);
        CountedEntity entity = new CountedEntity(7L, "next");

        Mono<Long> update = repository.update(entity, where());
        assertEquals(0, entity.idReads, "entity values must remain unread before subscription");
        assertEquals(0, entity.valueReads);
        assertEquals(1L, update.block());

        assertEquals(1, entity.idReads, "readForUpdate retains its single existing field-read pass");
        assertEquals(1, entity.valueReads);
        assertEquals(2, executor.request.get().parameters().size());
    }

    @Test
    void syncDeleteDoesNotReadAnEntityWhenVersioningIsDisabled() {
        CapturingSyncExecutor executor = new CapturingSyncExecutor();
        SyncFormClient client = SyncFormClient.create(executor, new UnusedSyncBatchExecutor(), renderer());
        SyncFormRepository<CountedEntity> repository = SyncFormRepository.create(
                client, client.entityModels().metadata(CountedEntity.class).toDynamicForm(), CountedEntity.class);
        CountedEntity entity = new CountedEntity(7L, "unused");

        assertEquals(1L, repository.delete(entity, where()));

        assertEquals(0, entity.idReads);
        assertEquals(0, entity.valueReads);
        assertTrue(executor.request.get().sql().startsWith("delete from "));
        assertEquals(List.of(7L), executor.request.get().parameters());
    }

    @Test
    void reactiveDeleteDoesNotReadAnEntityWhenVersioningIsDisabled() {
        CapturingReactiveExecutor executor = new CapturingReactiveExecutor();
        ReactiveFormClient client = ReactiveFormClient.create(executor, renderer());
        ReactiveFormRepository<CountedEntity> repository = ReactiveFormRepository.create(
                client, client.entityModels().metadata(CountedEntity.class).toDynamicForm(), CountedEntity.class);
        CountedEntity entity = new CountedEntity(7L, "unused");

        assertEquals(1L, repository.delete(entity, where()).block());

        assertEquals(0, entity.idReads);
        assertEquals(0, entity.valueReads);
        assertTrue(executor.request.get().sql().startsWith("delete from "));
        assertEquals(List.of(7L), executor.request.get().parameters());
    }

    @Test
    void readsTheVersionSnapshotBeforeTheWritableSnapshot() {
        CapturingSyncExecutor executor = new CapturingSyncExecutor();
        SyncFormClient client = SyncFormClient.create(executor, new UnusedSyncBatchExecutor(), renderer());
        SyncFormRepository<VersionedEntity> repository = SyncFormRepository.create(
                client, client.entityModels().metadata(VersionedEntity.class).toDynamicForm(), VersionedEntity.class);
        VersionedEntity entity = new VersionedEntity(7L, 3L, "next");

        assertEquals(1L, repository.update(entity, where()));

        assertTrue(executor.request.get().parameters().contains(3L));
    }

    private static ConditionGroup where() {
        return ConditionGroup.and().where("id", "=", 7L).build();
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
    }

    @TableName("counted_entities")
    private static final class CountedEntity {
        @TableId(type = IdType.INPUT)
        private final Long id;
        private final String value;
        private transient int idReads;
        private transient int valueReads;

        private CountedEntity(Long id, String value) {
            this.id = id;
            this.value = value;
        }

        public Long getId() {
            idReads++;
            return id;
        }

        public String getValue() {
            valueReads++;
            return value;
        }
    }

    @TableName("versioned_entities")
    private static final class VersionedEntity {
        @TableId(type = IdType.INPUT)
        private final Long id;
        @Version
        private Long version;
        private final String value;
        private transient int versionReads;

        private VersionedEntity(Long id, Long version, String value) {
            this.id = id;
            this.version = version;
            this.value = value;
        }

        public Long getId() {
            return id;
        }

        public Long getVersion() {
            if (versionReads++ == 0) {
                version = 8L;
                return 3L;
            }
            return version;
        }

        public String getValue() {
            return value;
        }
    }

    private static final class CapturingSyncExecutor implements SyncSqlExecutor {
        private final AtomicReference<com.flying.orm.core.sql.render.SqlRequest> request = new AtomicReference<>();

        @Override
        public List<DynamicRow> query(com.flying.orm.core.sql.render.SqlRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long rowsUpdated(com.flying.orm.core.sql.render.SqlRequest request) {
            this.request.set(request);
            return 1L;
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(
                com.flying.orm.core.sql.render.SqlRequest request, SqlExecutionOptions options) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class CapturingReactiveExecutor implements ReactiveSqlExecutor {
        private final AtomicReference<com.flying.orm.core.sql.render.SqlRequest> request = new AtomicReference<>();

        @Override
        public Flux<DynamicRow> query(com.flying.orm.core.sql.render.SqlRequest request) {
            return Flux.error(new UnsupportedOperationException());
        }

        @Override
        public Mono<Long> rowsUpdated(com.flying.orm.core.sql.render.SqlRequest request) {
            this.request.set(request);
            return Mono.just(1L);
        }
    }

    private static final class UnusedSyncBatchExecutor implements SyncBatchExecutor {
        @Override
        public BatchExecutionEvidence writeBatch(BatchWriteRequest request, java.util.function.LongConsumer rowCompleted) {
            throw new UnsupportedOperationException();
        }

    }
}
