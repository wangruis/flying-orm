package com.flying.orm.rdb.repository;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.form.BatchUpdate;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.mapping.EntityMetadata;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/** Composes entity mapping and row lifecycle callbacks through the shared Form batch chain. */
final class ReactiveRepositoryBatchCoordinator<T> {

    private final ReactiveFormClient client;
    private final DynamicForm form;
    private final EntityMetadata<T> metadata;
    private final EntityValues<T> entityValues;
    private final EntityLifecycleDispatcher<T> lifecycle;
    private final RepositoryEntityIdSupport<T> ids;

    ReactiveRepositoryBatchCoordinator(ReactiveFormClient client,
                                       DynamicForm form,
                                       EntityMetadata<T> metadata,
                                       EntityValues<T> entityValues,
                                       EntityLifecycleDispatcher<T> lifecycle,
                                       RepositoryEntityIdSupport<T> ids) {
        this.client = Objects.requireNonNull(client, "reactive form client must not be null");
        this.form = Objects.requireNonNull(form, "repository form must not be null");
        this.metadata = Objects.requireNonNull(metadata, "repository entity metadata must not be null");
        this.entityValues = Objects.requireNonNull(entityValues, "repository entity values must not be null");
        this.lifecycle = Objects.requireNonNull(lifecycle, "repository lifecycle support must not be null");
        this.ids = Objects.requireNonNull(ids, "repository entity id support must not be null");
    }

    Mono<BatchExecutionEvidence> write(List<T> entities, RepositoryBatchKind kind, String[] conflictProperties) {
        Publisher<T> rows = Flux.fromIterable(List.copyOf(
                Objects.requireNonNull(entities, "repository entities must not be null")));
        return write(rows, client.defaultBatchWriteOptions(),
                kind == RepositoryBatchKind.UPDATE ? DataScope.none() : null, kind, conflictProperties);
    }

    Mono<BatchExecutionEvidence> writeSingle(T entity, RepositoryBatchKind kind, String[] conflictProperties) {
        return write(Mono.just(Objects.requireNonNull(entity, "repository entity must not be null")),
                client.defaultBatchWriteOptions(), null, kind, conflictProperties);
    }

    Mono<BatchExecutionEvidence> write(Publisher<T> entities, BatchWriteOptions options,
                                       DataScope scope, RepositoryBatchKind kind, String[] conflictProperties) {
        String[] columns = RepositoryBatchKind.conflictColumns(metadata, conflictProperties);
        return Mono.defer(() -> {
            Function<T, ?> mapper = kind.mapper(metadata, entityValues, ids, columns != null);
            BatchWriteOptions safeOptions = Objects.requireNonNull(options, "batch write options must not be null");
            if (kind == RepositoryBatchKind.UPDATE) Objects.requireNonNull(scope, "batch data scope must not be null");
            boolean returnGeneratedKeys = (kind == RepositoryBatchKind.INSERT
                    || kind == RepositoryBatchKind.UPSERT && columns == null) && ids.databaseGenerated();
            boolean postLifecycle = lifecycle.hasWork();
            if (!postLifecycle && !returnGeneratedKeys) {
                return client.writeBatch(kind.spec(form, Flux.from(entities).map(mapper),
                        safeOptions, scope, BatchGeneratedKeys.none(), columns));
            }
            RepositoryBatchRetention<T> retained = new RepositoryBatchRetention<>(ids, returnGeneratedKeys,
                    safeOptions.maxBufferedBytes(), "combinedRetainedBytes", this::retainedMemory);
            Publisher<?> rows = retained.map(Flux.from(entities)
                    .concatMap(entity -> lifecycle.fire(kind.before(), entity, null).thenReturn(entity)), mapper);
            return client.writeBatch(kind.spec(form, rows, safeOptions, scope, retained.generatedKeys(), columns),
                    index -> Mono.defer(() -> {
                T entity = retained.complete(index);
                return entity != null && postLifecycle ? lifecycle.fire(kind.after(), entity, index) : Mono.empty();
            })).doFinally(signal -> retained.clear());
        });
    }

    private long retainedMemory(T entity, Object row) {
        Object[] entityValuesRoot = entityValues.readRetainedValues(entity);
        long retainedValueBytes;
        long rowOverhead = 0L;
        if (row instanceof BatchUpdate update) {
            retainedValueBytes = update.estimatedRetainedBytes(entityValuesRoot);
            rowOverhead = 128L;
        } else {
            retainedValueBytes = BatchMemoryBudget.estimateValueBytes(new Object[]{entityValuesRoot, row});
        }
        return saturatedAdd(64L + rowOverhead, retainedValueBytes);
    }

    private static long saturatedAdd(long left, long right) {
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }

}
