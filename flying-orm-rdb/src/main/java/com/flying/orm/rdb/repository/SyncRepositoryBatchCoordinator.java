package com.flying.orm.rdb.repository;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.mapping.EntityMetadata;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/** Converts entity batches into Form requests and releases per-row lifecycle retention. */
final class SyncRepositoryBatchCoordinator<T> {

    private final SyncFormClient client;
    private final DynamicForm form;
    private final EntityMetadata<T> metadata;
    private final EntityValues<T> entityValues;
    private final SyncRepositoryLifecycleSupport<T> lifecycle;
    private final RepositoryEntityIdSupport<T> ids;

    SyncRepositoryBatchCoordinator(SyncFormClient client,
                                   DynamicForm form,
                                   EntityMetadata<T> metadata,
                                   EntityValues<T> entityValues,
                                   SyncRepositoryLifecycleSupport<T> lifecycle,
                                   RepositoryEntityIdSupport<T> ids) {
        this.client = Objects.requireNonNull(client, "sync form client must not be null");
        this.form = Objects.requireNonNull(form, "repository form must not be null");
        this.metadata = Objects.requireNonNull(metadata, "repository entity metadata must not be null");
        this.entityValues = Objects.requireNonNull(entityValues, "repository entity values must not be null");
        this.lifecycle = Objects.requireNonNull(lifecycle, "repository lifecycle support must not be null");
        this.ids = Objects.requireNonNull(ids, "repository entity id support must not be null");
    }

    BatchExecutionEvidence write(List<T> entities, RepositoryBatchKind kind, String[] conflictProperties) {
        return write(fromList(entities), client.defaultBatchWriteOptions(), null, kind, conflictProperties);
    }

    BatchExecutionEvidence writeSingle(T entity, RepositoryBatchKind kind, String[] conflictProperties) {
        return write(List.of(Objects.requireNonNull(entity, "repository entity must not be null")), kind, conflictProperties);
    }

    BatchExecutionEvidence write(Publisher<T> entities,
                                 BatchWriteOptions options, DataScope scope, RepositoryBatchKind kind,
                                 String[] conflictProperties) {
        String[] columns = RepositoryBatchKind.conflictColumns(metadata, conflictProperties);
        BatchWriteOptions safeOptions = Objects.requireNonNull(options, "batch write options must not be null");
        Function<T, ?> mapper = kind.mapper(metadata, entityValues, ids, columns != null);
        EntityLifecyclePhase after = kind.after();
        boolean returnGeneratedKeys = (kind == RepositoryBatchKind.INSERT
                || kind == RepositoryBatchKind.UPSERT && columns == null) && ids.databaseGenerated();
        boolean postLifecycle = lifecycle.hasWork();
        if (!postLifecycle && !returnGeneratedKeys) {
            return client.writeBatch(kind.spec(form, Flux.from(entities).map(mapper),
                    safeOptions, scope, BatchGeneratedKeys.none(), columns));
        }
        RepositoryBatchRetention<T> retained = new RepositoryBatchRetention<>(ids, returnGeneratedKeys,
                safeOptions.maxBufferedBytes(), "lifecycleRetainedBytes", this::retainedMemory);
        try {
            return client.writeBatch(kind.spec(form,
                    retained.map(lifecycle.beforeRows(entities, kind.before()), mapper),
                    safeOptions, scope, retained.generatedKeys(), columns), offset -> {
                        T entity = retained.complete(offset);
                        if (entity != null && postLifecycle) lifecycle.fire(after, entity, offset);
                    });
        } finally {
            retained.clear();
        }
    }

    private long retainedMemory(T entity, Object row) {
        long bytes = BatchMemoryBudget.estimateRowBytes(entityValues.readRetainedValues(entity));
        return bytes > Long.MAX_VALUE - 64L ? Long.MAX_VALUE : bytes + 64L;
    }

    private Publisher<T> fromList(List<T> entities) {
        lifecycle.rejectNonBlockingThread();
        // 同步调用返回前消费完列表，不复制整批引用数组。
        List<T> safeEntities = Objects.requireNonNull(entities, "repository entities must not be null");
        safeEntities.forEach(entity -> Objects.requireNonNull(entity, "repository entity must not be null"));
        return Flux.fromIterable(safeEntities);
    }

}
