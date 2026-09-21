package com.flying.orm.rdb.repository;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.form.BatchOptimisticUpdate;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.mapping.EntityMetadata;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Converts entity batches into Form requests and releases per-row lifecycle retention. */
final class SyncRepositoryBatchCoordinator<T> {

    private final SyncFormClient client;
    private final DynamicForm form;
    private final SyncRepositoryBatchSupport<T> support;

    SyncRepositoryBatchCoordinator(SyncFormClient client,
                                   DynamicForm form,
                                   EntityMetadata<T> metadata,
                                   EntityValues<T> entityValues,
                                   SyncRepositoryLifecycleSupport<T> lifecycle,
                                   RepositoryEntityIdSupport<T> ids) {
        this.client = Objects.requireNonNull(client, "sync form client must not be null");
        this.form = Objects.requireNonNull(form, "repository form must not be null");
        this.support = new SyncRepositoryBatchSupport<>(metadata, entityValues, lifecycle, ids);
    }

    BatchExecutionEvidence insert(List<T> entities) { return insert(support.fromList(entities)); }
    BatchExecutionEvidence upsert(List<T> entities) { return upsert(support.fromList(entities)); }
    BatchExecutionEvidence update(List<T> entities) { return update(support.fromList(entities)); }

    BatchExecutionEvidence insert(Publisher<T> entities) {
        return insert(entities, client.defaultBatchWriteOptions());
    }

    BatchExecutionEvidence upsert(Publisher<T> entities) {
        return upsert(entities, client.defaultBatchWriteOptions());
    }

    BatchExecutionEvidence update(Publisher<T> entities) {
        return update(entities, client.defaultBatchWriteOptions());
    }

    BatchExecutionEvidence insert(Publisher<T> entities, BatchWriteOptions options) {
        return write(entities, options, null, BatchKind.INSERT);
    }

    BatchExecutionEvidence upsert(Publisher<T> entities, BatchWriteOptions options) {
        return write(entities, options, null, BatchKind.UPSERT);
    }

    BatchExecutionEvidence update(Publisher<T> entities, BatchWriteOptions options) {
        return write(entities, options, null, BatchKind.UPDATE);
    }

    BatchExecutionEvidence update(Publisher<T> entities, DataScope scope, BatchWriteOptions options) {
        return write(entities, options, scope, BatchKind.UPDATE);
    }

    BatchExecutionEvidence insertEvidence(Publisher<T> entities, BatchWriteOptions options) {
        return write(entities, options, null, BatchKind.INSERT);
    }

    BatchExecutionEvidence upsertEvidence(Publisher<T> entities, BatchWriteOptions options) {
        return write(entities, options, null, BatchKind.UPSERT);
    }

    BatchExecutionEvidence updateEvidence(Publisher<T> entities, BatchWriteOptions options) {
        return write(entities, options, null, BatchKind.UPDATE);
    }

    BatchExecutionEvidence updateEvidence(Publisher<T> entities, DataScope scope, BatchWriteOptions options) {
        return write(entities, options, scope, BatchKind.UPDATE);
    }





    private BatchExecutionEvidence write(Publisher<T> entities,
                                         BatchWriteOptions options, DataScope scope, BatchKind kind) {
        BatchWriteOptions safeOptions = requireOptions(options);
        support.requireStableWriteLayout(kind);
        boolean returnGeneratedKeys = support.returnsGeneratedKeys(kind);
        if (!support.requiresLifecycleTracking(kind, returnGeneratedKeys)) {
            return client.writeBatch(spec(support.directRows(entities, kind), safeOptions, scope, kind,
                    BatchGeneratedKeys.none()));
        }
        SyncRepositoryBatchLifecycle<T> retained = support.retention(kind, safeOptions, returnGeneratedKeys);
        try {
            return client.writeBatch(spec(support.trackedRows(entities, kind, retained), safeOptions,
                    scope, kind, retained.generatedKeys()), retained::rowCompleted);
        } finally {
            retained.abort();
        }
    }

    @SuppressWarnings("unchecked")
    private BatchSpec spec(Publisher<?> rows,
                           BatchWriteOptions options,
                           DataScope scope,
                           BatchKind kind,
                           BatchGeneratedKeys generatedKeys) {
        BatchSpec spec = switch (kind) {
            case INSERT -> BatchSpec.insert(form, (Publisher<Map<String, Object>>) rows);
            case UPSERT -> BatchSpec.upsert(form, (Publisher<Map<String, Object>>) rows);
            case UPDATE -> BatchSpec.update(form, (Publisher<BatchOptimisticUpdate>) rows);
        };
        if (scope != null) {
            spec = spec.withScope(scope);
        }
        spec = spec.withOptions(options);
        return kind == BatchKind.INSERT
                ? spec.withGeneratedKeys(generatedKeys)
                : spec;
    }

    private static BatchWriteOptions requireOptions(BatchWriteOptions options) {
        return Objects.requireNonNull(options, "batch write options must not be null");
    }

    enum BatchKind { INSERT, UPSERT, UPDATE }
}
