package com.flying.orm.rdb.repository;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchWriteCompletion;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.form.BatchOptimisticUpdate;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.mapping.EntityMetadata;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 同步实体批量写入的编排器。
 *
 * <p>它负责把实体流变成表单批量规格，并保留生命周期所需的最小实体引用；真正的有界分片、连接管理、
 * ATOMIC/INDEPENDENT、ENLISTED、UNKNOWN 和 JDBC 参数绑定由 {@link SyncFormClient} 的批量运行时负责。
 * 这样同步 Repository 不会复制一套批量事务实现。</p>
 */
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

    BatchWriteResult insert(List<T> entities) { return insert(support.fromList(entities)); }
    BatchWriteResult upsert(List<T> entities) { return upsert(support.fromList(entities)); }
    BatchWriteResult update(List<T> entities) { return update(support.fromList(entities)); }

    BatchWriteResult insert(Publisher<T> entities) {
        return insert(entities, client.defaultBatchWriteOptions());
    }

    BatchWriteResult upsert(Publisher<T> entities) {
        return upsert(entities, client.defaultBatchWriteOptions());
    }

    BatchWriteResult update(Publisher<T> entities) {
        return update(entities, client.defaultBatchWriteOptions());
    }

    BatchWriteResult insert(Publisher<T> entities, BatchWriteOptions options) {
        return write(entities, options, null, BatchKind.INSERT);
    }

    BatchWriteResult upsert(Publisher<T> entities, BatchWriteOptions options) {
        return write(entities, options, null, BatchKind.UPSERT);
    }

    BatchWriteResult update(Publisher<T> entities, BatchWriteOptions options) {
        return write(entities, options, null, BatchKind.UPDATE);
    }

    BatchWriteResult update(Publisher<T> entities, DataScope scope, BatchWriteOptions options) {
        return write(entities, options, scope, BatchKind.UPDATE);
    }

    BatchExecutionEvidence insertEvidence(Publisher<T> entities, BatchWriteOptions options) {
        return evidence(entities, options, null, BatchKind.INSERT);
    }

    BatchExecutionEvidence upsertEvidence(Publisher<T> entities, BatchWriteOptions options) {
        return evidence(entities, options, null, BatchKind.UPSERT);
    }

    BatchExecutionEvidence updateEvidence(Publisher<T> entities, BatchWriteOptions options) {
        return evidence(entities, options, null, BatchKind.UPDATE);
    }

    BatchExecutionEvidence updateEvidence(Publisher<T> entities, DataScope scope, BatchWriteOptions options) {
        return evidence(entities, options, scope, BatchKind.UPDATE);
    }

    List<BatchChunkResult> insertChunks(Publisher<T> entities, BatchWriteOptions options) {
        return chunks(entities, options, null, BatchKind.INSERT);
    }

    List<BatchChunkResult> upsertChunks(Publisher<T> entities, BatchWriteOptions options) {
        return chunks(entities, options, null, BatchKind.UPSERT);
    }

    List<BatchChunkResult> updateChunks(Publisher<T> entities, BatchWriteOptions options) {
        return chunks(entities, options, null, BatchKind.UPDATE);
    }

    List<BatchChunkResult> updateChunks(Publisher<T> entities, DataScope scope, BatchWriteOptions options) {
        return chunks(entities, options, scope, BatchKind.UPDATE);
    }

    private BatchWriteResult write(Publisher<T> entities,
                                   BatchWriteOptions options,
                                   DataScope scope,
                                   BatchKind kind) {
        BatchWriteOptions safeOptions = requireOptions(options);
        support.requireStableWriteLayout(kind);
        boolean returnGeneratedKeys = support.returnsGeneratedKeys(kind);
        if (!support.lifecyclePlan(kind, returnGeneratedKeys).tracked()) {
            try {
                return client.writeBatch(spec(support.directRows(entities, kind), safeOptions, scope, kind,
                                                  BatchWriteCompletion.noop(), BatchGeneratedKeys.none()));
            } catch (Throwable error) {
                throw RepositoryFailureSupport.propagate(error);
            }
        }
        SyncRepositoryBatchLifecycle<T> retained = support.retention(kind, safeOptions, returnGeneratedKeys);
        try {
            BatchWriteResult result = client.writeBatch(spec(support.trackedRows(entities, kind, retained), safeOptions,
                                                             scope, kind, retained.completion(),
                                                             retained.generatedKeys()));
            retained.finish(result);
            return result;
        } catch (Throwable error) {
            throw RepositoryFailureSupport.propagate(
                    RepositoryFailureSupport.afterCleanup(error, () -> finishFailure(retained, error)));
        }
    }

    private BatchExecutionEvidence evidence(Publisher<T> entities,
                                            BatchWriteOptions options,
                                            DataScope scope,
                                            BatchKind kind) {
        BatchWriteOptions safeOptions = requireOptions(options);
        support.requireStableWriteLayout(kind);
        boolean returnGeneratedKeys = support.returnsGeneratedKeys(kind);
        if (support.lifecyclePlan(kind, returnGeneratedKeys).tracked()) {
            throw new UnsupportedOperationException(
                    "repository batch evidence cannot complete entity lifecycle or generated-key assignment");
        }
        try {
            return client.writeBatchEvidence(spec(support.directRows(entities, kind), safeOptions, scope, kind,
                                                  BatchWriteCompletion.noop(), BatchGeneratedKeys.none()));
        } catch (Throwable error) {
            throw RepositoryFailureSupport.propagate(error);
        }
    }

    private List<BatchChunkResult> chunks(Publisher<T> entities,
                                          BatchWriteOptions options,
                                          DataScope scope,
                                          BatchKind kind) {
        BatchWriteOptions safeOptions = requireOptions(options);
        if (safeOptions.mode() != BatchWriteOptions.Mode.INDEPENDENT) {
            throw new IllegalArgumentException("batch chunks require independent mode");
        }
        support.requireStableWriteLayout(kind);
        boolean returnGeneratedKeys = support.returnsGeneratedKeys(kind);
        if (!support.lifecyclePlan(kind, returnGeneratedKeys).tracked()) {
            try {
                return client.writeBatchChunks(spec(support.directRows(entities, kind), safeOptions, scope, kind,
                                                        BatchWriteCompletion.noop(), BatchGeneratedKeys.none()));
            } catch (Throwable error) {
                throw RepositoryFailureSupport.propagate(error);
            }
        }
        SyncRepositoryBatchLifecycle<T> retained = support.retention(kind, safeOptions, returnGeneratedKeys);
        try {
            List<BatchChunkResult> chunks = client.writeBatchChunks(spec(
                    support.trackedRows(entities, kind, retained), safeOptions, scope, kind,
                    retained.completion(), retained.generatedKeys()));
            retained.finish(BatchWriteResult.from(BatchWriteOptions.Mode.INDEPENDENT, chunks));
            return chunks;
        } catch (Throwable error) {
            throw RepositoryFailureSupport.propagate(
                    RepositoryFailureSupport.afterCleanup(error, () -> finishFailure(retained, error)));
        }
    }

    private static void finishFailure(SyncRepositoryBatchLifecycle<?> retained, Throwable error) {
        if (error instanceof BatchWriteException batchFailure
                && batchFailure.result().mode() == BatchWriteOptions.Mode.INDEPENDENT) {
            retained.finishFailure(batchFailure.result());
            return;
        }
        retained.abort();
    }

    @SuppressWarnings("unchecked")
    private BatchSpec spec(Publisher<?> rows,
                           BatchWriteOptions options,
                           DataScope scope,
                           BatchKind kind,
                           BatchWriteCompletion completion,
                           BatchGeneratedKeys generatedKeys) {
        BatchSpec spec = switch (kind) {
            case INSERT -> BatchSpec.insert(form, (Publisher<Map<String, Object>>) rows);
            case UPSERT -> BatchSpec.upsert(form, (Publisher<Map<String, Object>>) rows);
            case UPDATE -> BatchSpec.update(form, (Publisher<BatchOptimisticUpdate>) rows);
        };
        if (scope != null) {
            spec = spec.withScope(scope);
        }
        spec = spec.withOptions(options).withCompletion(completion);
        return kind == BatchKind.INSERT
                ? spec.withGeneratedKeys(generatedKeys)
                : spec;
    }

    private static BatchWriteOptions requireOptions(BatchWriteOptions options) {
        return Objects.requireNonNull(options, "batch write options must not be null");
    }

    enum BatchKind { INSERT, UPSERT, UPDATE }
}
