package com.flying.orm.rdb.repository;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchGeneratedKeys;
import com.flying.orm.rdb.batch.BatchWriteCompletion;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.form.BatchOptimisticUpdate;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.MappingException;
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

    BatchWriteResult insert(List<T> entities) { return insert(fromList(entities)); }
    BatchWriteResult upsert(List<T> entities) { return upsert(fromList(entities)); }
    BatchWriteResult update(List<T> entities) { return update(fromList(entities)); }

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
        requireStableWriteLayout(kind);
        boolean returnGeneratedKeys = returnsGeneratedKeys(kind);
        if (!lifecyclePlan(kind, returnGeneratedKeys).tracked()) {
            try {
                return client.writeBatch(spec(directRows(entities, kind), safeOptions, scope, kind,
                                                  BatchWriteCompletion.noop(), BatchGeneratedKeys.none()));
            } catch (Throwable error) {
                throw RepositoryFailureSupport.propagate(error);
            }
        }
        SyncRepositoryBatchLifecycle<T> retained = retention(kind, safeOptions, returnGeneratedKeys);
        try {
            BatchWriteResult result = client.writeBatch(spec(trackedRows(entities, kind, retained), safeOptions,
                                                             scope, kind, retained.completion(),
                                                             retained.generatedKeys()));
            retained.finish(result);
            return result;
        } catch (Throwable error) {
            throw RepositoryFailureSupport.propagate(
                    RepositoryFailureSupport.afterCleanup(error, () -> finishFailure(retained, error)));
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
        requireStableWriteLayout(kind);
        boolean returnGeneratedKeys = returnsGeneratedKeys(kind);
        if (!lifecyclePlan(kind, returnGeneratedKeys).tracked()) {
            try {
                return client.writeBatchChunks(spec(directRows(entities, kind), safeOptions, scope, kind,
                                                        BatchWriteCompletion.noop(), BatchGeneratedKeys.none()));
            } catch (Throwable error) {
                throw RepositoryFailureSupport.propagate(error);
            }
        }
        SyncRepositoryBatchLifecycle<T> retained = retention(kind, safeOptions, returnGeneratedKeys);
        try {
            List<BatchChunkResult> chunks = client.writeBatchChunks(spec(
                    trackedRows(entities, kind, retained), safeOptions, scope, kind,
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

    private Publisher<?> directRows(Publisher<T> entities, BatchKind kind) {
        return switch (kind) {
            case INSERT -> new SyncRepositoryBatchRows<>(entities, this::prepareInsert);
            case UPSERT -> new SyncRepositoryBatchRows<>(entities, this::prepareUpsert);
            case UPDATE -> new SyncRepositoryBatchRows<>(entities, this::optimisticUpdate);
        };
    }

    private Publisher<?> trackedRows(Publisher<T> entities,
                                     BatchKind kind,
                                     SyncRepositoryBatchLifecycle<T> retained) {
        return switch (kind) {
            case INSERT -> new SyncRepositoryBatchRows<>(entities, this::prepareInsert, EntityLifecyclePhase.PRE_PERSIST,
                                                          lifecycle, retained);
            case UPSERT -> new SyncRepositoryBatchRows<>(entities, this::prepareUpsert, EntityLifecyclePhase.PRE_PERSIST,
                                                          lifecycle, retained);
            case UPDATE -> new SyncRepositoryBatchRows<>(entities, this::optimisticUpdate,
                                                          EntityLifecyclePhase.PRE_UPDATE, lifecycle, retained);
        };
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

    private BatchOptimisticUpdate optimisticUpdate(T entity) {
        return RepositoryOptimisticLocks.batchUpdate(
                metadata, entityValues.read(entity), entityValues.readForUpdate(entity));
    }

    /** 在 JDBC 批量参数行创建前执行和单条 insert 一致的主键校验或生成。 */
    private Map<String, Object> prepareInsert(T entity) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        ids.prepare(safeEntity);
        return entityValues.readForInsert(safeEntity);
    }

    /** upsert 仍然可能走 insert，不能绕过主键准备。 */
    private Map<String, Object> prepareUpsert(T entity) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        ids.prepare(safeEntity);
        return entityValues.repositoryUpsertValues(safeEntity);
    }

    private SyncRepositoryBatchLifecycle<T> retention(BatchKind kind,
                                                       BatchWriteOptions options,
                                                       boolean retainForGeneratedKeys) {
        EntityLifecyclePhase after = kind == BatchKind.UPDATE
                ? EntityLifecyclePhase.POST_UPDATE : EntityLifecyclePhase.POST_PERSIST;
        return new SyncRepositoryBatchLifecycle<>(lifecycle, after, entityValues, ids, retainForGeneratedKeys,
                                                   options.maxBufferedBytes());
    }

    private RepositoryBatchLifecyclePlan lifecyclePlan(BatchKind kind, boolean returnGeneratedKeys) {
        EntityLifecyclePhase after = kind == BatchKind.UPDATE
                ? EntityLifecyclePhase.POST_UPDATE : EntityLifecyclePhase.POST_PERSIST;
        return RepositoryBatchLifecyclePlan.select(lifecycle.hasWork(after), returnGeneratedKeys);
    }

    private void requireStableWriteLayout(BatchKind kind) {
        switch (kind) {
            case INSERT -> RepositoryBatchLayoutPolicy.requireStableInsertLayout(metadata);
            case UPSERT -> {
                // 数据库生成的主键在写入前没有冲突目标，不能把语义不明确的 upsert 交给数据库猜。
                // 这段检查发生在 rows Publisher 被订阅前，因此不会消费输入，也不会获取连接或执行 SQL。
                if (ids.databaseGenerated()) {
                    throw new MappingException(
                            "repository batch upsert does not support AUTO or sequence-generated primary keys");
                }
                RepositoryBatchLayoutPolicy.requireStableUpsertLayout(metadata);
            }
            case UPDATE -> {
                // 本次只收紧 insert/upsert；update 继续使用既有的乐观锁批量计划和参数规则。
            }
        }
    }

    private boolean returnsGeneratedKeys(BatchKind kind) {
        return kind == BatchKind.INSERT && ids.databaseGenerated();
    }

    private Publisher<T> fromList(List<T> entities) {
        lifecycle.rejectNonBlockingThread();
        // 同步调用会在返回前消费完列表，不需要为了快照再复制一份可能很大的引用数组。
        List<T> safeEntities = Objects.requireNonNull(entities, "repository entities must not be null");
        safeEntities.forEach(entity -> Objects.requireNonNull(entity, "repository entity must not be null"));
        return SyncRepositoryPublishers.fromIterable(safeEntities);
    }

    private static BatchWriteOptions requireOptions(BatchWriteOptions options) {
        return Objects.requireNonNull(options, "batch write options must not be null");
    }

    private enum BatchKind { INSERT, UPSERT, UPDATE }
}
