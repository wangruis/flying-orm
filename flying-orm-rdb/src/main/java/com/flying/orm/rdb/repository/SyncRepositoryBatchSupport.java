package com.flying.orm.rdb.repository;

import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.form.BatchOptimisticUpdate;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.MappingException;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 同步 Repository 批量写入的实体转换和生命周期准备。
 *
 * <p>这里不获取连接、不管理事务，只把实体按既有规则转换为批量运行时需要的行。</p>
 */
final class SyncRepositoryBatchSupport<T> {

    private final EntityMetadata<T> metadata;
    private final EntityValues<T> entityValues;
    private final SyncRepositoryLifecycleSupport<T> lifecycle;
    private final RepositoryEntityIdSupport<T> ids;

    SyncRepositoryBatchSupport(EntityMetadata<T> metadata,
                               EntityValues<T> entityValues,
                               SyncRepositoryLifecycleSupport<T> lifecycle,
                               RepositoryEntityIdSupport<T> ids) {
        this.metadata = Objects.requireNonNull(metadata, "repository entity metadata must not be null");
        this.entityValues = Objects.requireNonNull(entityValues, "repository entity values must not be null");
        this.lifecycle = Objects.requireNonNull(lifecycle, "repository lifecycle support must not be null");
        this.ids = Objects.requireNonNull(ids, "repository entity id support must not be null");
    }

    Publisher<?> directRows(Publisher<T> entities, SyncRepositoryBatchCoordinator.BatchKind kind) {
        return switch (kind) {
            case INSERT -> new SyncRepositoryBatchRows<>(entities, this::prepareInsert);
            case UPSERT -> new SyncRepositoryBatchRows<>(entities, this::prepareUpsert);
            case UPDATE -> new SyncRepositoryBatchRows<>(entities, this::optimisticUpdate);
        };
    }

    Publisher<?> trackedRows(Publisher<T> entities,
                             SyncRepositoryBatchCoordinator.BatchKind kind,
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

    SyncRepositoryBatchLifecycle<T> retention(SyncRepositoryBatchCoordinator.BatchKind kind,
                                              BatchWriteOptions options,
                                              boolean retainForGeneratedKeys) {
        EntityLifecyclePhase after = kind == SyncRepositoryBatchCoordinator.BatchKind.UPDATE
                ? EntityLifecyclePhase.POST_UPDATE : EntityLifecyclePhase.POST_PERSIST;
        return new SyncRepositoryBatchLifecycle<>(lifecycle, after, entityValues, ids, retainForGeneratedKeys,
                                                   options.maxBufferedBytes());
    }

    RepositoryBatchLifecyclePlan lifecyclePlan(SyncRepositoryBatchCoordinator.BatchKind kind,
                                                boolean returnGeneratedKeys) {
        EntityLifecyclePhase after = kind == SyncRepositoryBatchCoordinator.BatchKind.UPDATE
                ? EntityLifecyclePhase.POST_UPDATE : EntityLifecyclePhase.POST_PERSIST;
        return RepositoryBatchLifecyclePlan.select(lifecycle.hasWork(after), returnGeneratedKeys);
    }

    void requireStableWriteLayout(SyncRepositoryBatchCoordinator.BatchKind kind) {
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

    boolean returnsGeneratedKeys(SyncRepositoryBatchCoordinator.BatchKind kind) {
        return kind == SyncRepositoryBatchCoordinator.BatchKind.INSERT && ids.databaseGenerated();
    }

    Publisher<T> fromList(List<T> entities) {
        lifecycle.rejectNonBlockingThread();
        // 同步调用会在返回前消费完列表，不需要为了快照再复制一份可能很大的引用数组。
        List<T> safeEntities = Objects.requireNonNull(entities, "repository entities must not be null");
        safeEntities.forEach(entity -> Objects.requireNonNull(entity, "repository entity must not be null"));
        return SyncRepositoryPublishers.fromIterable(safeEntities);
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
}
