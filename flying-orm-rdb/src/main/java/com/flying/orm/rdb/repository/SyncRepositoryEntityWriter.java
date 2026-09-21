package com.flying.orm.rdb.repository;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.mapping.EntityMetadata;

import java.util.Map;
import java.util.Objects;

/**
 * 同步单实体写入协作者。
 *
 * <p>它沿用响应式 Repository 的规则：实体版本列自动转成乐观锁条件，版本列不重复进入 SET；实体逻辑删除
 * 已在绑定表单时确定，由 {@link SyncFormClient} 和字段、Scope、租户、执行保护一起统一处理。</p>
 */
final class SyncRepositoryEntityWriter<T> {

    private final SyncFormClient client;
    private final DynamicForm form;
    private final EntityMetadata<T> metadata;
    private final EntityValues<T> entityValues;
    private final SyncRepositoryLifecycleSupport<T> lifecycle;
    private final RepositoryEntityIdSupport<T> ids;

    SyncRepositoryEntityWriter(SyncFormClient client,
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
        this.ids = Objects.requireNonNull(ids, "repository id support must not be null");
    }

    long insert(T entity) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        if (!lifecycle.hasWork(EntityLifecyclePhase.POST_PERSIST)) {
            return executeInsert(safeEntity);
        }
        return lifecycle.persist(safeEntity, () -> executeInsert(safeEntity));
    }

    long update(T entity, ConditionGroup where) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return lifecycle.update(safeEntity, () -> {
            Map<String, Object> allValues = metadata.versionField().isPresent()
                    ? entityValues.read(safeEntity) : null;
            Map<String, Object> row = entityValues.readForUpdate(safeEntity);
            OptimisticLockOptions lock = allValues == null ? null
                    : RepositoryOptimisticLocks.incrementLock(metadata, allValues).orElse(null);
            if (lock != null) {
                row = RepositoryOptimisticLocks.withoutLockField(row, lock);
            }
            WriteSpec spec = WriteSpec.updateOwned(form, row, where);
            return client.update(lock == null ? spec : spec.withLock(lock));
        });
    }

    long delete(ConditionGroup where) {
        return client.delete(WriteSpec.delete(form, where));
    }

    long delete(T entity, ConditionGroup where) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return lifecycle.remove(safeEntity, () -> {
            OptimisticLockOptions lock = metadata.versionField().isPresent()
                    ? RepositoryOptimisticLocks.incrementLock(metadata, entityValues.read(safeEntity)).orElse(null)
                    : null;
            WriteSpec spec = WriteSpec.delete(form, where);
            return client.delete(lock == null ? spec : spec.withLock(lock));
        });
    }

    long physicalDelete(ConditionGroup where) {
        return client.physicalDelete(WriteSpec.delete(form, where));
    }

    private long executeInsert(T entity) {
        ids.prepare(entity);
        WriteSpec spec = WriteSpec.insertOwned(form, entityValues.readForInsert(entity));
        if (!ids.databaseGenerated()) {
            return client.insert(spec);
        }
        final com.flying.orm.rdb.execution.SqlWriteResult result;
        try {
            result = client.insertReturningKeys(spec);
        } catch (GeneratedKeyReadException failure) {
            throw RepositoryFailureSupport.generatedKeyReadFailure(failure);
        }
        return ids.assignGeneratedKey(entity, result);
    }

}
