package com.flying.orm.rdb.repository;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.mapping.EntityMetadata;
import reactor.core.publisher.Mono;

import java.util.Map;
import java.util.Objects;

/**
 * 负责单实体写入和自动版本锁，逻辑删除使用绑定表单的统一规则。
 *
 * <p>它只拼出 {@link WriteSpec} 并交给 {@link ReactiveFormClient}。Scope、安全校验、租户校验、SQL 渲染、
 * 执行保护仍由表单客户端及其下层执行器完成，这里没有第二套写入实现。</p>
 *
 * @param <T> 实体类型
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class ReactiveRepositoryEntityWriter<T> {

    private final ReactiveFormClient client;
    private final DynamicForm form;
    private final EntityMetadata<T> metadata;
    private final EntityValues<T> entityValues;
    private final ReactiveRepositoryLifecycleSupport<T> lifecycle;
    private final RepositoryEntityIdSupport<T> ids;

    ReactiveRepositoryEntityWriter(ReactiveFormClient client,
                                   DynamicForm form,
                                   EntityMetadata<T> metadata,
                                   EntityValues<T> entityValues,
                                   ReactiveRepositoryLifecycleSupport<T> lifecycle,
                                   RepositoryEntityIdSupport<T> ids) {
        this.client = Objects.requireNonNull(client, "reactive form client must not be null");
        this.form = Objects.requireNonNull(form, "repository form must not be null");
        this.metadata = Objects.requireNonNull(metadata, "repository entity metadata must not be null");
        this.entityValues = Objects.requireNonNull(entityValues, "repository entity values must not be null");
        this.lifecycle = Objects.requireNonNull(lifecycle, "repository lifecycle support must not be null");
        this.ids = Objects.requireNonNull(ids, "repository id support must not be null");
    }

    Mono<Long> insert(T entity) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return lifecycle.persist(safeEntity, () -> executeInsert(safeEntity));
    }

    Mono<Long> update(T entity, ConditionGroup where) {
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

    Mono<Long> delete(ConditionGroup where) {
        return client.delete(WriteSpec.delete(form, where));
    }

    Mono<Long> delete(T entity, ConditionGroup where) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return lifecycle.remove(safeEntity, () -> {
            OptimisticLockOptions lock = metadata.versionField().isPresent()
                    ? RepositoryOptimisticLocks.incrementLock(metadata, entityValues.read(safeEntity)).orElse(null)
                    : null;
            WriteSpec spec = WriteSpec.delete(form, where);
            return client.delete(lock == null ? spec : spec.withLock(lock));
        });
    }

    Mono<Long> physicalDelete(ConditionGroup where) {
        return client.physicalDelete(WriteSpec.delete(form, where));
    }

    private Mono<Long> executeInsert(T entity) {
        ids.prepare(entity);
        WriteSpec spec = WriteSpec.insertOwned(form, entityValues.readForInsert(entity));
        if (!ids.databaseGenerated()) {
            return client.insert(spec);
        }
        return client.insertReturningKeys(spec)
                .onErrorMap(GeneratedKeyReadException.class, failure -> {
                    try {
                        return RepositoryFailureSupport.generatedKeyReadFailure(failure);
                    } catch (VirtualMachineError fatal) {
                        return fatal;
                    }
                })
                .map(result -> ids.assignGeneratedKey(entity, result));
    }

}
