package com.flying.orm.rdb.repository;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.lifecycle.ReactiveEntityListener;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.operator.EntityDmlDeleteOperator;
import com.flying.orm.rdb.operator.EntityDmlQueryOperator;
import com.flying.orm.rdb.operator.EntityDmlUpdateOperator;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * 实体侧的轻量响应式 Repository。
 *
 * <p>这个类是稳定的实体入口，不重新实现 ORM 内核。实体映射交给客户端的元数据模型，SQL、Scope、
 * 逻辑删除、乐观锁、批量事务、执行保护和异常分类继续统一走 {@link ReactiveFormClient}。</p>
 *
 * <p>内部职责已经分开：单实体写入由 {@link ReactiveRepositoryEntityWriter} 负责，批量协调由
 * {@link ReactiveRepositoryBatchCoordinator} 负责，查询规格和分页结果由 {@link ReactiveRepositoryReadMapper}
 * 负责，生命周期顺序由 {@link ReactiveRepositoryLifecycleSupport} 负责。本门面保留原有全部
 * public/package-private 方法签名，便于实体操作符和同包调用继续使用。</p>
 *
 * <p>Repository 创建后不可变，可以并发共享。所有 Mono/Flux 都是惰性的；真正的实体转换、连接获取和 SQL
 * 执行发生在订阅时，取消和背压不会被这里偷偷吞掉。</p>
 *
 * @param <T> 实体类型
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
public final class ReactiveFormRepository<T> {

    private final ReactiveFormClient client;
    private final DynamicForm form;
    private final Class<T> entityType;
    private final Function<T, Map<String, Object>> values;
    private final Function<T, Map<String, Object>> insertValues;
    private final Function<T, Map<String, Object>> updateValues;
    private final Function<T, Map<String, Object>> upsertValues;
    private final ReactiveRepositoryEntityWriter<T> entityWriter;
    private final ReactiveRepositoryBatchOperations<T> batchOperations;
    private final ReactiveRepositoryReadMapper<T> readMapper;

    private ReactiveFormRepository(ReactiveFormClient client,
                                   DynamicForm form,
                                   Class<T> type,
                                   Function<T, Map<String, Object>> values,
                                   Function<T, Map<String, Object>> insertValues,
                                   Function<T, Map<String, Object>> updateValues,
                                   Function<T, Map<String, Object>> upsertValues,
                                   ReactiveEntityListener<T> listener) {
        this.client = Objects.requireNonNull(client, "reactive form client must not be null");
        this.form = Objects.requireNonNull(form, "repository form must not be null");
        this.entityType = Objects.requireNonNull(type, "repository type must not be null");
        this.values = Objects.requireNonNull(values, "repository entity values must not be null");
        this.insertValues = Objects.requireNonNull(insertValues, "repository insert values must not be null");
        this.updateValues = Objects.requireNonNull(updateValues, "repository update values must not be null");
        this.upsertValues = Objects.requireNonNull(upsertValues, "repository upsert values must not be null");
        EntityMetadata<T> metadata = client.entityModels().metadata(entityType);
        RepositoryEntityIdSupport<T> ids = RepositoryEntityIdSupport.create(
                metadata, client.entityModels().idGenerator());
        ReactiveRepositoryLifecycleSupport<T> lifecycle =
                new ReactiveRepositoryLifecycleSupport<>(metadata, listener);
        this.entityWriter = new ReactiveRepositoryEntityWriter<>(client, form, metadata,
                                                                 values, insertValues, updateValues, lifecycle);
        ReactiveRepositoryBatchCoordinator<T> batchCoordinator = new ReactiveRepositoryBatchCoordinator<>(metadata,
                values, insertValues, upsertValues, updateValues, lifecycle, ids);
        this.batchOperations = new ReactiveRepositoryBatchOperations<>(client, form, batchCoordinator);
        this.readMapper = new ReactiveRepositoryReadMapper<>(client, form, entityType, metadata, lifecycle);
    }

    /**
     * 使用默认实体反射映射创建 Repository。默认映射从客户端实体模型缓存中取得，和其他实体入口共用同一套安全规则。
     *
     * @param client 响应式动态表单客户端
     * @param form 实体对应的动态表单
     * @param type 实体类型
     * @param <T> 实体类型
     * @return 可并发共享的 Repository
     */
    public static <T> ReactiveFormRepository<T> create(ReactiveFormClient client,
                                                       DynamicForm form,
                                                       Class<T> type) {
        EntityValues<T> entityValues = client.entityModels().entityValues(type);
        return new ReactiveFormRepository<>(client, form, type, entityValues::read, entityValues::readForInsert,
                entityValues::readForUpdate, entityValues::readForUpsert, null);
    }

    /** 返回一个使用相同映射、客户端和安全配置，但追加了生命周期监听器的新 Repository。 */
    public ReactiveFormRepository<T> withListener(ReactiveEntityListener<T> listener) {
        return new ReactiveFormRepository<>(client, form, entityType,
                                            values, insertValues, updateValues, upsertValues,
                                            Objects.requireNonNull(listener,
                                                                   "entity lifecycle listener must not be null"));
    }

    /** @return 当前实体的 Lambda 查询命令 */
    public EntityDmlQueryOperator<T> createQuery() {
        return client.entity(entityType).query();
    }

    /** @return 当前实体的 Lambda 更新命令 */
    public EntityDmlUpdateOperator<T> createUpdate() {
        return client.entity(entityType).update();
    }

    /** @return 当前实体的 Lambda 删除命令 */
    public EntityDmlDeleteOperator<T> createDelete() {
        return client.entity(entityType).delete();
    }

    static <T> ReactiveFormRepository<T> create(ReactiveFormClient client,
                                                DynamicForm form,
                                                Class<T> type,
                                                Function<T, Map<String, Object>> values) {
        Function<T, Map<String, Object>> safeValues = Objects.requireNonNull(
                values, "repository entity values must not be null");
        return new ReactiveFormRepository<>(client, form, type, safeValues, safeValues, safeValues, safeValues, null);
    }

    public Mono<Long> insert(T entity) { return entityWriter.insert(entity); }
    public Mono<BatchWriteResult> insertBatch(List<T> entities) { return batchOperations.insert(entities); }
    public Mono<BatchWriteResult> upsertBatch(List<T> entities) { return batchOperations.upsert(entities); }
    Mono<BatchWriteResult> insertBatch(Publisher<T> entities) { return batchOperations.insert(entities); }
    Mono<BatchWriteResult> upsertBatch(Publisher<T> entities) { return batchOperations.upsert(entities); }
    public Mono<BatchWriteResult> insertBatch(Publisher<T> entities, BatchWriteOptions options) {
        return batchOperations.insert(entities, options);
    }
    public Mono<BatchWriteResult> upsertBatch(Publisher<T> entities, BatchWriteOptions options) {
        return batchOperations.upsert(entities, options);
    }
    public Flux<BatchChunkResult> insertBatchChunks(Publisher<T> entities, BatchWriteOptions options) {
        return batchOperations.insertChunks(entities, options);
    }
    public Flux<BatchChunkResult> upsertBatchChunks(Publisher<T> entities, BatchWriteOptions options) {
        return batchOperations.upsertChunks(entities, options);
    }
    public Mono<BatchWriteResult> updateBatch(List<T> entities) { return batchOperations.update(entities); }
    Mono<BatchWriteResult> updateBatch(Publisher<T> entities) { return batchOperations.update(entities); }
    public Mono<BatchWriteResult> updateBatch(Publisher<T> entities, BatchWriteOptions options) {
        return batchOperations.update(entities, options);
    }
    public Mono<BatchWriteResult> updateBatch(Publisher<T> entities, DataScope scope, BatchWriteOptions options) {
        return batchOperations.update(entities, scope, options);
    }
    public Flux<BatchChunkResult> updateBatchChunks(Publisher<T> entities, BatchWriteOptions options) {
        return batchOperations.updateChunks(entities, options);
    }
    public Flux<BatchChunkResult> updateBatchChunks(Publisher<T> entities,
                                                     DataScope scope,
                                                     BatchWriteOptions options) {
        return batchOperations.updateChunks(entities, scope, options);
    }

    public Mono<Long> update(T entity, ConditionGroup where) { return entityWriter.update(entity, where); }
    Mono<Long> update(T entity, ConditionGroup where, SqlExecutionOptions options) {
        return entityWriter.update(entity, where, options);
    }
    Mono<Long> update(T entity, ConditionGroup where, DataScope scope) { return entityWriter.update(entity, where, scope); }
    Mono<Long> update(T entity, ConditionGroup where, DataScope scope, SqlExecutionOptions options) {
        return entityWriter.update(entity, where, scope, options);
    }
    Mono<Long> update(T entity, ConditionGroup where, OptimisticLockOptions lock) {
        return entityWriter.updateWithLock(entity, where, lock);
    }
    Mono<Long> update(T entity, ConditionGroup where, OptimisticLockOptions lock, SqlExecutionOptions options) {
        return entityWriter.updateWithLock(entity, where, lock, options);
    }
    Mono<Long> update(T entity, ConditionGroup where, DataScope scope, OptimisticLockOptions lock) {
        return entityWriter.updateWithLock(entity, where, lock, scope);
    }
    Mono<Long> update(T entity, ConditionGroup where, DataScope scope,
                      OptimisticLockOptions lock, SqlExecutionOptions options) {
        return entityWriter.updateWithLock(entity, where, lock, scope, options);
    }

    public Mono<Long> delete(ConditionGroup where) { return entityWriter.delete(where); }
    public Mono<Long> delete(T entity, ConditionGroup where) { return entityWriter.delete(entity, where); }
    Mono<Long> delete(ConditionGroup where, SqlExecutionOptions options) { return entityWriter.delete(where, options); }
    Mono<Long> delete(ConditionGroup where, DataScope scope) { return entityWriter.delete(where, scope); }
    Mono<Long> delete(ConditionGroup where, DataScope scope, SqlExecutionOptions options) {
        return entityWriter.delete(where, scope, options);
    }
    Mono<Long> delete(T entity, ConditionGroup where, SqlExecutionOptions options) {
        return entityWriter.delete(entity, where, options);
    }
    Mono<Long> delete(T entity, ConditionGroup where, DataScope scope) { return entityWriter.delete(entity, where, scope); }
    Mono<Long> delete(T entity, ConditionGroup where, DataScope scope, SqlExecutionOptions options) {
        return entityWriter.delete(entity, where, scope, options);
    }
    Mono<Long> delete(ConditionGroup where, OptimisticLockOptions lock) { return entityWriter.delete(where, lock); }
    Mono<Long> delete(ConditionGroup where, OptimisticLockOptions lock, SqlExecutionOptions options) {
        return entityWriter.delete(where, lock, options);
    }
    Mono<Long> delete(ConditionGroup where, DataScope scope, OptimisticLockOptions lock) {
        return entityWriter.delete(where, scope, lock);
    }
    Mono<Long> delete(ConditionGroup where, DataScope scope,
                      OptimisticLockOptions lock, SqlExecutionOptions options) {
        return entityWriter.delete(where, scope, lock, options);
    }

    public Mono<Long> physicalDelete(ConditionGroup where) { return entityWriter.physicalDelete(where); }
    Mono<Long> physicalDelete(ConditionGroup where, SqlExecutionOptions options) {
        return entityWriter.physicalDelete(where, options);
    }
    Mono<Long> physicalDelete(ConditionGroup where, OptimisticLockOptions lock) {
        return entityWriter.physicalDelete(where, lock);
    }
    Mono<Long> physicalDelete(ConditionGroup where, OptimisticLockOptions lock, SqlExecutionOptions options) {
        return entityWriter.physicalDelete(where, lock, options);
    }
    Mono<Long> physicalDelete(ConditionGroup where, DataScope scope) { return entityWriter.physicalDelete(where, scope); }
    Mono<Long> physicalDelete(ConditionGroup where, DataScope scope, SqlExecutionOptions options) {
        return entityWriter.physicalDelete(where, scope, options);
    }
    Mono<Long> physicalDelete(ConditionGroup where, DataScope scope, OptimisticLockOptions lock) {
        return entityWriter.physicalDelete(where, scope, lock);
    }
    Mono<Long> physicalDelete(ConditionGroup where, DataScope scope,
                              OptimisticLockOptions lock, SqlExecutionOptions options) {
        return entityWriter.physicalDelete(where, scope, lock, options);
    }

    public Flux<T> select(ConditionGroup where) { return readMapper.select(where, null, null); }
    Flux<T> select(ConditionGroup where, SqlExecutionOptions options) { return readMapper.select(where, null, options); }
    Flux<T> select(ConditionGroup where, DataScope scope) { return readMapper.select(where, scope, null); }
    Flux<T> select(ConditionGroup where, DataScope scope, SqlExecutionOptions options) {
        return readMapper.select(where, scope, options);
    }
    public Mono<PageResult<T>> page(ConditionGroup where, PageQuery page) {
        return readMapper.page(where, page, null, null);
    }
    Mono<PageResult<T>> page(ConditionGroup where, PageQuery page, SqlExecutionOptions options) {
        return readMapper.page(where, page, null, options);
    }
    Mono<PageResult<T>> page(ConditionGroup where, PageQuery page, DataScope scope) {
        return readMapper.page(where, page, scope, null);
    }
    Mono<PageResult<T>> page(ConditionGroup where, PageQuery page, DataScope scope, SqlExecutionOptions options) {
        return readMapper.page(where, page, scope, options);
    }

}
