package com.flying.orm.rdb.repository;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.lifecycle.ReactiveEntityListener;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.operator.EntityDmlDeleteOperator;
import com.flying.orm.rdb.operator.EntityDmlOperator;
import com.flying.orm.rdb.operator.EntityDmlQueryOperator;
import com.flying.orm.rdb.operator.EntityDmlUpdateOperator;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.Objects;

/**
 * 实体侧的轻量响应式 Repository。
 *
 * <p>这个类是稳定的实体入口，不重新实现 ORM 内核。实体映射交给客户端的元数据模型，SQL、Scope、
 * 逻辑删除、乐观锁、批量事务、执行保护和异常分类继续统一走 {@link ReactiveFormClient}。</p>
 *
 * <p>内部职责已经分开：单实体写入由 {@link ReactiveRepositoryEntityWriter} 负责，批量协调由
 * {@link ReactiveRepositoryBatchCoordinator} 负责，查询规格和分页结果由 {@link ReactiveRepositoryReadMapper}
 * 负责，生命周期顺序由 {@link ReactiveRepositoryLifecycleSupport} 负责。本门面只保留使用者入口，
 * 带 Scope、乐观锁和执行选项的组合由实体操作符及写入协作者表达。</p>
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
    private final EntityValues<T> entityValues;
    private final ReactiveRepositoryEntityWriter<T> entityWriter;
    private final ReactiveRepositoryBatchOperations<T> batchOperations;
    private final ReactiveRepositoryReadMapper<T> readMapper;

    private ReactiveFormRepository(ReactiveFormClient client,
                                   DynamicForm form,
                                   Class<T> type,
                                   EntityValues<T> entityValues,
                                   ReactiveEntityListener<T> listener) {
        this.client = Objects.requireNonNull(client, "reactive form client must not be null");
        this.form = Objects.requireNonNull(form, "repository form must not be null");
        this.entityType = Objects.requireNonNull(type, "repository type must not be null");
        this.entityValues = Objects.requireNonNull(entityValues, "repository entity values must not be null");
        EntityMetadata<T> metadata = client.entityModels().metadata(entityType);
        RepositoryEntityIdSupport<T> ids = RepositoryEntityIdSupport.create(
                metadata, client.entityModels().idGenerator());
        ReactiveRepositoryLifecycleSupport<T> lifecycle =
                new ReactiveRepositoryLifecycleSupport<>(metadata, listener);
        this.entityWriter = new ReactiveRepositoryEntityWriter<>(
                client, form, metadata, this.entityValues, lifecycle);
        ReactiveRepositoryBatchCoordinator<T> batchCoordinator = new ReactiveRepositoryBatchCoordinator<>(metadata,
                this.entityValues, lifecycle, ids);
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
        return new ReactiveFormRepository<>(client, form, type, entityValues, null);
    }

    /** 返回一个使用相同映射、客户端和安全配置，但追加了生命周期监听器的新 Repository。 */
    public ReactiveFormRepository<T> withListener(ReactiveEntityListener<T> listener) {
        return new ReactiveFormRepository<>(client, form, entityType,
                                            entityValues,
                                            Objects.requireNonNull(listener,
                                                                   "entity lifecycle listener must not be null"));
    }

    /** @return 当前实体的 Lambda 查询命令 */
    public EntityDmlQueryOperator<T> createQuery() {
        return entityOperator().query();
    }

    /** @return 当前实体的 Lambda 更新命令 */
    public EntityDmlUpdateOperator<T> createUpdate() {
        return entityOperator().update();
    }

    /** @return 当前实体的 Lambda 删除命令 */
    public EntityDmlDeleteOperator<T> createDelete() {
        return entityOperator().delete();
    }

    private EntityDmlOperator<T> entityOperator() {
        return EntityDmlOperator.create(client, client.entityRenderer(), form, entityType);
    }

    public Mono<Long> insert(T entity) { return entityWriter.insert(entity); }
    public Mono<BatchWriteResult> insertBatch(List<T> entities) { return batchOperations.insert(entities); }
    public Mono<BatchWriteResult> upsertBatch(List<T> entities) { return batchOperations.upsert(entities); }
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

    public Mono<Long> delete(ConditionGroup where) { return entityWriter.delete(where); }
    public Mono<Long> delete(T entity, ConditionGroup where) { return entityWriter.delete(entity, where); }

    public Mono<Long> physicalDelete(ConditionGroup where) { return entityWriter.physicalDelete(where); }

    public Flux<T> select(ConditionGroup where) { return readMapper.select(where, null, null); }
    public Mono<PageResult<T>> page(ConditionGroup where, PageQuery page) {
        return readMapper.page(where, page, null, null);
    }

}
