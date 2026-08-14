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
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.lifecycle.ReactiveEntityListener;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.operator.SyncEntityDmlDeleteOperator;
import com.flying.orm.rdb.operator.SyncEntityDmlQueryOperator;
import com.flying.orm.rdb.operator.SyncEntityDmlUpdateOperator;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * 实体 Repository 的同步入口。
 *
 * <p>同步 Repository 不再包装 {@code ReactiveFormRepository}。它只把实体映射、生命周期和实体专属
 * 的逻辑删除、乐观锁规则组织成表单规格，再交给 {@link SyncFormClient} 执行。SQL 从连接取得到结果返回
 * 全程走原生 JDBC。</p>
 *
 * <p>这个类故意只保留面向使用者的 Repository 方法。查询、单实体写入、批量回执和生命周期顺序分别放在
 * 独立协作者中，避免同步入口随着功能增加重新长成第二个 ORM 内核。</p>
 *
 * @param <T> 实体类型
 * @author wangr
 * @version v2.0.0
 */
public final class SyncFormRepository<T> {

    private final SyncFormClient client;
    private final DynamicForm form;
    private final Class<T> entityType;
    private final Function<T, Map<String, Object>> values;
    private final Function<T, Map<String, Object>> insertValues;
    private final Function<T, Map<String, Object>> updateValues;
    private final Function<T, Map<String, Object>> upsertValues;
    private final SyncRepositoryEntityWriter<T> entityWriter;
    private final SyncRepositoryBatchCoordinator<T> batchCoordinator;
    private final SyncRepositoryReadMapper<T> readMapper;

    private SyncFormRepository(SyncFormClient client,
                               DynamicForm form,
                               Class<T> type,
                               Function<T, Map<String, Object>> values,
                               Function<T, Map<String, Object>> insertValues,
                               Function<T, Map<String, Object>> updateValues,
                               Function<T, Map<String, Object>> upsertValues,
                               ReactiveEntityListener<T> listener) {
        this.client = Objects.requireNonNull(client, "sync form client must not be null");
        this.form = Objects.requireNonNull(form, "repository form must not be null");
        this.entityType = Objects.requireNonNull(type, "repository type must not be null");
        this.values = Objects.requireNonNull(values, "repository entity values must not be null");
        this.insertValues = Objects.requireNonNull(insertValues, "repository insert values must not be null");
        this.updateValues = Objects.requireNonNull(updateValues, "repository update values must not be null");
        this.upsertValues = Objects.requireNonNull(upsertValues, "repository upsert values must not be null");

        EntityMetadata<T> metadata = client.entityModels().metadata(type);
        RepositoryEntityIdSupport<T> ids = RepositoryEntityIdSupport.create(
                metadata, client.entityModels().idGenerator());
        SyncRepositoryLifecycleSupport<T> lifecycle = new SyncRepositoryLifecycleSupport<>(
                metadata, listener, new SyncRepositoryAwaiter(client.timeout()));
        this.entityWriter = new SyncRepositoryEntityWriter<>(client, form, metadata, this.values,
                                                              this.insertValues, this.updateValues, lifecycle);
        this.batchCoordinator = new SyncRepositoryBatchCoordinator<>(client, form, metadata, this.values,
                                                                      this.insertValues, this.updateValues,
                                                                      this.upsertValues, lifecycle, ids);
        this.readMapper = new SyncRepositoryReadMapper<>(client, form, type, metadata, lifecycle);
    }

    /** 使用共享的实体元数据和字段策略创建同步 Repository。 */
    public static <T> SyncFormRepository<T> create(SyncFormClient client, DynamicForm form, Class<T> type) {
        SyncFormClient safeClient = Objects.requireNonNull(client, "sync form client must not be null");
        EntityValues<T> entityValues = safeClient.entityModels().entityValues(type);
        return new SyncFormRepository<>(safeClient, form, type,
                                        entityValues::read,
                                        entityValues::readForInsert,
                                        entityValues::readForUpdate,
                                        entityValues::readForUpsert,
                                        null);
    }

    /**
     * 返回带有额外监听器的新 Repository。实例本身不可变，因此可以安全地在并发请求之间复用。
     */
    public SyncFormRepository<T> withListener(ReactiveEntityListener<T> listener) {
        return new SyncFormRepository<>(client, form, entityType, values, insertValues, updateValues, upsertValues,
                                        Objects.requireNonNull(listener, "entity lifecycle listener must not be null"));
    }

    /** @return 当前实体的同步 Lambda 查询命令 */
    public SyncEntityDmlQueryOperator<T> createQuery() {
        return client.entity(entityType).query();
    }

    /** @return 当前实体的同步 Lambda 更新命令 */
    public SyncEntityDmlUpdateOperator<T> createUpdate() {
        return client.entity(entityType).update();
    }

    /** @return 当前实体的同步 Lambda 删除命令 */
    public SyncEntityDmlDeleteOperator<T> createDelete() {
        return client.entity(entityType).delete();
    }

    static <T> SyncFormRepository<T> create(SyncFormClient client,
                                             DynamicForm form,
                                             Class<T> type,
                                             Function<T, Map<String, Object>> values) {
        Function<T, Map<String, Object>> safeValues = Objects.requireNonNull(
                values, "repository entity values must not be null");
        return new SyncFormRepository<>(client, form, type, safeValues, safeValues, safeValues, safeValues, null);
    }

    public long insert(T entity) { return entityWriter.insert(entity); }
    public BatchWriteResult insertBatch(List<T> entities) { return batchCoordinator.insert(entities); }
    public BatchWriteResult upsertBatch(List<T> entities) { return batchCoordinator.upsert(entities); }
    BatchWriteResult insertBatch(Publisher<T> entities) { return batchCoordinator.insert(entities); }
    BatchWriteResult upsertBatch(Publisher<T> entities) { return batchCoordinator.upsert(entities); }
    public BatchWriteResult insertBatch(Publisher<T> entities, BatchWriteOptions options) {
        return batchCoordinator.insert(entities, options);
    }
    public BatchWriteResult upsertBatch(Publisher<T> entities, BatchWriteOptions options) {
        return batchCoordinator.upsert(entities, options);
    }
    public List<BatchChunkResult> insertBatchChunks(Publisher<T> entities, BatchWriteOptions options) {
        return batchCoordinator.insertChunks(entities, options);
    }
    public List<BatchChunkResult> upsertBatchChunks(Publisher<T> entities, BatchWriteOptions options) {
        return batchCoordinator.upsertChunks(entities, options);
    }
    public BatchWriteResult updateBatch(List<T> entities) { return batchCoordinator.update(entities); }
    BatchWriteResult updateBatch(Publisher<T> entities) { return batchCoordinator.update(entities); }
    public BatchWriteResult updateBatch(Publisher<T> entities, BatchWriteOptions options) {
        return batchCoordinator.update(entities, options);
    }
    public BatchWriteResult updateBatch(Publisher<T> entities, DataScope scope, BatchWriteOptions options) {
        return batchCoordinator.update(entities, scope, options);
    }
    public List<BatchChunkResult> updateBatchChunks(Publisher<T> entities, BatchWriteOptions options) {
        return batchCoordinator.updateChunks(entities, options);
    }
    public List<BatchChunkResult> updateBatchChunks(Publisher<T> entities,
                                                     DataScope scope,
                                                     BatchWriteOptions options) {
        return batchCoordinator.updateChunks(entities, scope, options);
    }

    public long update(T entity, ConditionGroup where) { return entityWriter.update(entity, where); }
    long update(T entity, ConditionGroup where, SqlExecutionOptions options) {
        return entityWriter.update(entity, where, options);
    }
    long update(T entity, ConditionGroup where, DataScope scope) { return entityWriter.update(entity, where, scope); }
    long update(T entity, ConditionGroup where, DataScope scope, SqlExecutionOptions options) {
        return entityWriter.update(entity, where, scope, options);
    }
    long update(T entity, ConditionGroup where, OptimisticLockOptions lock) {
        return entityWriter.updateWithLock(entity, where, lock);
    }
    long update(T entity, ConditionGroup where, OptimisticLockOptions lock, SqlExecutionOptions options) {
        return entityWriter.updateWithLock(entity, where, lock, options);
    }
    long update(T entity, ConditionGroup where, DataScope scope, OptimisticLockOptions lock) {
        return entityWriter.updateWithLock(entity, where, lock, scope);
    }
    long update(T entity, ConditionGroup where, DataScope scope,
                OptimisticLockOptions lock, SqlExecutionOptions options) {
        return entityWriter.updateWithLock(entity, where, lock, scope, options);
    }

    public long delete(ConditionGroup where) { return entityWriter.delete(where); }
    public long delete(T entity, ConditionGroup where) { return entityWriter.delete(entity, where); }
    long delete(ConditionGroup where, SqlExecutionOptions options) { return entityWriter.delete(where, options); }
    long delete(ConditionGroup where, DataScope scope) { return entityWriter.delete(where, scope); }
    long delete(ConditionGroup where, DataScope scope, SqlExecutionOptions options) {
        return entityWriter.delete(where, scope, options);
    }
    long delete(T entity, ConditionGroup where, SqlExecutionOptions options) {
        return entityWriter.delete(entity, where, options);
    }
    long delete(T entity, ConditionGroup where, DataScope scope) { return entityWriter.delete(entity, where, scope); }
    long delete(T entity, ConditionGroup where, DataScope scope, SqlExecutionOptions options) {
        return entityWriter.delete(entity, where, scope, options);
    }
    long delete(ConditionGroup where, OptimisticLockOptions lock) { return entityWriter.delete(where, lock); }
    long delete(ConditionGroup where, OptimisticLockOptions lock, SqlExecutionOptions options) {
        return entityWriter.delete(where, lock, options);
    }
    long delete(ConditionGroup where, DataScope scope, OptimisticLockOptions lock) {
        return entityWriter.delete(where, scope, lock);
    }
    long delete(ConditionGroup where, DataScope scope, OptimisticLockOptions lock, SqlExecutionOptions options) {
        return entityWriter.delete(where, scope, lock, options);
    }

    public long physicalDelete(ConditionGroup where) { return entityWriter.physicalDelete(where); }
    long physicalDelete(ConditionGroup where, SqlExecutionOptions options) {
        return entityWriter.physicalDelete(where, options);
    }
    long physicalDelete(ConditionGroup where, OptimisticLockOptions lock) {
        return entityWriter.physicalDelete(where, lock);
    }
    long physicalDelete(ConditionGroup where, OptimisticLockOptions lock, SqlExecutionOptions options) {
        return entityWriter.physicalDelete(where, lock, options);
    }
    long physicalDelete(ConditionGroup where, DataScope scope) { return entityWriter.physicalDelete(where, scope); }
    long physicalDelete(ConditionGroup where, DataScope scope, SqlExecutionOptions options) {
        return entityWriter.physicalDelete(where, scope, options);
    }
    long physicalDelete(ConditionGroup where, DataScope scope, OptimisticLockOptions lock) {
        return entityWriter.physicalDelete(where, scope, lock);
    }
    long physicalDelete(ConditionGroup where, DataScope scope,
                        OptimisticLockOptions lock, SqlExecutionOptions options) {
        return entityWriter.physicalDelete(where, scope, lock, options);
    }

    public List<T> select(ConditionGroup where) { return readMapper.select(where, null, null); }
    List<T> select(ConditionGroup where, SqlExecutionOptions options) { return readMapper.select(where, null, options); }
    List<T> select(ConditionGroup where, DataScope scope) { return readMapper.select(where, scope, null); }
    List<T> select(ConditionGroup where, DataScope scope, SqlExecutionOptions options) {
        return readMapper.select(where, scope, options);
    }
    public PageResult<T> page(ConditionGroup where, PageQuery page) { return readMapper.page(where, page, null, null); }
    PageResult<T> page(ConditionGroup where, PageQuery page, SqlExecutionOptions options) {
        return readMapper.page(where, page, null, options);
    }
    PageResult<T> page(ConditionGroup where, PageQuery page, DataScope scope) {
        return readMapper.page(where, page, scope, null);
    }
    PageResult<T> page(ConditionGroup where, PageQuery page, DataScope scope, SqlExecutionOptions options) {
        return readMapper.page(where, page, scope, options);
    }
}
