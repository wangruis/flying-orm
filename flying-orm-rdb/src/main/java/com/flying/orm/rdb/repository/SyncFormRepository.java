package com.flying.orm.rdb.repository;

import com.flying.orm.rdb.batch.BatchExecutionEvidence;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.aggregate.AggregateRow;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.lifecycle.ReactiveEntityListener;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.lock.ReadLock;
import com.flying.orm.rdb.operator.SyncEntityDmlDeleteOperator;
import com.flying.orm.rdb.operator.SyncEntityDmlOperator;
import com.flying.orm.rdb.operator.SyncEntityDmlQueryOperator;
import com.flying.orm.rdb.operator.SyncEntityDmlUpdateOperator;
import org.reactivestreams.Publisher;

import java.util.List;
import java.util.Objects;

/**
 * 实体 Repository 的同步入口。
 *
 * <p>同步 Repository 不再包装 {@code ReactiveFormRepository}。它只把实体映射、生命周期和实体专属
 * 的逻辑删除、乐观锁规则组织成表单规格，再交给 {@link SyncFormClient} 执行。SQL 从连接取得到结果返回
 * 全程走原生 JDBC。</p>
 *
 * <p>这个类故意只保留面向使用者的 Repository 方法。查询、单实体写入、批量执行和生命周期顺序分别放在
 * 独立协作者中，避免同步入口随着功能增加重新长成第二个 ORM 内核。</p>
 *
 * <p>同步批量按有界缓冲顺序执行并返回实际 SQL 事实。每行完成生成键回填和辅助 SQL 后执行
 * POST；连接获取、释放与多语句一致性由上层提供和保证。</p>
 *
 * @param <T> 实体类型
 * @author wangr
 * @version v2.0.0
 */
public final class SyncFormRepository<T> {

    private final SyncFormClient client;
    private final DynamicForm form;
    private final DynamicForm boundForm;
    private final Class<T> entityType;
    private final EntityValues<T> entityValues;
    private final ReactiveEntityListener<T> listener;
    private final SyncRepositoryEntityWriter<T> entityWriter;
    private final SyncRepositoryBatchCoordinator<T> batchCoordinator;
    private final SyncRepositoryReadMapper<T> readMapper;

    private SyncFormRepository(SyncFormClient client,
                               DynamicForm form,
                               Class<T> type,
                               EntityValues<T> entityValues,
                               ReactiveEntityListener<T> listener) {
        this.client = Objects.requireNonNull(client, "sync form client must not be null");
        this.boundForm = Objects.requireNonNull(form, "repository form must not be null");
        this.entityType = Objects.requireNonNull(type, "repository type must not be null");
        this.entityValues = Objects.requireNonNull(entityValues, "repository entity values must not be null");
        this.listener = listener;

        EntityMetadata<T> metadata = client.entityModels().metadata(type);
        this.form = RepositoryLogicDeletes.bind(metadata, boundForm);
        RepositoryEntityIdSupport<T> ids = RepositoryEntityIdSupport.create(
                metadata, client.entityModels());
        SyncRepositoryLifecycleSupport<T> lifecycle = new SyncRepositoryLifecycleSupport<>(
                metadata, listener, new SyncRepositoryAwaiter());
        this.entityWriter = new SyncRepositoryEntityWriter<>(
                client, this.form, metadata, this.entityValues, lifecycle, ids);
        this.batchCoordinator = new SyncRepositoryBatchCoordinator<>(
                client, this.form, metadata, this.entityValues, lifecycle, ids);
        this.readMapper = new SyncRepositoryReadMapper<>(client, this.form, type, metadata, lifecycle);
    }

    /** 使用共享的实体元数据和字段策略创建同步 Repository。 */
    public static <T> SyncFormRepository<T> create(SyncFormClient client, DynamicForm form, Class<T> type) {
        SyncFormClient safeClient = Objects.requireNonNull(client, "sync form client must not be null");
        EntityValues<T> entityValues = safeClient.entityModels().entityValues(type);
        return new SyncFormRepository<>(safeClient, form, type, entityValues, null);
    }

    /**
     * 返回带有额外监听器的新 Repository。实例本身不可变，因此可以安全地在并发请求之间复用。
     */
    public SyncFormRepository<T> withListener(ReactiveEntityListener<T> listener) {
        ReactiveEntityListener<T> additional = Objects.requireNonNull(
                listener, "entity lifecycle listener must not be null");
        return new SyncFormRepository<>(client, boundForm, entityType, entityValues,
                this.listener == null ? additional : ReactiveEntityListener.compose(this.listener, additional));
    }

    /** @return 当前实体的同步 Lambda 查询命令 */
    public SyncEntityDmlQueryOperator<T> createQuery() {
        return entityOperator().query();
    }

    /** @return 当前实体的同步 Lambda 更新命令 */
    public SyncEntityDmlUpdateOperator<T> createUpdate() {
        return entityOperator().update();
    }

    /** @return 当前实体的同步 Lambda 删除命令 */
    public SyncEntityDmlDeleteOperator<T> createDelete() {
        return entityOperator().delete();
    }

    private SyncEntityDmlOperator<T> entityOperator() {
        return SyncEntityDmlOperator.create(client, client.entityRenderer(), form, entityType);
    }

    public long insert(T entity) { return entityWriter.insert(entity); }
    public BatchExecutionEvidence insertBatch(List<T> entities) { return batchCoordinator.insert(entities); }
    public BatchExecutionEvidence upsertBatch(List<T> entities) { return batchCoordinator.upsert(entities); }
    public BatchExecutionEvidence insertBatch(Publisher<T> entities, BatchWriteOptions options) {
        return batchCoordinator.insert(entities, options);
    }
    public BatchExecutionEvidence upsertBatch(Publisher<T> entities, BatchWriteOptions options) {
        return batchCoordinator.upsert(entities, options);
    }
    public BatchExecutionEvidence updateBatch(List<T> entities) { return batchCoordinator.update(entities); }
    public BatchExecutionEvidence updateBatch(Publisher<T> entities, BatchWriteOptions options) {
        return batchCoordinator.update(entities, options);
    }
    public BatchExecutionEvidence updateBatch(Publisher<T> entities, DataScope scope, BatchWriteOptions options) {
        return batchCoordinator.update(entities, scope, options);
    }

    public long update(T entity, ConditionGroup where) { return entityWriter.update(entity, conditions(where)); }

    public long delete(ConditionGroup where) { return entityWriter.delete(conditions(where)); }
    public long delete(T entity, ConditionGroup where) { return entityWriter.delete(entity, conditions(where)); }

    public long physicalDelete(ConditionGroup where) { return entityWriter.physicalDelete(conditions(where)); }

    public List<T> select(ConditionGroup where) { return readMapper.select(conditions(where), null, null); }
    /** 在调用方 JDBC 事务内按受控锁读取当前实体；事务生命周期仍归调用方。 */
    public List<T> lockingRead(ConditionGroup where, ReadLock lock) {
        return readMapper.lockingRead(conditions(where), lock);
    }
    /** 在当前 Repository 绑定的表单上执行类型化聚合。 */
    public List<AggregateRow> aggregate(AggregateSpec spec) {
        return client.aggregate(requireRepositoryAggregate(spec));
    }
    public PageResult<T> page(ConditionGroup where, PageQuery page) { return readMapper.page(conditions(where), page, null, null); }

    private ConditionGroup conditions(ConditionGroup where) {
        return entityValues.normalizeCondition(where, client.entityRenderer().terms());
    }

    private AggregateSpec requireRepositoryAggregate(AggregateSpec spec) {
        AggregateSpec safeSpec = Objects.requireNonNull(spec, "aggregate spec must not be null");
        if (safeSpec.query().form() != boundForm) {
            throw new IllegalArgumentException("aggregate spec must use the repository form");
        }
        return RepositoryLogicDeletes.aggregate(safeSpec, form);
    }
}
