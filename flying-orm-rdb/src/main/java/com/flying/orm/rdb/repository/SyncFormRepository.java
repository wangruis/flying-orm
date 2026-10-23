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
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.lifecycle.ReactiveEntityListener;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.EntityQueryDefaults;
import com.flying.orm.rdb.lock.ReadLock;
import com.flying.orm.rdb.operator.SyncEntityDmlDeleteOperator;
import com.flying.orm.rdb.operator.SyncEntityDmlOperator;
import com.flying.orm.rdb.operator.SyncEntityDmlQueryOperator;
import com.flying.orm.rdb.operator.SyncEntityDmlUpdateOperator;
import org.reactivestreams.Publisher;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.UnaryOperator;

/**
 * 实体 Repository 的同步入口。
 *
 * <p>同步 Repository 不再包装 {@code ReactiveFormRepository}。它只把实体映射、生命周期和实体专属
 * 的逻辑删除、乐观锁规则组织成表单规格，再交给 {@link SyncFormClient} 执行。SQL 从连接取得到结果返回
 * 全程走原生 JDBC。</p>
 *
 * <p>查询和单实体写入直接使用共享规格与表单客户端；批量协调、主键回填及生命周期各自复用已有实现，
 * 不重复保存一套单实体写入状态，也不重新实现 SQL 或字段映射。</p>
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
    private final RepositoryEntityIdSupport<T> ids;
    private final SyncEntityDmlOperator<T> entityOperator;
    private final SyncRepositoryBatchCoordinator<T> batchCoordinator;
    private final EntityMetadata<T> metadata;
    private final SyncRepositoryLifecycleSupport<T> lifecycle;

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

        this.metadata = client.entityModels().metadata(type);
        this.form = RepositoryLogicDeletes.bind(metadata, boundForm);
        this.entityOperator = SyncEntityDmlOperator.create(client, client.entityRenderer(), this.form, entityType);
        this.ids = RepositoryEntityIdSupport.create(
                metadata, client.entityModels());
        this.lifecycle = new SyncRepositoryLifecycleSupport<>(
                metadata, listener);
        this.batchCoordinator = new SyncRepositoryBatchCoordinator<>(
                client, this.form, metadata, this.entityValues, lifecycle, ids);
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

    /** 派生查询准备配置；覆盖 QuerySpec 查询终端，不处理聚合或写入，沿用客户端的同步准备契约。 */
    public SyncFormRepository<T> withQueryPreparation(UnaryOperator<QuerySpec> preparation) {
        return new SyncFormRepository<>(client.withQueryPreparation(preparation), boundForm, entityType,
                entityValues, listener);
    }

    /** @return 当前实体的同步 Lambda 查询命令 */
    public SyncEntityDmlQueryOperator<T> createQuery() {
        SyncEntityDmlQueryOperator<T> query = entityOperator.query();
        return listener == null ? query
                : query.onLoad(entity -> lifecycle.fire(EntityLifecyclePhase.POST_LOAD, entity, null));
    }

    /** @return 当前实体的同步 Lambda 更新命令 */
    public SyncEntityDmlUpdateOperator<T> createUpdate() {
        return entityOperator.update();
    }

    /** @return 当前实体的同步 Lambda 删除命令 */
    public SyncEntityDmlDeleteOperator<T> createDelete() {
        return entityOperator.delete();
    }

    public long insert(T entity) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        if (!lifecycle.hasWork()) {
            return executeInsert(safeEntity);
        }
        return lifecycle.persist(safeEntity, () -> executeInsert(safeEntity));
    }
    /**
     * 按完整主键插入或更新单个实体，支持 NOT_NULL/NOT_EMPTY 字段策略。
     * 沿用 upsertBatch 的 PRE/POST_PERSIST 和 SQL 执行事实，不推断插入/更新分支或事务结果。
     * 数据库生成主键为空时插入并回填，非空时按主键 UPSERT（受数据库显式 identity 写入规则约束）。
     * 不执行 @Version 比较，版本更新应使用 update。
     */
    public BatchExecutionEvidence upsert(T entity) {
        return batchCoordinator.writeSingle(entity, RepositoryBatchKind.UPSERT, null);
    }
    /**
     * 按完整主键插入，冲突时保留已有行；内置 PostgreSQL、Oracle、SQL Server、H2 支持。
     * 主键要求及 PRE/POST_PERSIST 沿用 upsert；执行完成不代表一定新增，影响行数以返回事实为准。
     */
    public BatchExecutionEvidence insertIfAbsent(T entity) {
        return batchCoordinator.writeSingle(entity, RepositoryBatchKind.INSERT_IF_ABSENT, null);
    }
    /** 批量插入并忽略主键冲突，仅使用 INSERT 字段策略；不执行冲突更新。 */
    public BatchExecutionEvidence insertBatchIfAbsent(List<T> entities) {
        return batchCoordinator.write(entities, RepositoryBatchKind.INSERT_IF_ABSENT, null);
    }
    /** 按指定批量选项消费实体流并忽略主键冲突，语义同 {@link #insertIfAbsent(Object)}。 */
    public BatchExecutionEvidence insertBatchIfAbsent(Publisher<T> entities, BatchWriteOptions options) {
        return batchCoordinator.write(entities, options, null, RepositoryBatchKind.INSERT_IF_ABSENT, null);
    }
    public BatchExecutionEvidence insertBatch(List<T> entities) {
        return batchCoordinator.write(entities, RepositoryBatchKind.INSERT, null);
    }
    public BatchExecutionEvidence upsertBatch(List<T> entities) {
        return batchCoordinator.write(entities, RepositoryBatchKind.UPSERT, null);
    }
    public BatchExecutionEvidence insertBatch(Publisher<T> entities, BatchWriteOptions options) {
        return batchCoordinator.write(entities, options, null, RepositoryBatchKind.INSERT, null);
    }
    public BatchExecutionEvidence upsertBatch(Publisher<T> entities, BatchWriteOptions options) {
        return batchCoordinator.write(entities, options, null, RepositoryBatchKind.UPSERT, null);
    }
    public BatchExecutionEvidence updateBatch(List<T> entities) {
        return batchCoordinator.write(entities, RepositoryBatchKind.UPDATE, null);
    }
    public BatchExecutionEvidence updateBatch(Publisher<T> entities, BatchWriteOptions options) {
        return batchCoordinator.write(entities, options, null, RepositoryBatchKind.UPDATE, null);
    }
    public BatchExecutionEvidence updateBatch(Publisher<T> entities, DataScope scope, BatchWriteOptions options) {
        return batchCoordinator.write(entities, options, scope, RepositoryBatchKind.UPDATE, null);
    }

    /**
     * 按已有业务唯一键保存，参数使用实体属性名或列名，空参数仍按主键匹配。
     * 显式业务键允许数据库生成主键，但其值必须为空；不回填或校正主键，不推断执行分支，不比较版本。
     * 应用侧主键仍按声明策略准备。约束、字段策略和 Scope 沿用批量 UPSERT。
     */
    public BatchExecutionEvidence upsert(T entity, String... conflictProperties) {
        return batchCoordinator.writeSingle(entity, RepositoryBatchKind.UPSERT, conflictProperties);
    }
    /** 按业务唯一键保存列表；生成键与执行结果语义同 {@link #upsert(Object, String...)}。 */
    public BatchExecutionEvidence upsertBatch(List<T> entities, String... conflictProperties) {
        return batchCoordinator.write(entities, RepositoryBatchKind.UPSERT, conflictProperties);
    }
    /** 按业务唯一键消费实体流；不收集整流，使用调用方指定的批量选项。 */
    public BatchExecutionEvidence upsertBatch(Publisher<T> entities, BatchWriteOptions options,
                                                String... conflictProperties) {
        return batchCoordinator.write(entities, options, null, RepositoryBatchKind.UPSERT, conflictProperties);
    }

    public long update(T entity, ConditionGroup where) { return updateEntity(entity, conditions(where)); }

    /** 按实体全部主键更新；主键不进入 SET，声明 @Version 时自动比较并递增版本。 */
    public long update(T entity) { return updateEntity(entity, null); }

    /** 按实体全部主键删除，保留实体生命周期、逻辑删除和声明的版本锁。 */
    public long delete(T entity) { return deleteEntity(entity, null); }

    /**
     * 按完整主键查询，未找到返回 null，多行时报错。单主键传值；复合键传属性名或列名到值的 Map。
     * Map 必须恰好包含全部主键，结果沿用实体映射、Scope 和 POST_LOAD。
     */
    public T findById(Object key) {
        T entity = client.selectOne(query(RepositoryKeys.fromKey(metadata, key)), entityType);
        if (entity != null && listener != null) {
            lifecycle.fire(EntityLifecyclePhase.POST_LOAD, entity, null);
        }
        return entity;
    }

    /** 按完整主键删除并沿用逻辑删除；无实体输入，不触发实体事件或推测预期版本。 */
    public long deleteById(Object key) { return delete(RepositoryKeys.fromKey(metadata, key)); }

    /**
     * 一次查询多个完整主键；每项格式同 {@link #findById(Object)}，结果不保证输入键顺序。
     * 空集合直接返回空列表，不要求模型声明主键；不隐式分批。
     * 数据库参数数量限制由调用方按需组织输入。
     */
    public List<T> findByIds(Collection<?> keys) {
        Objects.requireNonNull(keys, "repository keys must not be null");
        return keys.isEmpty() ? List.of() : select(RepositoryKeys.fromKeys(metadata, keys));
    }

    /**
     * 一次删除多个完整主键，沿用逻辑删除和 Scope；空集合返回 0。
     * 无实体输入，不触发实体事件或推测预期版本；不隐式分批。
     */
    public long deleteByIds(Collection<?> keys) {
        Objects.requireNonNull(keys, "repository keys must not be null");
        return keys.isEmpty() ? 0L : delete(RepositoryKeys.fromKeys(metadata, keys));
    }

    public long delete(ConditionGroup where) { return client.delete(WriteSpec.delete(form, conditions(where))); }
    public long delete(T entity, ConditionGroup where) { return deleteEntity(entity, conditions(where)); }

    public long physicalDelete(ConditionGroup where) {
        return client.physicalDelete(WriteSpec.delete(form, conditions(where)));
    }

    public List<T> select(ConditionGroup where) {
        return lifecycle.postLoad(client.select(query(where), entityType));
    }
    /** 在调用方 JDBC 事务内按受控锁读取当前实体；事务生命周期仍归调用方。 */
    public List<T> lockingRead(ConditionGroup where, ReadLock lock) {
        return lifecycle.postLoad(client.lockingRead(query(where).withLock(Objects.requireNonNull(lock, "repository read lock must not be null")), entityType));
    }
    /** 在当前 Repository 绑定的表单上执行类型化聚合。 */
    public List<AggregateRow> aggregate(AggregateSpec spec) {
        return client.aggregate(requireRepositoryAggregate(spec));
    }
    public PageResult<T> page(ConditionGroup where, PageQuery page) {
        QuerySpec query = query(where);
        if (!page.sorts().isEmpty()) {
            query = query.withSorts(page.sorts());
        }
        return lifecycle.postLoad(client.page(query, page, entityType));
    }

    private long updateEntity(T entity, ConditionGroup where) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return lifecycle.update(safeEntity, () -> client.update(
                RepositoryUpdateSupport.updateSpec(form, metadata, entityValues, safeEntity, where)));
    }

    private long deleteEntity(T entity, ConditionGroup where) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return lifecycle.remove(safeEntity, () -> client.delete(
                RepositoryUpdateSupport.deleteSpec(form, metadata, entityValues, safeEntity, where)));
    }

    private long executeInsert(T entity) {
        ids.prepare(entity);
        WriteSpec spec = WriteSpec.insertOwned(form, entityValues.readForInsert(entity));
        if (!ids.databaseGenerated()) {
            return client.insert(spec);
        }
        final SqlWriteResult result;
        try {
            result = client.insertReturningKeys(spec);
        } catch (GeneratedKeyReadException failure) {
            throw RepositoryFailureSupport.generatedKeyReadFailure(failure);
        }
        return ids.assignGeneratedKey(entity, result);
    }

    private QuerySpec query(ConditionGroup where) {
        return EntityQueryDefaults.query(form, metadata, conditions(where), client.defaultFieldScope());
    }

    private ConditionGroup conditions(ConditionGroup where) {
        return entityValues.normalizeCondition(Objects.requireNonNull(where, "where condition must not be null"),
                client.entityRenderer().terms());
    }

    private AggregateSpec requireRepositoryAggregate(AggregateSpec spec) {
        AggregateSpec safeSpec = Objects.requireNonNull(spec, "aggregate spec must not be null");
        if (safeSpec.query().form() != boundForm) {
            throw new IllegalArgumentException("aggregate spec must use the repository form");
        }
        return RepositoryLogicDeletes.aggregate(safeSpec, form);
    }
}
