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
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.lifecycle.ReactiveEntityListener;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.EntityQueryDefaults;
import com.flying.orm.rdb.lock.ReadLock;
import com.flying.orm.rdb.operator.EntityDmlDeleteOperator;
import com.flying.orm.rdb.operator.EntityDmlOperator;
import com.flying.orm.rdb.operator.EntityDmlQueryOperator;
import com.flying.orm.rdb.operator.EntityDmlUpdateOperator;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * 实体侧的轻量响应式 Repository。
 *
 * <p>这个类是稳定的实体入口，不重新实现 ORM 内核。实体映射交给客户端的元数据模型，SQL、Scope、
 * 逻辑删除、乐观锁、批量 SQL 执行事实、执行保护和异常分类继续统一走 {@link ReactiveFormClient}。</p>
 *
 * <p>查询和单实体写入直接使用共享规格与表单客户端；批量协调、主键回填及生命周期各自复用已有实现。
 * 带 Scope、乐观锁和执行选项的组合由实体操作符表达，不重复实现 SQL 或字段映射。</p>
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
    private final DynamicForm boundForm;
    private final Class<T> entityType;
    private final EntityValues<T> entityValues;
    private final ReactiveEntityListener<T> listener;
    private final RepositoryEntityIdSupport<T> ids;
    private final EntityDmlOperator<T> entityOperator;
    private final ReactiveRepositoryBatchCoordinator<T> batchCoordinator;
    private final EntityMetadata<T> metadata;
    private final EntityLifecycleDispatcher<T> lifecycle;

    private ReactiveFormRepository(ReactiveFormClient client,
                                   DynamicForm form,
                                   Class<T> type,
                                   EntityValues<T> entityValues,
                                   ReactiveEntityListener<T> listener) {
        this.client = Objects.requireNonNull(client, "reactive form client must not be null");
        this.boundForm = Objects.requireNonNull(form, "repository form must not be null");
        this.entityType = Objects.requireNonNull(type, "repository type must not be null");
        this.entityValues = Objects.requireNonNull(entityValues, "repository entity values must not be null");
        this.listener = listener;
        this.metadata = client.entityModels().metadata(entityType);
        this.form = RepositoryLogicDeletes.bind(metadata, boundForm);
        this.entityOperator = EntityDmlOperator.create(client, client.entityRenderer(), this.form, entityType);
        this.ids = RepositoryEntityIdSupport.create(
                metadata, client.entityModels());
        this.lifecycle =
                new EntityLifecycleDispatcher<>(metadata, listener);
        this.batchCoordinator = new ReactiveRepositoryBatchCoordinator<>(client, this.form, metadata,
                this.entityValues, lifecycle, ids);
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
        ReactiveEntityListener<T> additional = Objects.requireNonNull(
                listener, "entity lifecycle listener must not be null");
        return new ReactiveFormRepository<>(client, boundForm, entityType,
                entityValues,
                this.listener == null ? additional : ReactiveEntityListener.compose(this.listener, additional));
    }

    /** 派生查询准备配置；覆盖 QuerySpec 查询终端，不处理聚合或写入，沿用客户端的异步准备契约。 */
    public ReactiveFormRepository<T> withQueryPreparation(Function<QuerySpec, ? extends Publisher<QuerySpec>> preparation) {
        return new ReactiveFormRepository<>(client.withQueryPreparation(preparation), boundForm, entityType,
                entityValues, listener);
    }

    /** @return 当前实体的 Lambda 查询命令 */
    public EntityDmlQueryOperator<T> createQuery() {
        EntityDmlQueryOperator<T> query = entityOperator.query();
        return listener == null ? query
                : query.onLoad(entity -> lifecycle.fire(EntityLifecyclePhase.POST_LOAD, entity, null));
    }

    /** @return 当前实体的 Lambda 更新命令 */
    public EntityDmlUpdateOperator<T> createUpdate() {
        return entityOperator.update();
    }

    /** @return 当前实体的 Lambda 删除命令 */
    public EntityDmlDeleteOperator<T> createDelete() {
        return entityOperator.delete();
    }

    public Mono<Long> insert(T entity) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return lifecycle.persist(safeEntity, () -> executeInsert(safeEntity));
    }
    /**
     * 按完整主键插入或更新单个实体，支持 NOT_NULL/NOT_EMPTY 字段策略。
     * 沿用 upsertBatch 的 PRE/POST_PERSIST 和 SQL 执行事实，不推断插入/更新分支或事务结果。
     * 数据库生成主键为空时插入并回填，非空时按主键 UPSERT（受数据库显式 identity 写入规则约束）。
     * 不执行 @Version 比较，版本更新应使用 update。
     */
    public Mono<BatchExecutionEvidence> upsert(T entity) {
        return batchCoordinator.writeSingle(entity, RepositoryBatchKind.UPSERT, null);
    }
    /**
     * 按完整主键插入，冲突时保留已有行；内置 PostgreSQL、Oracle、SQL Server、H2 支持。
     * 主键要求及 PRE/POST_PERSIST 沿用 upsert；执行完成不代表一定新增，影响行数以返回事实为准。
     */
    public Mono<BatchExecutionEvidence> insertIfAbsent(T entity) {
        return batchCoordinator.writeSingle(entity, RepositoryBatchKind.INSERT_IF_ABSENT, null);
    }
    /** 批量插入并忽略主键冲突，仅使用 INSERT 字段策略；不执行冲突更新。 */
    public Mono<BatchExecutionEvidence> insertBatchIfAbsent(List<T> entities) {
        return batchCoordinator.write(entities, RepositoryBatchKind.INSERT_IF_ABSENT, null);
    }
    /** 使用默认批量选项按背压消费实体流并忽略主键冲突。 */
    public Mono<BatchExecutionEvidence> insertBatchIfAbsent(Publisher<T> entities) {
        return insertBatchIfAbsent(entities, client.defaultBatchWriteOptions());
    }
    /** 按指定批量选项消费实体流并忽略主键冲突，语义同 {@link #insertIfAbsent(Object)}。 */
    public Mono<BatchExecutionEvidence> insertBatchIfAbsent(Publisher<T> entities, BatchWriteOptions options) {
        return batchCoordinator.write(entities, options, null, RepositoryBatchKind.INSERT_IF_ABSENT, null);
    }
    public Mono<BatchExecutionEvidence> insertBatch(List<T> entities) {
        return batchCoordinator.write(entities, RepositoryBatchKind.INSERT, null);
    }
    public Mono<BatchExecutionEvidence> upsertBatch(List<T> entities) {
        return batchCoordinator.write(entities, RepositoryBatchKind.UPSERT, null);
    }
    /** 使用客户端默认批量选项，按背压消费实体流。 */
    public Mono<BatchExecutionEvidence> insertBatch(Publisher<T> entities) {
        return insertBatch(entities, client.defaultBatchWriteOptions());
    }
    /** 使用客户端默认批量选项，按背压消费实体流。 */
    public Mono<BatchExecutionEvidence> upsertBatch(Publisher<T> entities) {
        return upsertBatch(entities, client.defaultBatchWriteOptions());
    }
    public Mono<BatchExecutionEvidence> insertBatch(Publisher<T> entities, BatchWriteOptions options) {
        return batchCoordinator.write(entities, options, null, RepositoryBatchKind.INSERT, null);
    }
    public Mono<BatchExecutionEvidence> upsertBatch(Publisher<T> entities, BatchWriteOptions options) {
        return batchCoordinator.write(entities, options, null, RepositoryBatchKind.UPSERT, null);
    }
    public Mono<BatchExecutionEvidence> updateBatch(List<T> entities) {
        return batchCoordinator.write(entities, RepositoryBatchKind.UPDATE, null);
    }
    /** 使用客户端默认批量选项，按背压消费实体流并保留乐观锁语义。 */
    public Mono<BatchExecutionEvidence> updateBatch(Publisher<T> entities) {
        return updateBatch(entities, client.defaultBatchWriteOptions());
    }
    public Mono<BatchExecutionEvidence> updateBatch(Publisher<T> entities, BatchWriteOptions options) {
        return batchCoordinator.write(entities, options, DataScope.none(), RepositoryBatchKind.UPDATE, null);
    }
    public Mono<BatchExecutionEvidence> updateBatch(Publisher<T> entities, DataScope scope, BatchWriteOptions options) {
        return batchCoordinator.write(entities, options, scope, RepositoryBatchKind.UPDATE, null);
    }

    /**
     * 按已有业务唯一键保存，参数使用实体属性名或列名，空参数仍按主键匹配。
     * 显式业务键允许数据库生成主键，但其值必须为空；不回填或校正主键，不推断执行分支，不比较版本。
     * 应用侧主键仍按声明策略准备。约束、字段策略和 Scope 沿用批量 UPSERT。
     */
    public Mono<BatchExecutionEvidence> upsert(T entity, String... conflictProperties) {
        return batchCoordinator.writeSingle(entity, RepositoryBatchKind.UPSERT, conflictProperties);
    }
    /** 按业务唯一键保存列表；生成键与执行结果语义同 {@link #upsert(Object, String...)}。 */
    public Mono<BatchExecutionEvidence> upsertBatch(List<T> entities, String... conflictProperties) {
        return batchCoordinator.write(entities, RepositoryBatchKind.UPSERT, conflictProperties);
    }
    /** 按业务唯一键消费实体流；不收集整流，使用调用方指定的批量选项。 */
    public Mono<BatchExecutionEvidence> upsertBatch(Publisher<T> entities, BatchWriteOptions options,
                                                String... conflictProperties) {
        return batchCoordinator.write(entities, options, null, RepositoryBatchKind.UPSERT, conflictProperties);
    }

    public Mono<Long> update(T entity, ConditionGroup where) { return updateEntity(entity, conditions(where)); }

    /** 按实体全部主键更新；主键不进入 SET，声明 @Version 时自动比较并递增版本。 */
    public Mono<Long> update(T entity) { return updateEntity(entity, null); }

    /** 按实体全部主键删除，保留实体生命周期、逻辑删除和声明的版本锁。 */
    public Mono<Long> delete(T entity) { return deleteEntity(entity, null); }

    /**
     * 按完整主键查询，未找到时为空，多行时报错。单主键传值；复合键传属性名或列名到值的 Map。
     * Map 必须恰好包含全部主键，条件在调用时快照，SQL 和 POST_LOAD 在订阅时执行。
     */
    public Mono<T> findById(Object key) {
        Mono<T> result = client.select(query(RepositoryKeys.fromKey(metadata, key)), entityType).singleOrEmpty();
        return listener == null ? result
                : result.delayUntil(entity -> lifecycle.fire(EntityLifecyclePhase.POST_LOAD, entity, null));
    }

    /** 按完整主键删除并沿用逻辑删除；无实体输入，不触发实体事件或推测预期版本。 */
    public Mono<Long> deleteById(Object key) { return delete(RepositoryKeys.fromKey(metadata, key)); }

    /**
     * 一次查询多个完整主键；每项格式同 {@link #findById(Object)}，结果不保证输入键顺序。
     * 调用时快照键值，订阅时执行；空集合直接返回空流，不要求模型声明主键。
     * 不隐式分批，数据库参数数量限制由调用方按需组织输入。
     */
    public Flux<T> findByIds(Collection<?> keys) {
        Objects.requireNonNull(keys, "repository keys must not be null");
        return keys.isEmpty() ? Flux.empty() : select(RepositoryKeys.fromKeys(metadata, keys));
    }

    /**
     * 一次删除多个完整主键，沿用逻辑删除和 Scope；调用时快照键值，空集合返回 0。
     * 无实体输入，不触发实体事件或推测预期版本；不隐式分批。
     */
    public Mono<Long> deleteByIds(Collection<?> keys) {
        Objects.requireNonNull(keys, "repository keys must not be null");
        return keys.isEmpty() ? Mono.just(0L) : delete(RepositoryKeys.fromKeys(metadata, keys));
    }

    public Mono<Long> delete(ConditionGroup where) { return client.delete(WriteSpec.delete(form, conditions(where))); }
    public Mono<Long> delete(T entity, ConditionGroup where) { return deleteEntity(entity, conditions(where)); }

    public Mono<Long> physicalDelete(ConditionGroup where) {
        return client.physicalDelete(WriteSpec.delete(form, conditions(where)));
    }

    public Flux<T> select(ConditionGroup where) {
        return lifecycle.postLoad(client.select(query(where), entityType));
    }
    /** 在订阅时按受控锁读取当前实体；调用方决定事务范围。 */
    public Flux<T> lockingRead(ConditionGroup where, ReadLock lock) {
        return lifecycle.postLoad(client.lockingRead(query(where).withLock(Objects.requireNonNull(lock, "repository read lock must not be null")), entityType));
    }
    /** 在当前 Repository 绑定的表单上执行类型化聚合。 */
    public Flux<AggregateRow> aggregate(AggregateSpec spec) {
        return client.aggregate(requireRepositoryAggregate(spec));
    }
    public Mono<PageResult<T>> page(ConditionGroup where, PageQuery page) {
        QuerySpec query = query(where);
        if (!page.sorts().isEmpty()) {
            query = query.withSorts(page.sorts());
        }
        return client.page(query, page, entityType).flatMap(lifecycle::postLoad);
    }

    private Mono<Long> updateEntity(T entity, ConditionGroup where) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return lifecycle.update(safeEntity, () -> client.update(
                RepositoryUpdateSupport.updateSpec(form, metadata, entityValues, safeEntity, where)));
    }

    private Mono<Long> deleteEntity(T entity, ConditionGroup where) {
        T safeEntity = Objects.requireNonNull(entity, "repository entity must not be null");
        return lifecycle.remove(safeEntity, () -> client.delete(
                RepositoryUpdateSupport.deleteSpec(form, metadata, entityValues, safeEntity, where)));
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
