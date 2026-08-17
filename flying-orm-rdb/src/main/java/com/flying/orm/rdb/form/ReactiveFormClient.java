package com.flying.orm.rdb.form;

import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorPageResult;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.internal.InternalApi;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.operator.EntityDmlOperator;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 动态表单响应式 CRUD 的稳定公开入口。
 *
 * <p>客户端本身只保存不可变配置和对外 API。查询、分页、写入、批量写入、Scope 合并、字段解码等细节
 * 由同包内部操作协作者完成，所以一个已经装配好的客户端可以安全复用给并发订阅。所有数据库 I/O 仍在
 * Reactor 订阅链内发生：这里不会调用 {@code block()} 或 {@code subscribe()}，也不会先收集整个输入流。</p>
 *
 * <p>同包内部的细粒度操作按职责放在 {@link ReactiveFormOperations} 管理的协作者中，同步门面直接复用这些
 * 协作者，因此 R2DBC、SQL、Scope、逻辑删除、乐观锁和执行保护始终只有一套实现。业务代码优先使用
 * QuerySpec、WriteSpec、BatchSpec 三类不可变规格，避免在调用点拼接 SQL。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
public final class ReactiveFormClient {

    private final ReactiveFormOperationContext context;
    private final ReactiveFormOperations operations;
    private final FormDataSqlRenderer renderer;
    private final ReactiveFormResultSupport results;
    private final EntityModelRegistry entityModels;
    private final BatchWriteOptions defaultBatchWriteOptions;

    private ReactiveFormClient(ReactiveSqlExecutor executor, FormDataSqlRenderer renderer) {
        this(new ReactiveFormOperationContext(executor,
                                              renderer,
                                              defaultStructuredConditionResolver(renderer),
                                              DataScope.none(),
                                              SqlExecutionOptions.safeDefaults(),
                                              BatchWriteOptions.defaults(),
                                              EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults())));
    }

    private ReactiveFormClient(ReactiveFormOperationContext context) {
        this.context = Objects.requireNonNull(context, "form operation context must not be null");
        this.operations = new ReactiveFormOperations(context);
        this.renderer = context.renderer();
        this.results = operations.results;
        this.entityModels = context.entityModels();
        this.defaultBatchWriteOptions = context.defaultBatchWriteOptions();
    }

    /** 创建动态表单响应式客户端。 */
    public static ReactiveFormClient create(ReactiveSqlExecutor executor, FormDataSqlRenderer renderer) {
        return new ReactiveFormClient(executor, renderer);
    }

    /** 为前端结构化条件替换线程安全的解析器。 */
    public ReactiveFormClient withStructuredConditionResolver(StructuredConditionResolver resolver) {
        return new ReactiveFormClient(context.withResolver(resolver));
    }

    /** 设置没有显式 options 时使用的 SQL 执行保护。 */
    public ReactiveFormClient withDefaultExecutionOptions(SqlExecutionOptions options) {
        return new ReactiveFormClient(context.withExecutionOptions(options));
    }

    /** 设置没有显式 options 时使用的批量策略。 */
    public ReactiveFormClient withDefaultBatchWriteOptions(BatchWriteOptions options) {
        return new ReactiveFormClient(context.withBatchWriteOptions(
                Objects.requireNonNull(options, "batch write options must not be null")));
    }

    /** 在客户端默认范围上继续收窄。 */
    public ReactiveFormClient withDefaultDataScope(DataScope scope) {
        DataScope combined = context.defaultDataScope().and(Objects.requireNonNull(scope,
                                                                                   "data scope must not be null"));
        return new ReactiveFormClient(context.withDataScope(combined));
    }

    /** Repository 和容器集成读取批量默认策略时使用。 */
    @InternalApi
    public BatchWriteOptions defaultBatchWriteOptions() {
        return defaultBatchWriteOptions;
    }

    /** 为当前客户端绑定实例级实体映射缓存。 */
    @InternalApi
    public ReactiveFormClient withEntityModelRegistry(EntityModelRegistry registry) {
        return new ReactiveFormClient(context.withEntityModels(
                Objects.requireNonNull(registry, "entity model registry must not be null")));
    }

    /** @return 当前客户端使用的实体映射注册表。 */
    @InternalApi
    public EntityModelRegistry entityModels() {
        return entityModels;
    }

    /** 为实体提供复用当前 Scope、执行保护和映射缓存的 Lambda DML 入口。 */
    public <T> EntityDmlOperator<T> entity(Class<T> type) {
        return EntityDmlOperator.create(this, renderer.conditionRenderer(), type);
    }

    /** Repository 内部创建 Lambda DML 状态时复用当前已装配的条件渲染器。 */
    @InternalApi
    public SqlRenderer entityRenderer() {
        return renderer.conditionRenderer();
    }

    /** 执行不可变查询规格并返回紧凑动态行流。 */
    public Flux<DynamicRow> select(QuerySpec spec) {
        return operations().selectSpec(spec);
    }

    /** 执行查询规格并映射实体。 */
    public <T> Flux<T> select(QuerySpec spec, Class<T> type) {
        RowMapper<T> mapper = results.rowMapper(type, "form result type must not be null");
        return FormResultMappingSupport.mapRows(select(spec), mapper);
    }

    /** 执行一基页码分页。 */
    public Mono<PageResult<DynamicRow>> page(QuerySpec spec, PageQuery page) {
        return operations().pageSpec(spec, page);
    }

    /** 执行页码分页并映射实体。 */
    public <T> Mono<PageResult<T>> page(QuerySpec spec, PageQuery page, Class<T> type) {
        return FormResultMappingSupport.mapPage(page(spec, page),
                                                results.rowMapper(type, "page result type must not be null"));
    }

    /** 执行稳定游标分页，不额外执行 count SQL。 */
    public Mono<CursorPageResult<DynamicRow>> cursorPage(QuerySpec spec, CursorPageQuery page) {
        return operations().cursorPageSpec(spec, page);
    }

    /** 执行游标分页并映射实体。 */
    public <T> Mono<CursorPageResult<T>> cursorPage(QuerySpec spec, CursorPageQuery page, Class<T> type) {
        return FormResultMappingSupport.mapCursorPage(cursorPage(spec, page),
                                                      results.rowMapper(type, "cursor page result type must not be null"));
    }

    /** 执行插入规格。 */
    public Mono<Long> insert(WriteSpec spec) {
        return operations().insertSpec(spec);
    }

    /** 执行轻量多表查询并返回显式投影组成的紧凑动态行。 */
    public Flux<DynamicRow> selectJoin(JoinQuerySpec spec) {
        return operations().selectJoin(spec, null);
    }

    /** 使用本次显式执行保护执行轻量多表查询。 */
    public Flux<DynamicRow> selectJoin(JoinQuerySpec spec, SqlExecutionOptions options) {
        return operations().selectJoin(spec, Objects.requireNonNull(
                options, "join execution options must not be null"));
    }

    /** 使用 JOIN AST 的同一 Scope/条件分别执行 count 与页数据查询。 */
    public Mono<PageResult<DynamicRow>> pageJoin(JoinQuerySpec spec, PageQuery page) {
        return operations().pageJoin(spec, page, null);
    }

    /** 使用本次执行保护完成 JOIN 页码分页。 */
    public Mono<PageResult<DynamicRow>> pageJoin(JoinQuerySpec spec,
                                                 PageQuery page,
                                                 SqlExecutionOptions options) {
        return operations().pageJoin(spec, page, Objects.requireNonNull(
                options, "join execution options must not be null"));
    }

    /** Repository 的数据库生成主键回填路径；不会改变动态表单 insert 的简洁返回值。 */
    @InternalApi
    public Mono<SqlWriteResult> insertReturningKeys(WriteSpec spec) {
        return operations().insertReturningKeysSpec(spec);
    }

    /** 执行更新规格。 */
    public Mono<Long> update(WriteSpec spec) {
        return operations().updateSpec(spec);
    }

    /** 执行逻辑删除优先的删除规格。 */
    public Mono<Long> delete(WriteSpec spec) {
        return operations().deleteSpec(spec);
    }

    /** 执行明确的物理删除。 */
    public Mono<Long> physicalDelete(WriteSpec spec) {
        return operations().physicalDeleteSpec(spec);
    }

    /** 执行流式 insert、upsert 或逐行乐观更新批量规格。 */
    public Mono<BatchWriteResult> writeBatch(BatchSpec spec) {
        return operations().writeBatchSpec(spec);
    }

    /** 流式发布 INDEPENDENT 模式下已完成的分片结果。 */
    public Flux<BatchChunkResult> writeBatchChunks(BatchSpec spec) {
        return operations().writeBatchChunksSpec(spec);
    }

    /** 同包同步门面和实体入口复用这条已经装配好的内部操作链。 */
    ReactiveFormOperations operations() {
        return operations;
    }

    private static StructuredConditionResolver defaultStructuredConditionResolver(FormDataSqlRenderer renderer) {
        FormDataSqlRenderer safeRenderer = Objects.requireNonNull(renderer,
                                                                  "form data sql renderer must not be null");
        return StructuredConditionResolver.defaults(safeRenderer.valueCodecs());
    }
}
