package com.flying.orm.rdb.operator;

import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetPageResult;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.aggregate.AggregateRow;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 以实体 Lambda 声明条件的响应式查询命令。
 *
 * <p>本门面只负责响应式返回值。条件、字段解析、投影、排序、分组和 QuerySpec 生成全部收在
 * {@link EntityQueryCommand}，原生同步命令复用同一个计划类，因此两种执行方式不会生成不同 SQL。</p>
 *
 * @param <T> 实体类型
 * @author wangr
 * @version v2.0.0
 */
public final class EntityDmlQueryOperator<T>
        extends EntityQueryCommand<T, EntityDmlQueryOperator<T>> {

    private final ReactiveFormClient client;
    private Function<? super T, ? extends Publisher<Void>> onLoad;

    EntityDmlQueryOperator(ReactiveFormClient client, EntityDmlModel<T> model, SqlRenderer renderer) {
        super(model, renderer, client.defaultFieldScope());
        this.client = Objects.requireNonNull(client, "form client must not be null");
    }

    /** 使用完整查询输入中的分页窗口，同时返回总数。 */
    public Mono<PageResult<T>> page() {
        PageQuery page = inputPage();
        return page(page.page(), page.size());
    }

    /** 使用完整查询输入中的窗口映射分页结果。 */
    public <R> Mono<PageResult<R>> page(Class<R> type) {
        PageQuery page = inputPage();
        return page(page.page(), page.size(), type);
    }

    private EntityDmlQueryOperator(EntityDmlQueryOperator<T> source) {
        super(source);
        this.client = source.client;
        this.onLoad = source.onLoad;
    }

    @Override
    protected EntityDmlQueryOperator<T> self() {
        return this;
    }

    /** 复制查询配置与加载回调；后续修改互不影响。 */
    public EntityDmlQueryOperator<T> copy() {
        return new EntityDmlQueryOperator<>(this);
    }

    /**
     * 追加实体返回前的异步回调，按注册顺序和结果顺序等待完成。
     * 回调只用于实体终端，executeRows 不触发；异常和取消沿原执行链传播。
     */
    public EntityDmlQueryOperator<T> onLoad(Function<? super T, ? extends Publisher<Void>> callback) {
        Function<? super T, ? extends Publisher<Void>> additional = Objects.requireNonNull(
                callback, "entity load callback must not be null");
        Function<? super T, ? extends Publisher<Void>> previous = onLoad;
        onLoad = previous == null ? additional : entity -> Mono.from(previous.apply(entity))
                .then(Mono.defer(() -> Mono.from(additional.apply(entity))));
        return this;
    }

    /** 查询实体流；显式 select 时只填写已选择字段，其余遵循既有映射默认值。 */
    public Flux<T> fetch() { return loaded(client.select(entitySpec(), metadata().type())); }
    /** 读取投影 DTO/record，不触发实体加载回调。 */
    public <R> Flux<R> fetch(Class<R> type) { return client.select(resultSpec(), type); }
    /** 读取零或一条投影结果；多行报错，不触发实体加载回调。 */
    public <R> Mono<R> one(Class<R> type) { return client.select(resultSpec(), type).singleOrEmpty(); }
    /** 读取首条投影结果，不触发实体加载回调。 */
    public <R> Mono<R> first(Class<R> type) { return client.first(resultSpec(), type); }
    /** 按当前条件和排序分页读取投影结果，不触发实体加载回调。 */
    public <R> Mono<PageResult<R>> page(int page, int size, Class<R> type) {
        return client.page(resultSpec(), PageQuery.of(page, size), type);
    }
    /** 使用本次资源保护读取实体或部分实体。 */
    public Flux<T> fetch(SqlExecutionOptions options) { return loaded(client.select(entitySpec().withExecutionOptions(options), metadata().type())); }
    /** 查询零或一条记录；多于一条会失败，避免静默截断。 */
    public Mono<T> one() { return loaded(client.select(entitySpec(), metadata().type()).singleOrEmpty()); }

    /** 只取排序下的首条实体，不查询总数；未排序时由数据库决定首行。 */
    public Mono<T> first() { return loaded(client.first(entitySpec(), metadata().type())); }
    /** 使用本次资源保护读取首条实体，保留投影和加载回调。 */
    public Mono<T> first(SqlExecutionOptions options) { return loaded(client.first(entitySpec().withExecutionOptions(options), metadata().type())); }

    /** 统计当前条件下的行数；有 groupBy 时统计组数，忽略投影和排序，不触发实体加载回调。 */
    public Mono<Long> count() { return client.count(countSpec()); }
    /** 使用本次执行保护统计行数或组数。 */
    public Mono<Long> count(SqlExecutionOptions options) { return client.count(countSpec().withExecutionOptions(options)); }

    /** 执行有界页码分页。 */
    public Mono<PageResult<T>> page(int page, int size) { return loadedPage(client.page(entitySpec(), PageQuery.of(page, size), metadata().type()), PageResult::rows); }
    /** 使用本次资源保护执行有界分页。 */
    public Mono<PageResult<T>> page(int page, int size, SqlExecutionOptions options) { return loadedPage(client.page(entitySpec().withExecutionOptions(options), PageQuery.of(page, size), metadata().type()), PageResult::rows); }

    /** 支持空值及自动补齐唯一排序键的实体分页；排序在 page 中声明，保留加载回调。 */
    public Mono<KeysetPageResult<T>> keysetPage(KeysetPageQuery page) {
        return loadedPage(client.keysetPage(entitySpec(), keysetQuery(page),
                metadata().type()), KeysetPageResult::rows);
    }

    /** keyset 读取 DTO/record，保留隐藏排序位置，不触发实体加载回调。 */
    public <R> Mono<KeysetPageResult<R>> keysetPage(KeysetPageQuery page, Class<R> type) {
        return client.keysetPage(resultSpec(), keysetQuery(page), type);
    }
    /** 执行投影或分组查询。 */
    public Flux<DynamicRow> executeRows() { return client.select(projectedSpec()); }
    /** 使用本次资源保护执行投影或分组查询。 */
    public Flux<DynamicRow> executeRows(SqlExecutionOptions options) { return client.select(projectedSpec().withExecutionOptions(options)); }

    /** 使用当前条件和 Scope 生成报表，输出由回调声明，不触发实体加载回调。 */
    public Flux<AggregateRow> aggregate(Consumer<EntityAggregate> consumer) {
        return client.aggregate(aggregateSpec(consumer));
    }

    /** 对 HAVING 后的聚合结果分页，排序使用报表别名。 */
    public Mono<PageResult<AggregateRow>> aggregatePage(Consumer<EntityAggregate> consumer, PageQuery page) {
        return client.aggregatePage(aggregateSpec(consumer), page);
    }

    public Mono<PageResult<AggregateRow>> aggregatePage(Consumer<EntityAggregate> consumer, int page, int size) {
        return aggregatePage(consumer, PageQuery.of(page, size));
    }

    private Flux<T> loaded(Flux<T> source) {
        Function<? super T, ? extends Publisher<Void>> callback = onLoad;
        return callback == null ? source
                : source.concatMap(entity -> Mono.from(callback.apply(entity)).thenReturn(entity), 0);
    }

    private Mono<T> loaded(Mono<T> source) {
        Function<? super T, ? extends Publisher<Void>> callback = onLoad;
        return callback == null ? source
                : source.flatMap(entity -> Mono.from(callback.apply(entity)).thenReturn(entity));
    }

    private <R> Mono<R> loadedPage(Mono<R> source, Function<R, List<T>> rows) {
        Function<? super T, ? extends Publisher<Void>> callback = onLoad;
        return callback == null ? source : source.flatMap(page -> Flux.fromIterable(rows.apply(page))
                .concatMap(entity -> Mono.from(callback.apply(entity)), 0).then(Mono.just(page)));
    }

}
