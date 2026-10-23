package com.flying.orm.rdb.operator;

import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetPageResult;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.aggregate.AggregateRow;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Function;

/**
 * 实体 Lambda 查询的同步命令。
 *
 * <p>原生路径只调用 {@link SyncFormClient} 的同步方法，JDBC 执行发生在调用线程或业务主动选择的虚拟线程中。
 * 它不会创建 Publisher，也不会退回已经删除的 R2DBC 同步桥。</p>
 *
 * @param <T> 实体类型
 * @author wangr
 * @version v2.0.0
 */
public final class SyncEntityDmlQueryOperator<T>
        extends EntityQueryCommand<T, SyncEntityDmlQueryOperator<T>> {

    private final SyncFormClient client;
    private Consumer<? super T> onLoad;

    SyncEntityDmlQueryOperator(SyncFormClient client, EntityDmlModel<T> model, SqlRenderer renderer) {
        super(model, renderer, client.defaultFieldScope());
        this.client = Objects.requireNonNull(client, "sync form client must not be null");
    }

    /** 使用完整查询输入中的分页窗口，同时返回总数。 */
    public PageResult<T> page() {
        PageQuery page = inputPage();
        return page(page.page(), page.size());
    }

    /** 使用完整查询输入中的窗口映射分页结果。 */
    public <R> PageResult<R> page(Class<R> type) {
        PageQuery page = inputPage();
        return page(page.page(), page.size(), type);
    }

    private SyncEntityDmlQueryOperator(SyncEntityDmlQueryOperator<T> source) {
        super(source);
        this.client = source.client;
        this.onLoad = source.onLoad;
    }

    @Override
    protected SyncEntityDmlQueryOperator<T> self() {
        return this;
    }

    /** 复制查询配置与加载回调；后续修改互不影响。 */
    public SyncEntityDmlQueryOperator<T> copy() {
        return new SyncEntityDmlQueryOperator<>(this);
    }

    /** 追加实体返回前的同步回调；按注册顺序执行，executeRows 不触发。 */
    public SyncEntityDmlQueryOperator<T> onLoad(Consumer<? super T> callback) {
        Consumer<? super T> additional = Objects.requireNonNull(callback, "entity load callback must not be null");
        Consumer<? super T> previous = onLoad;
        onLoad = previous == null ? additional : entity -> {
            previous.accept(entity);
            additional.accept(entity);
        };
        return this;
    }

    /** 查询实体列表；显式 select 时只填写已选择字段，其余遵循既有映射默认值。 */
    public List<T> fetch() { return loaded(client.select(entitySpec(), metadata().type())); }
    /** 逐行映射、执行加载回调并消费；原生 JDBC 不收集列表，回调期间不要重入同一连接。 */
    public long forEach(Consumer<? super T> consumer) {
        Consumer<? super T> safeConsumer = Objects.requireNonNull(consumer, "row consumer must not be null");
        return client.forEach(entitySpec(), metadata().type(),
                entity -> safeConsumer.accept(loaded(entity)));
    }
    /** 逐行读取投影 DTO/record，不触发实体加载回调。 */
    public <R> long forEach(Class<R> type, Consumer<? super R> consumer) {
        return client.forEach(resultSpec(), type, consumer);
    }
    /** 逐行加载实体，false 正常停止；返回数包含停止行，不再加载后续实体。 */
    public long forEachWhile(Predicate<? super T> consumer) {
        Predicate<? super T> safeConsumer = Objects.requireNonNull(consumer, "row consumer must not be null");
        return client.forEachWhile(entitySpec(), metadata().type(),
                entity -> safeConsumer.test(loaded(entity)));
    }
    /** 逐行映射投影并允许正常停止，不触发实体加载回调；返回数包含停止行。 */
    public <R> long forEachWhile(Class<R> type, Predicate<? super R> consumer) {
        return client.forEachWhile(resultSpec(), type, consumer);
    }
    /** 读取投影 DTO/record，不触发实体加载回调。 */
    public <R> List<R> fetch(Class<R> type) { return client.select(resultSpec(), type); }
    /** 读取零或一条投影结果；多行报错，不触发实体加载回调。 */
    public <R> R one(Class<R> type) { return client.selectOne(resultSpec(), type); }
    /** 读取首条投影结果，不触发实体加载回调。 */
    public <R> R first(Class<R> type) { return client.first(resultSpec(), type); }
    /** 按当前条件和排序分页读取投影结果，不触发实体加载回调。 */
    public <R> PageResult<R> page(int page, int size, Class<R> type) {
        return client.page(resultSpec(), PageQuery.of(page, size), type);
    }
    /** 使用本次资源保护读取实体或部分实体。 */
    public List<T> fetch(SqlExecutionOptions options) { return loaded(client.select(entitySpec().withExecutionOptions(options), metadata().type())); }
    /** 查询零或一条；没有记录返回 null，多于一条明确失败。 */
    public T one() { return loaded(client.selectOne(entitySpec(), metadata().type())); }
    /** 只取排序下的首条实体，不查询总数；未排序时由数据库决定首行，空查询返回 null。 */
    public T first() { return loaded(client.first(entitySpec(), metadata().type())); }
    /** 使用本次资源保护读取首条实体，保留投影和加载回调。 */
    public T first(SqlExecutionOptions options) { return loaded(client.first(entitySpec().withExecutionOptions(options), metadata().type())); }
    /** 统计当前条件下的行数；有 groupBy 时统计组数，忽略投影和排序，不触发实体加载回调。 */
    public long count() { return client.count(countSpec()); }
    /** 使用本次执行保护统计行数或组数。 */
    public long count(SqlExecutionOptions options) { return client.count(countSpec().withExecutionOptions(options)); }
    /** 执行有界页码分页。 */
    public PageResult<T> page(int page, int size) { return loadedPage(client.page(entitySpec(), PageQuery.of(page, size), metadata().type()), PageResult::rows); }
    /** 使用本次资源保护执行有界分页。 */
    public PageResult<T> page(int page, int size, SqlExecutionOptions options) { return loadedPage(client.page(entitySpec().withExecutionOptions(options), PageQuery.of(page, size), metadata().type()), PageResult::rows); }

    /** 支持空值及自动补齐唯一排序键的实体分页；排序在 page 中声明，保留加载回调。 */
    public KeysetPageResult<T> keysetPage(KeysetPageQuery page) {
        return loadedPage(client.keysetPage(entitySpec(), keysetQuery(page),
                metadata().type()), KeysetPageResult::rows);
    }

    /** keyset 读取 DTO/record，保留隐藏排序位置，不触发实体加载回调。 */
    public <R> KeysetPageResult<R> keysetPage(KeysetPageQuery page, Class<R> type) {
        return client.keysetPage(resultSpec(), keysetQuery(page), type);
    }
    /** 执行投影或分组查询并返回紧凑行。 */
    public List<DynamicRow> executeRows() { return client.select(projectedSpec()); }
    /** 使用本次资源保护执行投影或分组查询。 */
    public List<DynamicRow> executeRows(SqlExecutionOptions options) { return client.select(projectedSpec().withExecutionOptions(options)); }

    /** 使用当前条件和 Scope 生成报表，输出由回调声明，不触发实体加载回调。 */
    public List<AggregateRow> aggregate(Consumer<EntityAggregate> consumer) {
        return client.aggregate(aggregateSpec(consumer));
    }

    /** 对 HAVING 后的聚合结果分页，排序使用报表别名。 */
    public PageResult<AggregateRow> aggregatePage(Consumer<EntityAggregate> consumer, PageQuery page) {
        return client.aggregatePage(aggregateSpec(consumer), page);
    }

    public PageResult<AggregateRow> aggregatePage(Consumer<EntityAggregate> consumer, int page, int size) {
        return aggregatePage(consumer, PageQuery.of(page, size));
    }

    private T loaded(T entity) {
        if (entity != null && onLoad != null) {
            onLoad.accept(entity);
        }
        return entity;
    }

    private List<T> loaded(List<T> entities) {
        if (onLoad != null) {
            entities.forEach(onLoad);
        }
        return entities;
    }

    private <R> R loadedPage(R page, Function<R, List<T>> rows) {
        loaded(rows.apply(page));
        return page;
    }
}
