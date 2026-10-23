package com.flying.orm.rdb.operator;

import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.aggregate.AggregateRow;
import java.util.function.Consumer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 实体 Lambda 轻量 JOIN 的响应式链式入口。
 *
 * @param <R> 根实体类型
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public final class EntityJoinQueryOperator<R> extends EntityJoinQueryCommand<R, EntityJoinQueryOperator<R>> {

    private final ReactiveFormClient client;

    EntityJoinQueryOperator(ReactiveFormClient client, SqlRenderer renderer, Class<R> rootType) {
        super(client.entityModels(), renderer, rootType);
        this.client = Objects.requireNonNull(client, "form client must not be null");
    }

    @Override
    protected EntityJoinQueryOperator<R> self() { return this; }

    /** 使用客户端默认保护惰性执行查询。 */
    public Flux<DynamicRow> executeRows() { return client.selectJoin(spec()); }
    /** 使用实体 Lambda 已声明的 JOIN 排序执行页码分页。 */
    public Mono<PageResult<DynamicRow>> page(PageQuery page) {
        return client.pageJoin(spec(), Objects.requireNonNull(page, "join page query must not be null"));
    }
    /** 使用本次显式保护惰性执行查询。 */
    public Flux<DynamicRow> executeRows(SqlExecutionOptions options) {
        return client.selectJoin(spec(), options);
    }

    /** 使用当前条件和 Scope 生成报表，输出由回调声明，不触发实体加载回调。 */
    public Flux<AggregateRow> aggregate(Consumer<EntityAggregate> consumer) {
        return client.aggregate(entityAggregateSpec(consumer));
    }

    /** 对 HAVING 后的聚合结果分页，排序使用报表别名。 */
    public Mono<PageResult<AggregateRow>> aggregatePage(Consumer<EntityAggregate> consumer, PageQuery page) {
        return client.aggregatePage(entityAggregateSpec(consumer), page);
    }

    public Mono<PageResult<AggregateRow>> aggregatePage(Consumer<EntityAggregate> consumer, int page, int size) {
        return aggregatePage(consumer, PageQuery.of(page, size));
    }

    /** 使用现有平面 RowMapper 映射投影结果。 */
    public <T> Flux<T> execute(RowMapper<T> mapper) {
        RowMapper<T> safeMapper = Objects.requireNonNull(mapper, "join row mapper must not be null");
        return executeRows().map(safeMapper::map);
    }
}
