package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.aggregate.AggregateRow;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * DynamicForm 轻量 JOIN 的响应式链式入口。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public final class JoinQueryOperator extends JoinQueryCommand<JoinQueryOperator> {

    private final ReactiveFormClient client;

    JoinQueryOperator(ReactiveFormClient client, SqlRenderer renderer, DynamicForm rootForm) {
        super(rootForm, renderer);
        this.client = Objects.requireNonNull(client, "form client must not be null");
    }

    @Override
    protected JoinQueryOperator self() { return this; }

    /** 使用客户端默认保护惰性执行查询。 */
    public Flux<DynamicRow> executeRows() {
        return client.selectJoin(spec());
    }
    /** 使用本次显式保护惰性执行查询。 */
    public Flux<DynamicRow> executeRows(SqlExecutionOptions options) {
        return client.selectJoin(spec(), options);
    }
    /** 使用已声明的 source-qualified 排序执行 count + 页数据查询。 */
    public Mono<PageResult<DynamicRow>> page(PageQuery page) {
        return client.pageJoin(spec(), Objects.requireNonNull(page, "join page query must not be null"));
    }

    /** 直接在关联关系上分组和聚合；来源 Scope、字段转换及脱敏沿用同一内核。 */
    public Flux<AggregateRow> aggregate(Consumer<AggregateSpec.Builder> consumer) {
        return client.aggregate(aggregateSpec(consumer));
    }

    /** 对 HAVING 后的聚合结果做数据库分页，排序引用聚合结果别名。 */
    public Mono<PageResult<AggregateRow>> aggregatePage(Consumer<AggregateSpec.Builder> consumer, PageQuery page) {
        return client.aggregatePage(aggregateSpec(consumer), page);
    }

    public Mono<PageResult<AggregateRow>> aggregatePage(Consumer<AggregateSpec.Builder> consumer, int page, int size) {
        return aggregatePage(consumer, PageQuery.of(page, size));
    }
    /** 使用现有平面 RowMapper 映射投影结果。 */
    public <T> Flux<T> execute(RowMapper<T> mapper) {
        RowMapper<T> safeMapper = Objects.requireNonNull(mapper, "join row mapper must not be null");
        return executeRows().map(safeMapper::map);
    }
}
