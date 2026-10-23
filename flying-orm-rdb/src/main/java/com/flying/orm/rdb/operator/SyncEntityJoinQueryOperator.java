package com.flying.orm.rdb.operator;

import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.aggregate.AggregateRow;
import java.util.function.Consumer;

import java.util.List;
import java.util.Objects;

/**
 * 实体 Lambda 轻量 JOIN 的原生 JDBC 同步入口。
 *
 * @param <R> 根实体类型
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public final class SyncEntityJoinQueryOperator<R> extends EntityJoinQueryCommand<R, SyncEntityJoinQueryOperator<R>> {

    private final SyncFormClient client;

    SyncEntityJoinQueryOperator(SyncFormClient client, SqlRenderer renderer, Class<R> rootType) {
        super(client.entityModels(), renderer, rootType);
        this.client = Objects.requireNonNull(client, "sync form client must not be null");
    }

    @Override
    protected SyncEntityJoinQueryOperator<R> self() { return this; }

    /** 使用同步客户端默认保护执行查询。 */
    public List<DynamicRow> executeRows() { return client.selectJoin(spec()); }
    /** 使用实体 Lambda 已声明的 JOIN 排序执行原生 JDBC 页码分页。 */
    public PageResult<DynamicRow> page(PageQuery page) {
        return client.pageJoin(spec(), Objects.requireNonNull(page, "join page query must not be null"));
    }
    /** 使用本次显式保护执行查询。 */
    public List<DynamicRow> executeRows(SqlExecutionOptions options) {
        return client.selectJoin(spec(), options);
    }

    /** 使用当前条件和 Scope 生成报表，输出由回调声明，不触发实体加载回调。 */
    public List<AggregateRow> aggregate(Consumer<EntityAggregate> consumer) {
        return client.aggregate(entityAggregateSpec(consumer));
    }

    /** 对 HAVING 后的聚合结果分页，排序使用报表别名。 */
    public PageResult<AggregateRow> aggregatePage(Consumer<EntityAggregate> consumer, PageQuery page) {
        return client.aggregatePage(entityAggregateSpec(consumer), page);
    }

    public PageResult<AggregateRow> aggregatePage(Consumer<EntityAggregate> consumer, int page, int size) {
        return aggregatePage(consumer, PageQuery.of(page, size));
    }

    /** 使用现有平面 RowMapper 映射投影结果。 */
    public <T> List<T> execute(RowMapper<T> mapper) {
        RowMapper<T> safeMapper = Objects.requireNonNull(mapper, "join row mapper must not be null");
        return client.selectJoinMapped(spec(), safeMapper);
    }
}
