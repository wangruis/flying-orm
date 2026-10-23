package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.aggregate.AggregateRow;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.result.DynamicRow;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * DynamicForm 轻量 JOIN 的原生 JDBC 同步链式入口。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public final class SyncJoinQueryOperator extends JoinQueryCommand<SyncJoinQueryOperator> {

    private final SyncFormClient client;

    SyncJoinQueryOperator(SyncFormClient client, SqlRenderer renderer, DynamicForm rootForm) {
        super(rootForm, renderer);
        this.client = Objects.requireNonNull(client, "sync form client must not be null");
    }

    @Override
    protected SyncJoinQueryOperator self() { return this; }

    /** 使用同步客户端默认保护执行查询。 */
    public List<DynamicRow> executeRows() {
        return client.selectJoin(spec());
    }
    /** 使用本次显式保护执行查询。 */
    public List<DynamicRow> executeRows(SqlExecutionOptions options) {
        return client.selectJoin(spec(), options);
    }
    /** 使用已声明的 source-qualified 排序执行原生 JDBC count + 页数据查询。 */
    public PageResult<DynamicRow> page(PageQuery page) {
        return client.pageJoin(spec(), Objects.requireNonNull(page, "join page query must not be null"));
    }

    /** 直接执行关联聚合，复用普通聚合的布局、解码和字段保护。 */
    public List<AggregateRow> aggregate(Consumer<AggregateSpec.Builder> consumer) {
        return client.aggregate(aggregateSpec(consumer));
    }

    /** 对最终分组结果做数据库分页。 */
    public PageResult<AggregateRow> aggregatePage(Consumer<AggregateSpec.Builder> consumer, PageQuery page) {
        return client.aggregatePage(aggregateSpec(consumer), page);
    }

    public PageResult<AggregateRow> aggregatePage(Consumer<AggregateSpec.Builder> consumer, int page, int size) {
        return aggregatePage(consumer, PageQuery.of(page, size));
    }
    /** 使用现有平面 RowMapper 映射投影结果。 */
    public <T> List<T> execute(RowMapper<T> mapper) {
        RowMapper<T> safeMapper = Objects.requireNonNull(mapper, "join row mapper must not be null");
        return client.selectJoinMapped(spec(), safeMapper);
    }
}
