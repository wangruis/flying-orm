package com.flying.orm.rdb.operator;

import com.flying.orm.core.lambda.EntityProperty;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.result.DynamicRow;

import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

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
        implements ProtectedEntityQuery<T, SyncEntityDmlQueryOperator<T>> {

    private final SyncFormClient client;
    private final EntityQueryCommand<T> command;

    SyncEntityDmlQueryOperator(SyncFormClient client, EntityQueryCommand<T> command) {
        this.client = Objects.requireNonNull(client, "sync form client must not be null");
        this.command = Objects.requireNonNull(command, "entity query command must not be null");
    }

    /** 添加严格等值条件。 */
    public SyncEntityDmlQueryOperator<T> where(EntityProperty<T, ?> property, Object value) { command.state().where().equal(property, value); return this; }
    /** 继续追加严格 AND 等值条件。 */
    public SyncEntityDmlQueryOperator<T> and(EntityProperty<T, ?> property, Object value) { return where(property, value); }
    /** 使用已注册条件运算符追加 AND 条件。 */
    public SyncEntityDmlQueryOperator<T> and(EntityProperty<T, ?> property, String operator, Object value) { command.state().where().term(property, operator, value); return this; }
    /** 设置本次查询的敏感字段显示方式。 */
    @Override
    public SyncEntityDmlQueryOperator<T> sensitiveDisplay(SensitiveDisplayMode mode) {
        command.sensitiveDisplay(mode);
        return this;
    }
    /** 添加 IS NULL 条件。 */
    public SyncEntityDmlQueryOperator<T> isNull(EntityProperty<T, ?> property) { command.state().where().isNull(property); return this; }
    /** 添加 IS NOT NULL 条件。 */
    public SyncEntityDmlQueryOperator<T> isNotNull(EntityProperty<T, ?> property) { command.state().where().isNotNull(property); return this; }
    /** 添加 IN 集合条件。 */
    public SyncEntityDmlQueryOperator<T> in(EntityProperty<T, ?> property, Iterable<?> values) { command.state().where().in(property, values); return this; }
    /** 添加 NOT IN 集合条件。 */
    public SyncEntityDmlQueryOperator<T> notIn(EntityProperty<T, ?> property, Iterable<?> values) { command.state().where().notIn(property, values); return this; }
    /** 添加闭区间 BETWEEN 条件。 */
    public SyncEntityDmlQueryOperator<T> between(EntityProperty<T, ?> property, Object start, Object end) { command.state().where().between(property, start, end); return this; }
    /** 追加括号包裹的 OR 条件组。 */
    public SyncEntityDmlQueryOperator<T> or(Consumer<EntityCondition<T>> consumer) { command.state().where().or(consumer); return this; }
    /** 追加括号包裹的 AND 条件组。 */
    public SyncEntityDmlQueryOperator<T> andGroup(Consumer<EntityCondition<T>> consumer) { command.state().where().andGroup(consumer); return this; }
    /** 追加升序字段。 */
    public SyncEntityDmlQueryOperator<T> orderByAsc(EntityProperty<T, ?> property) { command.orderByAsc(property); return this; }
    /** 追加降序字段。 */
    public SyncEntityDmlQueryOperator<T> orderByDesc(EntityProperty<T, ?> property) { command.orderByDesc(property); return this; }
    /** 选择投影字段；投影结果通过 {@link #executeRows()} 返回。 */
    @SafeVarargs
    public final SyncEntityDmlQueryOperator<T> select(EntityProperty<T, ?>... properties) { command.select(properties); return this; }
    /** 追加 GROUP BY 字段。 */
    @SafeVarargs
    public final SyncEntityDmlQueryOperator<T> groupBy(EntityProperty<T, ?>... properties) { command.groupBy(properties); return this; }
    /** 叠加显式数据范围。 */
    public SyncEntityDmlQueryOperator<T> scope(DataScope scope) { command.state().scope(scope); return this; }

    /** 执行并收集执行保护约束的实体结果。 */
    public List<T> execute() { return client.select(command.entitySpec(), command.state().metadata().type()); }
    /** 使用本次显式执行保护查询实体。 */
    public List<T> execute(SqlExecutionOptions options) { return client.select(command.entitySpec().withExecutionOptions(options), command.state().metadata().type()); }
    /** 查询零或一条；没有记录返回 null，多于一条明确失败。 */
    public T one() { return only(execute()); }
    /** 执行有界页码分页。 */
    public PageResult<T> page(int page, int size) { return client.page(command.entitySpec(), new PageQuery(page, size, command.sorts()), command.state().metadata().type()); }
    /** 使用本次资源保护执行有界分页。 */
    public PageResult<T> page(int page, int size, SqlExecutionOptions options) { return client.page(command.entitySpec().withExecutionOptions(options), new PageQuery(page, size, command.sorts()), command.state().metadata().type()); }
    /** 执行投影或分组查询并返回紧凑行。 */
    public List<DynamicRow> executeRows() { return client.select(command.projectedSpec()); }
    /** 使用本次资源保护执行投影或分组查询。 */
    public List<DynamicRow> executeRows(SqlExecutionOptions options) { return client.select(command.projectedSpec().withExecutionOptions(options)); }

    private T only(List<T> rows) {
        if (rows.isEmpty()) {
            return null;
        }
        if (rows.size() != 1) {
            throw new IllegalStateException("entity query expected zero or one row but received " + rows.size());
        }
        return rows.getFirst();
    }
}
