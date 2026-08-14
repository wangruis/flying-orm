package com.flying.orm.rdb.operator;

import com.flying.orm.core.lambda.EntityProperty;
import com.flying.orm.core.page.PageQuery;
import com.flying.orm.core.page.PageResult;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.Consumer;

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
        implements ProtectedEntityQuery<T, EntityDmlQueryOperator<T>> {

    private final ReactiveFormClient client;
    private final EntityQueryCommand<T> command;

    EntityDmlQueryOperator(ReactiveFormClient client, EntityQueryCommand<T> command) {
        this.client = Objects.requireNonNull(client, "form client must not be null");
        this.command = Objects.requireNonNull(command, "entity query command must not be null");
    }

    /** 添加严格等值条件。 */
    public EntityDmlQueryOperator<T> where(EntityProperty<T, ?> property, Object value) { command.state().where().equal(property, value); return this; }
    /** 继续追加严格 AND 等值条件。 */
    public EntityDmlQueryOperator<T> and(EntityProperty<T, ?> property, Object value) { return where(property, value); }
    /** 使用已注册条件运算符追加 AND 条件。 */
    public EntityDmlQueryOperator<T> and(EntityProperty<T, ?> property, String operator, Object value) { command.state().where().term(property, operator, value); return this; }
    /** 设置本次查询的敏感字段显示方式。 */
    @Override
    public EntityDmlQueryOperator<T> sensitiveDisplay(SensitiveDisplayMode mode) {
        command.sensitiveDisplay(mode);
        return this;
    }
    /** 添加 IS NULL 条件。 */
    public EntityDmlQueryOperator<T> isNull(EntityProperty<T, ?> property) { command.state().where().isNull(property); return this; }
    /** 添加 IS NOT NULL 条件。 */
    public EntityDmlQueryOperator<T> isNotNull(EntityProperty<T, ?> property) { command.state().where().isNotNull(property); return this; }
    /** 添加 IN 集合条件。 */
    public EntityDmlQueryOperator<T> in(EntityProperty<T, ?> property, Iterable<?> values) { command.state().where().in(property, values); return this; }
    /** 添加 NOT IN 集合条件。 */
    public EntityDmlQueryOperator<T> notIn(EntityProperty<T, ?> property, Iterable<?> values) { command.state().where().notIn(property, values); return this; }
    /** 添加闭区间 BETWEEN 条件。 */
    public EntityDmlQueryOperator<T> between(EntityProperty<T, ?> property, Object start, Object end) { command.state().where().between(property, start, end); return this; }
    /** 追加一个括号包裹的 OR 条件组。 */
    public EntityDmlQueryOperator<T> or(Consumer<EntityCondition<T>> consumer) { command.state().where().or(consumer); return this; }
    /** 追加一个括号包裹的 AND 条件组。 */
    public EntityDmlQueryOperator<T> andGroup(Consumer<EntityCondition<T>> consumer) { command.state().where().andGroup(consumer); return this; }
    /** 追加升序字段。 */
    public EntityDmlQueryOperator<T> orderByAsc(EntityProperty<T, ?> property) { command.orderByAsc(property); return this; }
    /** 追加降序字段。 */
    public EntityDmlQueryOperator<T> orderByDesc(EntityProperty<T, ?> property) { command.orderByDesc(property); return this; }

    /** 选择投影字段；投影查询必须通过 {@link #executeRows()} 取得紧凑行。 */
    @SafeVarargs
    public final EntityDmlQueryOperator<T> select(EntityProperty<T, ?>... properties) { command.select(properties); return this; }

    /** 追加 GROUP BY 字段；分组查询必须显式选择字段。 */
    @SafeVarargs
    public final EntityDmlQueryOperator<T> groupBy(EntityProperty<T, ?>... properties) { command.groupBy(properties); return this; }

    /** 为本次查询叠加显式数据范围。 */
    public EntityDmlQueryOperator<T> scope(DataScope scope) { command.state().scope(scope); return this; }

    /** 执行查询并返回惰性实体流。 */
    public Flux<T> execute() { return client.select(command.entitySpec(), command.state().metadata().type()); }
    /** 使用本次显式执行保护执行查询。 */
    public Flux<T> execute(SqlExecutionOptions options) { return client.select(command.entitySpec().withExecutionOptions(options), command.state().metadata().type()); }
    /** 查询零或一条记录；多于一条会失败，避免静默截断。 */
    public Mono<T> one() { return execute().singleOrEmpty(); }

    /** 执行有界页码分页。 */
    public Mono<PageResult<T>> page(int page, int size) { return client.page(command.entitySpec(), new PageQuery(page, size, command.sorts()), command.state().metadata().type()); }
    /** 使用本次资源保护执行有界分页。 */
    public Mono<PageResult<T>> page(int page, int size, SqlExecutionOptions options) { return client.page(command.entitySpec().withExecutionOptions(options), new PageQuery(page, size, command.sorts()), command.state().metadata().type()); }
    /** 执行投影或分组查询。 */
    public Flux<DynamicRow> executeRows() { return client.select(command.projectedSpec()); }
    /** 使用本次资源保护执行投影或分组查询。 */
    public Flux<DynamicRow> executeRows(SqlExecutionOptions options) { return client.select(command.projectedSpec().withExecutionOptions(options)); }

    EntityQueryCommand<T> command() { return command; }
}
