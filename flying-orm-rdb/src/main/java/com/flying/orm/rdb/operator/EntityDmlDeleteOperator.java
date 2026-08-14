package com.flying.orm.rdb.operator;

import com.flying.orm.core.lambda.EntityProperty;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.ReactiveFormClient;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * 以实体 Lambda 选择条件字段的响应式删除命令。
 *
 * <p>默认删除遵守实体的逻辑删除定义。物理删除也必须经过同一份 where、Scope 和乐观锁计划，
 * 不能因为调用了 {@link #physical()} 就绕过 fail-closed 的写入保护。</p>
 *
 * @param <T> 实体类型
 * @author wangr
 * @version v2.0.0
 */
public final class EntityDmlDeleteOperator<T> {

    private final ReactiveFormClient client;
    private final EntityDeleteCommand<T> command;

    EntityDmlDeleteOperator(ReactiveFormClient client, EntityDeleteCommand<T> command) {
        this.client = Objects.requireNonNull(client, "form client must not be null");
        this.command = Objects.requireNonNull(command, "entity delete command must not be null");
    }

    /** 添加严格等值条件；null 不会被静默忽略。 */
    public EntityDmlDeleteOperator<T> where(EntityProperty<T, ?> property, Object value) { command.state().where().equal(property, value); return this; }
    /** 继续追加严格 AND 等值条件。 */
    public EntityDmlDeleteOperator<T> and(EntityProperty<T, ?> property, Object value) { return where(property, value); }
    /** 使用已注册条件运算符追加 AND 条件。 */
    public EntityDmlDeleteOperator<T> and(EntityProperty<T, ?> property, String operator, Object value) { command.state().where().term(property, operator, value); return this; }
    /** 添加 IS NULL 条件。 */
    public EntityDmlDeleteOperator<T> isNull(EntityProperty<T, ?> property) { command.state().where().isNull(property); return this; }
    /** 添加 IS NOT NULL 条件。 */
    public EntityDmlDeleteOperator<T> isNotNull(EntityProperty<T, ?> property) { command.state().where().isNotNull(property); return this; }
    /** 添加 IN 集合条件。 */
    public EntityDmlDeleteOperator<T> in(EntityProperty<T, ?> property, Iterable<?> values) { command.state().where().in(property, values); return this; }
    /** 添加 NOT IN 集合条件。 */
    public EntityDmlDeleteOperator<T> notIn(EntityProperty<T, ?> property, Iterable<?> values) { command.state().where().notIn(property, values); return this; }
    /** 添加闭区间 BETWEEN 条件。 */
    public EntityDmlDeleteOperator<T> between(EntityProperty<T, ?> property, Object start, Object end) { command.state().where().between(property, start, end); return this; }
    /** 追加括号包裹的 OR 条件组。 */
    public EntityDmlDeleteOperator<T> or(Consumer<EntityCondition<T>> consumer) { command.state().where().or(consumer); return this; }
    /** 追加括号包裹的 AND 条件组。 */
    public EntityDmlDeleteOperator<T> andGroup(Consumer<EntityCondition<T>> consumer) { command.state().where().andGroup(consumer); return this; }
    /** 为本次删除叠加显式数据范围。 */
    public EntityDmlDeleteOperator<T> scope(DataScope scope) { command.state().scope(scope); return this; }
    /** 使用实体版本字段校验期望版本。 */
    public EntityDmlDeleteOperator<T> optimisticLock(Object expectedVersion) { command.optimisticLock(expectedVersion); return this; }
    /** 显式切换到物理删除。 */
    public EntityDmlDeleteOperator<T> physical() { command.physical(); return this; }
    /** 使用客户端默认保护提交删除。 */
    public Mono<Long> execute() { return write(command.spec()); }
    /** 使用本次显式执行保护提交删除。 */
    public Mono<Long> execute(SqlExecutionOptions options) { return write(command.spec().withExecutionOptions(options)); }

    private Mono<Long> write(com.flying.orm.rdb.form.spec.WriteSpec spec) {
        return command.physicalDelete() ? client.physicalDelete(spec) : client.delete(spec);
    }

    EntityDeleteCommand<T> command() { return command; }
}
