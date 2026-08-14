package com.flying.orm.rdb.operator;

import com.flying.orm.core.lambda.EntityProperty;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.ReactiveFormClient;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * 以实体 Lambda 选择字段的响应式更新命令。
 *
 * <p>字段可写性、原子增减和乐观锁校验不在这里复制实现，而是交给 {@link EntityUpdateCommand}。
 * 这样响应式执行和原生 JDBC 同步执行使用一套 WriteSpec，参数顺序和安全规则始终一致。</p>
 *
 * @param <T> 实体类型
 * @author wangr
 * @version v2.0.0
 */
public final class EntityDmlUpdateOperator<T> {

    private final ReactiveFormClient client;
    private final EntityUpdateCommand<T> command;

    EntityDmlUpdateOperator(ReactiveFormClient client, EntityUpdateCommand<T> command) {
        this.client = Objects.requireNonNull(client, "form client must not be null");
        this.command = Objects.requireNonNull(command, "entity update command must not be null");
    }

    /** 设置待更新属性；版本列、逻辑删除列和不可更新列会在这里之前被拒绝。 */
    public EntityDmlUpdateOperator<T> set(EntityProperty<T, ?> property, Object value) { command.set(property, value); return this; }
    /** 对数值属性执行数据库端原子增加。 */
    public EntityDmlUpdateOperator<T> increment(EntityProperty<T, ? extends Number> property, Number amount) { command.increment(property, amount); return this; }
    /** 对数值属性执行数据库端原子减少。 */
    public EntityDmlUpdateOperator<T> decrement(EntityProperty<T, ? extends Number> property, Number amount) { command.decrement(property, amount); return this; }
    /** 添加严格等值条件；null 不会被静默忽略。 */
    public EntityDmlUpdateOperator<T> where(EntityProperty<T, ?> property, Object value) { command.state().where().equal(property, value); return this; }
    /** 继续追加严格 AND 等值条件。 */
    public EntityDmlUpdateOperator<T> and(EntityProperty<T, ?> property, Object value) { return where(property, value); }
    /** 使用已注册条件运算符追加 AND 条件。 */
    public EntityDmlUpdateOperator<T> and(EntityProperty<T, ?> property, String operator, Object value) { command.state().where().term(property, operator, value); return this; }
    /** 添加 IS NULL 条件。 */
    public EntityDmlUpdateOperator<T> isNull(EntityProperty<T, ?> property) { command.state().where().isNull(property); return this; }
    /** 添加 IS NOT NULL 条件。 */
    public EntityDmlUpdateOperator<T> isNotNull(EntityProperty<T, ?> property) { command.state().where().isNotNull(property); return this; }
    /** 添加 IN 集合条件。 */
    public EntityDmlUpdateOperator<T> in(EntityProperty<T, ?> property, Iterable<?> values) { command.state().where().in(property, values); return this; }
    /** 添加 NOT IN 集合条件。 */
    public EntityDmlUpdateOperator<T> notIn(EntityProperty<T, ?> property, Iterable<?> values) { command.state().where().notIn(property, values); return this; }
    /** 添加闭区间 BETWEEN 条件。 */
    public EntityDmlUpdateOperator<T> between(EntityProperty<T, ?> property, Object start, Object end) { command.state().where().between(property, start, end); return this; }
    /** 追加括号包裹的 OR 条件组。 */
    public EntityDmlUpdateOperator<T> or(Consumer<EntityCondition<T>> consumer) { command.state().where().or(consumer); return this; }
    /** 追加括号包裹的 AND 条件组。 */
    public EntityDmlUpdateOperator<T> andGroup(Consumer<EntityCondition<T>> consumer) { command.state().where().andGroup(consumer); return this; }
    /** 为本次更新叠加显式数据范围。 */
    public EntityDmlUpdateOperator<T> scope(DataScope scope) { command.state().scope(scope); return this; }
    /** 使用实体版本字段执行原子比较并递增。 */
    public EntityDmlUpdateOperator<T> optimisticLock(Object expectedVersion) { command.optimisticLock(expectedVersion); return this; }
    /** 使用客户端默认保护提交更新。 */
    public Mono<Long> execute() { return client.update(command.spec()); }
    /** 使用本次显式执行保护提交更新。 */
    public Mono<Long> execute(SqlExecutionOptions options) { return client.update(command.spec().withExecutionOptions(options)); }

    EntityUpdateCommand<T> command() { return command; }
}
