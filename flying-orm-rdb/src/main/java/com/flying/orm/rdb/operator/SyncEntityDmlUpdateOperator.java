package com.flying.orm.rdb.operator;

import com.flying.orm.core.lambda.EntityProperty;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.SyncFormClient;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * 实体 Lambda 更新的同步命令。
 *
 * <p>它把共享 {@link EntityUpdateCommand} 生成的 WriteSpec 直接交给同步表单客户端，
 * 不会把 JDBC 调用塞进 Reactor。</p>
 *
 * @param <T> 实体类型
 * @author wangr
 * @version v2.0.0
 */
public final class SyncEntityDmlUpdateOperator<T> {

    private final SyncFormClient client;
    private final EntityUpdateCommand<T> command;

    SyncEntityDmlUpdateOperator(SyncFormClient client, EntityUpdateCommand<T> command) {
        this.client = Objects.requireNonNull(client, "sync form client must not be null");
        this.command = Objects.requireNonNull(command, "entity update command must not be null");
    }

    /** 设置待更新属性；不可写字段、版本列和逻辑删除列会在 SQL 生成前失败。 */
    public SyncEntityDmlUpdateOperator<T> set(EntityProperty<T, ?> property, Object value) { command.set(property, value); return this; }
    /** 对数值属性执行数据库端原子增加。 */
    public SyncEntityDmlUpdateOperator<T> increment(EntityProperty<T, ? extends Number> property, Number amount) { command.increment(property, amount); return this; }
    /** 对数值属性执行数据库端原子减少。 */
    public SyncEntityDmlUpdateOperator<T> decrement(EntityProperty<T, ? extends Number> property, Number amount) { command.decrement(property, amount); return this; }
    /** 添加严格等值条件。 */
    public SyncEntityDmlUpdateOperator<T> where(EntityProperty<T, ?> property, Object value) { command.state().where().equal(property, value); return this; }
    /** 继续追加严格 AND 等值条件。 */
    public SyncEntityDmlUpdateOperator<T> and(EntityProperty<T, ?> property, Object value) { return where(property, value); }
    /** 使用已注册条件运算符追加 AND 条件。 */
    public SyncEntityDmlUpdateOperator<T> and(EntityProperty<T, ?> property, String operator, Object value) { command.state().where().term(property, operator, value); return this; }
    /** 添加 IS NULL 条件。 */
    public SyncEntityDmlUpdateOperator<T> isNull(EntityProperty<T, ?> property) { command.state().where().isNull(property); return this; }
    /** 添加 IS NOT NULL 条件。 */
    public SyncEntityDmlUpdateOperator<T> isNotNull(EntityProperty<T, ?> property) { command.state().where().isNotNull(property); return this; }
    /** 添加 IN 集合条件。 */
    public SyncEntityDmlUpdateOperator<T> in(EntityProperty<T, ?> property, Iterable<?> values) { command.state().where().in(property, values); return this; }
    /** 添加 NOT IN 集合条件。 */
    public SyncEntityDmlUpdateOperator<T> notIn(EntityProperty<T, ?> property, Iterable<?> values) { command.state().where().notIn(property, values); return this; }
    /** 添加闭区间 BETWEEN 条件。 */
    public SyncEntityDmlUpdateOperator<T> between(EntityProperty<T, ?> property, Object start, Object end) { command.state().where().between(property, start, end); return this; }
    /** 追加括号包裹的 OR 条件组。 */
    public SyncEntityDmlUpdateOperator<T> or(Consumer<EntityCondition<T>> consumer) { command.state().where().or(consumer); return this; }
    /** 追加括号包裹的 AND 条件组。 */
    public SyncEntityDmlUpdateOperator<T> andGroup(Consumer<EntityCondition<T>> consumer) { command.state().where().andGroup(consumer); return this; }
    /** 叠加显式数据范围。 */
    public SyncEntityDmlUpdateOperator<T> scope(DataScope scope) { command.state().scope(scope); return this; }
    /** 使用实体版本字段开启乐观锁。 */
    public SyncEntityDmlUpdateOperator<T> optimisticLock(Object expectedVersion) { command.optimisticLock(expectedVersion); return this; }
    /** 执行更新并返回影响行数。 */
    public long execute() { return client.update(command.spec()); }
    /** 使用本次执行保护执行更新。 */
    public long execute(SqlExecutionOptions options) { return client.update(command.spec().withExecutionOptions(options)); }
}
