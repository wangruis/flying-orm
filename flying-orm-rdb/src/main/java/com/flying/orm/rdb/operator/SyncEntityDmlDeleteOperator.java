package com.flying.orm.rdb.operator;

import com.flying.orm.core.lambda.EntityProperty;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.form.spec.WriteSpec;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * 实体 Lambda 删除的同步命令。
 *
 * <p>默认逻辑删除与显式物理删除使用同一份条件、Scope 和乐观锁计划；区别只在最后调用同步客户端的
 * delete 还是 physicalDelete。这样物理删除不会变成绕过数据范围的特殊通道。</p>
 *
 * @param <T> 实体类型
 * @author wangr
 * @version v2.0.0
 */
public final class SyncEntityDmlDeleteOperator<T> {

    private final SyncFormClient client;
    private final EntityDeleteCommand<T> command;

    SyncEntityDmlDeleteOperator(SyncFormClient client, EntityDeleteCommand<T> command) {
        this.client = Objects.requireNonNull(client, "sync form client must not be null");
        this.command = Objects.requireNonNull(command, "entity delete command must not be null");
    }

    /** 添加严格等值条件。 */
    public SyncEntityDmlDeleteOperator<T> where(EntityProperty<T, ?> property, Object value) { command.state().where().equal(property, value); return this; }
    /** 继续追加严格 AND 等值条件。 */
    public SyncEntityDmlDeleteOperator<T> and(EntityProperty<T, ?> property, Object value) { return where(property, value); }
    /** 使用已注册条件运算符追加 AND 条件。 */
    public SyncEntityDmlDeleteOperator<T> and(EntityProperty<T, ?> property, String operator, Object value) { command.state().where().term(property, operator, value); return this; }
    /** 添加 IS NULL 条件。 */
    public SyncEntityDmlDeleteOperator<T> isNull(EntityProperty<T, ?> property) { command.state().where().isNull(property); return this; }
    /** 添加 IS NOT NULL 条件。 */
    public SyncEntityDmlDeleteOperator<T> isNotNull(EntityProperty<T, ?> property) { command.state().where().isNotNull(property); return this; }
    /** 添加 IN 集合条件。 */
    public SyncEntityDmlDeleteOperator<T> in(EntityProperty<T, ?> property, Iterable<?> values) { command.state().where().in(property, values); return this; }
    /** 添加 NOT IN 集合条件。 */
    public SyncEntityDmlDeleteOperator<T> notIn(EntityProperty<T, ?> property, Iterable<?> values) { command.state().where().notIn(property, values); return this; }
    /** 添加闭区间 BETWEEN 条件。 */
    public SyncEntityDmlDeleteOperator<T> between(EntityProperty<T, ?> property, Object start, Object end) { command.state().where().between(property, start, end); return this; }
    /** 追加括号包裹的 OR 条件组。 */
    public SyncEntityDmlDeleteOperator<T> or(Consumer<EntityCondition<T>> consumer) { command.state().where().or(consumer); return this; }
    /** 追加括号包裹的 AND 条件组。 */
    public SyncEntityDmlDeleteOperator<T> andGroup(Consumer<EntityCondition<T>> consumer) { command.state().where().andGroup(consumer); return this; }
    /** 叠加显式数据范围。 */
    public SyncEntityDmlDeleteOperator<T> scope(DataScope scope) { command.state().scope(scope); return this; }
    /** 使用实体版本字段校验期望版本。 */
    public SyncEntityDmlDeleteOperator<T> optimisticLock(Object expectedVersion) { command.optimisticLock(expectedVersion); return this; }
    /** 显式切换到物理删除。 */
    public SyncEntityDmlDeleteOperator<T> physical() { command.physical(); return this; }
    /** 执行删除并返回影响行数。 */
    public long execute() { return write(command.spec()); }
    /** 使用本次执行保护执行删除。 */
    public long execute(SqlExecutionOptions options) { return write(command.spec().withExecutionOptions(options)); }

    private long write(WriteSpec spec) {
        return command.physicalDelete() ? client.physicalDelete(spec) : client.delete(spec);
    }
}
