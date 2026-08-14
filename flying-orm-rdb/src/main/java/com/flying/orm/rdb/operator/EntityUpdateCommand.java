package com.flying.orm.rdb.operator;

import com.flying.orm.core.lambda.EntityProperty;
import com.flying.orm.rdb.form.UpdateDelta;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.mapping.MappingException;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 实体更新的执行方式无关计划。
 *
 * <p>普通赋值、原子增减和乐观锁都在这里校验并生成 {@link WriteSpec}。执行器只能接到已经通过
 * 元数据校验的列名和参数值，避免同步和响应式路径在“字段能否写入”这一点上出现分叉。</p>
 *
 * @param <T> 实体类型
 */
final class EntityUpdateCommand<T> {

    private final EntityCommandState<T> state;
    private final Map<String, Object> values = new LinkedHashMap<>();
    private OptimisticLockOptions lock;

    EntityUpdateCommand(EntityCommandState<T> state) {
        this.state = Objects.requireNonNull(state, "entity command state must not be null");
    }

    EntityCommandState<T> state() {
        return state;
    }

    void set(EntityProperty<T, ?> property, Object value) {
        values.put(writableColumn(property, "a normal update"), value);
    }

    void increment(EntityProperty<T, ? extends Number> property, Number amount) {
        arithmetic(property, UpdateDelta.increment(amount));
    }

    void decrement(EntityProperty<T, ? extends Number> property, Number amount) {
        arithmetic(property, UpdateDelta.decrement(amount));
    }

    void optimisticLock(Object expectedVersion) {
        lock = EntityOptimisticLocks.increment(state.metadata(), expectedVersion);
    }

    WriteSpec spec() {
        WriteSpec spec = WriteSpec.update(state.form(), values, state.where().build()).withScope(state.scope());
        return lock == null ? spec : spec.withLock(lock);
    }

    private void arithmetic(EntityProperty<T, ? extends Number> property, UpdateDelta delta) {
        values.put(writableColumn(property, "an arithmetic update"), delta);
    }

    private String writableColumn(EntityProperty<T, ?> property, String operation) {
        String column = state.where().column(property);
        EntityFieldMetadata field = state.metadata().field(column);
        if (!field.updatable() || field.version() || field.logicDelete()) {
            throw new MappingException("entity field is not writable by " + operation + ": "
                                               + state.metadata().type().getName() + "." + field.name());
        }
        return column;
    }
}
