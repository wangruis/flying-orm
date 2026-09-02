package com.flying.orm.rdb.operator;

import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.lock.OptimisticLockOptions;

import java.util.Objects;

/**
 * 实体删除的执行方式无关计划。
 *
 * <p>默认删除仍交给表单定义决定是否逻辑删除；{@link #physical()} 只改变执行时选择的删除入口，
 * 不会绕过 where、Scope 或乐观锁条件。</p>
 *
 * @param <T> 实体类型
 */
final class EntityDeleteCommand<T> {

    private final EntityCommandState<T> state;
    private OptimisticLockOptions lock;
    private boolean physical;

    EntityDeleteCommand(EntityCommandState<T> state) {
        this.state = Objects.requireNonNull(state, "entity command state must not be null");
    }

    EntityCommandState<T> state() {
        return state;
    }

    void optimisticLock(Object expectedVersion) {
        lock = EntityOptimisticLocks.increment(state.metadata(), expectedVersion);
    }

    void physical() {
        physical = true;
    }

    boolean physicalDelete() {
        return physical;
    }

    WriteSpec spec() {
        WriteSpec spec = WriteSpec.delete(state.form(), state.where().build()).withScope(state.scope());
        return lock == null ? spec : spec.withLock(lock);
    }
}
