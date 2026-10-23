package com.flying.orm.rdb.operator;

import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.lock.OptimisticLockOptions;

/**
 * 实体删除的执行方式无关计划。
 *
 * <p>默认删除仍交给表单定义决定是否逻辑删除；{@link #physical()} 只改变执行时选择的删除入口，
 * 不会绕过 where、Scope 或乐观锁条件。</p>
 *
 * @param <T> 实体类型
 * @param <S> 同步或响应式删除终端
 * @author wangr
 * @version v1.0
 */
public abstract class EntityDeleteCommand<T, S extends EntityDeleteCommand<T, S>>
        extends EntityCommand<T, S> {

    private OptimisticLockOptions lock;
    private boolean physical;

    EntityDeleteCommand(EntityDmlModel<T> model, SqlRenderer renderer) {
        super(model, renderer);
    }

    /** 用实体版本字段校验期望版本。 */
    public S optimisticLock(Object expectedVersion) {
        lock = incrementVersion(expectedVersion);
        return self();
    }

    /** 显式使用物理删除，保留条件、Scope 和版本校验。 */
    public S physical() {
        physical = true;
        return self();
    }

    boolean physicalDelete() {
        return physical;
    }

    WriteSpec spec() {
        WriteSpec spec = WriteSpec.delete(form(), where().build()).withScope(scope());
        return lock == null ? spec : spec.withLock(lock);
    }
}
