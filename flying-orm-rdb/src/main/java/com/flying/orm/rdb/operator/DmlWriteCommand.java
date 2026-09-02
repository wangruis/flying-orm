package com.flying.orm.rdb.operator;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.LogicDeleteDefinition;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlIdentifiers;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.lock.OptimisticLockOptions;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * 动态更新和删除共用的命令状态。
 *
 * <p>本类只把链式调用收敛为 {@link WriteSpec}，不持有执行器。JDBC 与 R2DBC 因而共用字段校验、严格 where、
 * Scope、逻辑删除和乐观锁输入，执行方式只在最外层门面决定。命令可变，只供一次业务调用使用。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
final class DmlWriteCommand {

    private final Kind kind;
    private final SqlRenderer renderer;
    private final String table;
    private final Map<String, Object> values = new LinkedHashMap<>();

    private ConditionGroup where = ConditionGroup.and().build();
    private OptimisticLockOptions lock;
    private LogicDeleteDefinition logicDelete;
    private DataScope scope = DataScope.none();

    private DmlWriteCommand(Kind kind, SqlRenderer renderer, String table) {
        this.kind = Objects.requireNonNull(kind, "DML write kind must not be null");
        this.renderer = Objects.requireNonNull(renderer, "sql renderer must not be null");
        this.table = SqlIdentifiers.requireIdentifier(table, "operator " + kind.label + " table");
    }

    static DmlWriteCommand update(SqlRenderer renderer, String table) {
        return new DmlWriteCommand(Kind.UPDATE, renderer, table);
    }

    static DmlWriteCommand delete(SqlRenderer renderer, String table) {
        return new DmlWriteCommand(Kind.DELETE, renderer, table);
    }

    void set(String field, Object value) {
        requireKind(Kind.UPDATE);
        values.put(SqlIdentifiers.requireIdentifier(field, "operator update field"), value);
    }

    void where(Function<WhereDsl, WhereDsl> customizer) {
        WhereDsl dsl = new WhereDsl(renderer);
        this.where = Objects.requireNonNull(customizer, "where customizer must not be null")
                            .apply(dsl)
                            .build();
    }

    void optimisticLock(OptimisticLockOptions lock) {
        this.lock = Objects.requireNonNull(lock, "optimistic lock options must not be null");
    }

    void logicDelete(String fieldName, Object notDeletedValue, Object deletedValue) {
        this.logicDelete = LogicDeleteDefinition.of(
                SqlIdentifiers.requireIdentifier(fieldName, "operator logic delete field"),
                notDeletedValue,
                deletedValue);
    }

    void scope(DataScope scope) {
        this.scope = this.scope.and(Objects.requireNonNull(scope, "data scope must not be null"));
    }

    /** 生成交给 FormClient 的不可变写入描述，真正安全校验仍只在统一 Form 计划器中执行一次。 */
    WriteSpec spec() {
        WriteSpec spec;
        if (kind == Kind.UPDATE) {
            DynamicForm form = DmlFormBuilder.form(table, values.keySet(), lock, logicDelete, where);
            spec = WriteSpec.update(form, values, where).withScope(scope);
        } else {
            DynamicForm form = DmlFormBuilder.form(table, Set.of(), lock, logicDelete, where);
            spec = WriteSpec.delete(form, where).withScope(scope);
        }
        return lock == null ? spec : spec.withLock(lock);
    }

    private void requireKind(Kind expected) {
        if (kind != expected) {
            throw new IllegalStateException("cannot set values on operator " + kind.label + " command");
        }
    }

    private enum Kind {
        UPDATE("update"),
        DELETE("delete");

        private final String label;

        Kind(String label) {
            this.label = label;
        }
    }
}
