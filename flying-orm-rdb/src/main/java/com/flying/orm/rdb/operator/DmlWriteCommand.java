package com.flying.orm.rdb.operator;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.LogicDeleteDefinition;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlIdentifiers;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.internal.condition.ConditionNodes;
import com.flying.orm.rdb.lock.OptimisticLockOptions;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
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
public abstract class DmlWriteCommand<S extends DmlWriteCommand<S>> implements ConditionDsl<String, S> {

    private final SqlRenderer renderer;
    private final String table;
    private final DynamicForm boundForm;

    private WhereDsl where;
    private OptimisticLockOptions lock;
    private LogicDeleteDefinition logicDelete;
    private DataScope scope = DataScope.none();

    DmlWriteCommand(SqlRenderer renderer, String table, DynamicForm form) {
        this.renderer = Objects.requireNonNull(renderer, "sql renderer must not be null");
        this.table = SqlIdentifiers.requireIdentifier(table);
        this.boundForm = form;
        this.where = new WhereDsl(renderer);
    }

    protected abstract S self();

    abstract WriteSpec writeSpec(DynamicForm form, ConditionGroup conditions);

    Map<String, Object> values() {
        return Map.of();
    }

    /** 构建并以 AND 追加一组条件，采用函数返回的 DSL。 */
    public S where(Function<WhereDsl, WhereDsl> customizer) {
        where(conditions(customizer));
        return self();
    }

    /** 以 AND 追加已构建的条件；OR 组作为整体保留，Scope 与字段治理仍由原执行链处理。 */
    public S where(ConditionGroup conditions) {
        where.append(Objects.requireNonNull(conditions, "where conditions must not be null"));
        return self();
    }

    /** 显式替换业务 where；Scope、逻辑删除和乐观锁约束独立保留。 */
    public S replaceWhere(Function<WhereDsl, WhereDsl> customizer) {
        this.where = new WhereDsl(renderer, conditions(customizer));
        return self();
    }

    private ConditionGroup conditions(Function<WhereDsl, WhereDsl> customizer) {
        WhereDsl dsl = new WhereDsl(renderer);
        return Objects.requireNonNull(customizer, "where customizer must not be null")
                            .apply(dsl)
                            .build();
    }

    /** 追加一个参数化条件。 */
    @Override
    public S where(String field, String operator, Object value) {
        where.where(field, operator, value);
        return self();
    }

    @Override
    public S whereIfPresent(String field, String operator, Object value) {
        where.whereIfPresent(field, operator, value);
        return self();
    }

    /** 显式开启乐观锁。 */
    public S optimisticLock(OptimisticLockOptions lock) {
        this.lock = Objects.requireNonNull(lock, "optimistic lock options must not be null");
        return self();
    }

    /** 声明逻辑删除字段及业务值。 */
    public S logicDelete(String fieldName, Object notDeletedValue, Object deletedValue) {
        if (boundForm != null) {
            throw new IllegalStateException("configure logic delete on the bound DynamicForm");
        }
        this.logicDelete = LogicDeleteDefinition.of(
                SqlIdentifiers.requireIdentifier(fieldName),
                notDeletedValue,
                deletedValue);
        return self();
    }

    /** 追加本次数据范围，只会继续收紧默认保护。 */
    public S scope(DataScope scope) {
        this.scope = this.scope.and(Objects.requireNonNull(scope, "data scope must not be null"));
        return self();
    }

    /** 使用常见的 0/1 逻辑删除约定。 */
    public S logicDelete(String fieldName) {
        return logicDelete(fieldName, 0, 1);
    }

    /** 生成交给 FormClient 的不可变写入描述，真正安全校验仍只在统一 Form 计划器中执行一次。 */
    WriteSpec spec() {
        ConditionGroup conditions = where.build();
        DynamicForm form = form(conditions);
        WriteSpec spec = writeSpec(form, conditions).withScope(scope);
        return lock == null ? spec : spec.withLock(lock);
    }

    /**
     * 用本次命令真正出现的字段构造临时表单。
     *
     * <p>表名、赋值字段、条件字段和逻辑删除字段已经分别在进入命令时完成 SQL 标识符校验；这里只对
     * 来自独立 {@link OptimisticLockOptions} 的版本字段补齐同一边界校验，随后保留声明顺序生成表单。</p>
     */
    private DynamicForm form(ConditionGroup conditions) {
        if (boundForm != null) {
            return boundForm;
        }
        Map<String, Object> values = values();
        Set<FieldIdentity> names = new LinkedHashSet<>();
        values.keySet().forEach(name -> names.add(FieldIdentity.of(name)));
        for (Object value : values.values()) {
            if (value instanceof ValueExpression expression) {
                expression.forEachField(field -> names.add(FieldIdentity.of(
                        SqlIdentifiers.requireIdentifier(field.identity().name()))));
            }
        }
        ConditionNodes.forEachTerm(conditions, term -> names.add(FieldIdentity.of(term.field())));
        if (lock != null) {
            names.add(FieldIdentity.of(SqlIdentifiers.requireIdentifier(lock.field())));
        }
        if (logicDelete != null) {
            names.add(FieldIdentity.of(logicDelete.fieldName()));
        }

        DynamicForm.Builder builder = DynamicForm.builder(table, table);
        for (FieldIdentity name : names) {
            builder.addField(DynamicField.of(name.name(), "OTHER"));
        }
        if (logicDelete != null) {
            builder.logicDelete(
                    logicDelete.fieldName(), logicDelete.notDeletedValue(), logicDelete.deletedValue());
        }
        return builder.build();
    }

    /** 更新配置；只有更新命令持有赋值集合。 */
    public abstract static class Update<S extends Update<S>> extends DmlWriteCommand<S> {
        private final Map<String, Object> assignments = new LinkedHashMap<>();

        Update(SqlRenderer renderer, String table, DynamicForm form) {
            super(renderer, table, form);
        }

        /** 设置一个字段，保留 null；同名字段后设置的值覆盖前值。 */
        public S set(String field, Object value) {
            assignments.put(SqlIdentifiers.requireIdentifier(field), value);
            return self();
        }

        /** 按 Map 顺序追加赋值，不保留调用方 Map。 */
        public S set(Map<String, ?> values) {
            Objects.requireNonNull(values, "update values must not be null").forEach(this::set);
            return self();
        }

        /** 使用参数化数据库表达式赋值。 */
        public S setExpression(String field, ValueExpression expression) {
            return set(field, Objects.requireNonNull(expression, "SET expression must not be null"));
        }

        @Override
        Map<String, Object> values() {
            return assignments;
        }

        @Override
        WriteSpec writeSpec(DynamicForm form, ConditionGroup conditions) {
            return WriteSpec.update(form, assignments, conditions);
        }
    }

    /** 删除配置；物理删除不改变原数据范围和乐观锁语义。 */
    public abstract static class Delete<S extends Delete<S>> extends DmlWriteCommand<S> {
        private boolean physical;

        Delete(SqlRenderer renderer, String table, DynamicForm form) {
            super(renderer, table, form);
        }

        /** 显式使用物理 DELETE。 */
        public S physical() {
            physical = true;
            return self();
        }

        boolean physicalDelete() {
            return physical;
        }

        @Override
        WriteSpec writeSpec(DynamicForm form, ConditionGroup conditions) {
            return WriteSpec.delete(form, conditions);
        }
    }
}
