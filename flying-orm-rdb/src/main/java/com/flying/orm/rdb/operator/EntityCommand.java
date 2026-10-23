package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.lambda.EntityProperty;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.mapping.MappingException;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * 实体查询、更新和删除的共同命令所有者。
 *
 * <p>这里故意只保存执行方式无关的信息：实体表单、字段解析器、条件树和显式数据范围。它不持有
 * ReactiveFormClient、SyncFormClient、连接或订阅对象，所以同一份 SQL 计划可以被两种执行方式使用。
 * 命令对象不是线程安全的，和普通 fluent builder 一样应当一次调用链只由一个线程组装。</p>
 *
 * @param <T> 实体类型
 * @param <S> 具体命令类型
 * @author wangr
 * @version v4.1.2
 */
public abstract class EntityCommand<T, S extends EntityCommand<T, S>>
        implements ConditionDsl<EntityProperty<T, ?>, S> {

    private final EntityDmlModel<T> model;
    private final EntityCondition<T> where;
    private DataScope scope = DataScope.none();

    EntityCommand(EntityDmlModel<T> model, SqlRenderer renderer) {
        this.model = Objects.requireNonNull(model, "entity model must not be null");
        this.where = new EntityCondition<>(model.metadata(), renderer, model.entityValues());
    }

    EntityCommand(EntityCommand<T, ?> source) {
        this.model = source.model;
        this.where = source.where.copy();
        this.scope = source.scope;
    }

    EntityMetadata<T> metadata() {
        return model.metadata();
    }

    DynamicForm form() {
        return model.form();
    }

    EntityCondition<T> where() {
        return where;
    }

    Object writeValue(String fieldName, Object value) {
        EntityValues<T> values = model.entityValues();
        return values == null ? value : values.normalizeWriteValue(fieldName, value);
    }

    EntityValues<T> entityValues() {
        return model.entityValues();
    }

    /** 更新和删除共用实体版本规则；只生成本次锁选项，不保存请求版本值。 */
    OptimisticLockOptions incrementVersion(Object expectedVersion) {
        EntityFieldMetadata version = metadata().versionField().orElseThrow(() -> new MappingException(
                "entity has no @Version field: " + metadata().type().getName()));
        if (expectedVersion == null) {
            throw new MappingException("expected entity version must not be null: "
                    + metadata().type().getName() + "." + version.name());
        }
        boolean numeric = switch (version.databaseType().logicalType()) {
            case BIG_INTEGER, INTEGER, SMALL_INTEGER, DECIMAL -> true;
            default -> false;
        };
        if (!numeric) {
            throw new MappingException("entity version field cannot be incremented automatically: "
                    + metadata().type().getName() + "." + version.name() + " (" + version.dataType() + ")");
        }
        return OptimisticLockOptions.increment(version.columnName(), expectedVersion);
    }

    DataScope scope() {
        return scope;
    }

    /** 与当前数据范围取交集。 */
    public S scope(DataScope scope) {
        this.scope = this.scope.and(Objects.requireNonNull(scope, "data scope must not be null"));
        return self();
    }

    /** 追加标准或扩展条件，值沿实体映射进入参数绑定。 */
    @Override
    public S where(EntityProperty<T, ?> property, String operator, Object value) {
        where.where(property, operator, value);
        return self();
    }

    /** 仅在可选值存在时追加条件。 */
    @Override
    public S whereIfPresent(EntityProperty<T, ?> property, String operator, Object value) {
        where.whereIfPresent(property, operator, value);
        return self();
    }

    /** 查询、更新、删除按属性名或映射列名使用相同的实体条件规则。 */
    public S where(String property, String operator, Object value) {
        where.where(property, operator, value);
        return self();
    }

    public S where(String property, Object value) { return where(property, "=", value); }

    public S whereIfPresent(String property, String operator, Object value) {
        where.whereIfPresent(property, operator, value);
        return self();
    }

    public S whereIfPresent(String property, Object value) { return whereIfPresent(property, "=", value); }

    /** 追加括号包裹的 OR 条件组。 */
    public S or(Consumer<EntityCondition<T>> consumer) {
        where.or(consumer);
        return self();
    }

    /** 追加括号包裹的 AND 条件组。 */
    public S andGroup(Consumer<EntityCondition<T>> consumer) {
        where.andGroup(consumer);
        return self();
    }

    protected abstract S self();
}
