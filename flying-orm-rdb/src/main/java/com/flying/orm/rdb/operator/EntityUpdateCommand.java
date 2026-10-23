package com.flying.orm.rdb.operator;

import com.flying.orm.core.lambda.EntityProperty;
import com.flying.orm.core.lambda.BoundEntityProperty;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.form.UpdateDelta;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.internal.mapping.EntityPropertyResolver;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.MappingException;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * 实体更新的执行方式无关计划。
 *
 * <p>普通赋值、原子增减和乐观锁都在这里校验并生成 {@link WriteSpec}。执行器只能接到已经通过
 * 元数据校验的列名和参数值，避免同步和响应式路径在“字段能否写入”这一点上出现分叉。</p>
 *
 * @param <T> 实体类型
 * @param <S> 同步或响应式更新终端
 * @author wangr
 * @version v1.0
 */
public abstract class EntityUpdateCommand<T, S extends EntityUpdateCommand<T, S>>
        extends EntityCommand<T, S> {

    private final EntityModelRegistry models;
    private final Map<String, Object> values = new LinkedHashMap<>();
    private OptimisticLockOptions lock;

    EntityUpdateCommand(EntityDmlModel<T> model, SqlRenderer renderer, EntityModelRegistry models) {
        super(model, renderer);
        this.models = models;
    }

    /** 按 Getter 指定属性，值使用实体持久化映射；null 显式写入。 */
    public S set(EntityProperty<T, ?> property, Object value) {
        set(where().column(property), value);
        return self();
    }

    /** 按属性名或映射列名赋值，遵守字段可写性和枚举存储规则。 */
    public S set(String property, Object value) {
        put(writableField(property, "a normal update").columnName(), value);
        return self();
    }

    /** 调用绑定 Getter 一次；执行或重复订阅不会再次取值。 */
    public S set(BoundEntityProperty<?> property) {
        String column = writableField(EntityPropertyResolver.column(metadata(), property), "a normal update").columnName();
        put(column, property.get());
        return self();
    }

    private void put(String column, Object value) {
        values.put(column, writeValue(column, value));
    }

    /** 使用数据库端表达式赋值，引用和值沿同一实体上下文映射。 */
    public S setExpression(EntityProperty<T, ?> property, Function<EntityExpressions<T>, ValueExpression> expression) {
        String column = writableField(property, "an expression update").columnName();
        ValueExpression result = Objects.requireNonNull(Objects.requireNonNull(expression,
                "entity expression must not be null").apply(new EntityExpressions<>(this)), "SET expression must not be null");
        values.put(column, result instanceof ValueExpression.Value literal
                ? ValueExpression.value(writeValue(column, literal.value())) : result);
        return self();
    }

    private void setValues(T entity, EntityProperty<T, ?>[] properties, boolean exclude) {
        Objects.requireNonNull(entity, "entity must not be null");
        Objects.requireNonNull(properties, "entity properties must not be null");
        Set<String> columns = new LinkedHashSet<>();
        if (exclude || properties.length == 0) {
            for (EntityFieldMetadata field : metadata().fields()) {
                if (entityWritable(field)) {
                    columns.add(field.columnName());
                }
            }
        }
        if (exclude) {
            for (EntityProperty<T, ?> property : properties) {
                columns.remove(where().column(property));
            }
        } else if (properties.length != 0) {
            for (EntityProperty<T, ?> property : properties) {
                EntityFieldMetadata field = writableField(property, "an entity value update");
                if (field.primaryKey()) {
                    throw new MappingException("entity primary key cannot be selected for entity SET: " + field.name());
                }
                columns.add(field.columnName());
            }
        }
        EntityValues<T> plan = entityValues();
        if (plan == null) plan = models.entityValues(metadata().type());
        values.putAll(plan.readForUpdate(entity, columns));
    }

    /**
     * 读取所选普通可更新属性作为 SET；未指定时读取全部普通可更新属性。
     * 只读取一次，沿用填充、更新策略和枚举映射，不触发生命周期。
     */
    @SafeVarargs
    public final S set(T entity, EntityProperty<T, ?>... properties) {
        setValues(entity, properties, false);
        return self();
    }

    /** 读取排除指定属性后的普通可更新属性。 */
    @SafeVarargs
    public final S setExcept(T entity, EntityProperty<T, ?>... properties) {
        setValues(entity, properties, true);
        return self();
    }

    private static boolean entityWritable(EntityFieldMetadata field) {
        return field.updatable() && !field.primaryKey() && !field.version() && !field.logicDelete();
    }

    /** 数据库端原子增加。 */
    public S increment(EntityProperty<T, ? extends Number> property, Number amount) {
        arithmetic(property, UpdateDelta.increment(amount));
        return self();
    }

    /** 数据库端原子减少。 */
    public S decrement(EntityProperty<T, ? extends Number> property, Number amount) {
        arithmetic(property, UpdateDelta.decrement(amount));
        return self();
    }

    /** 使用实体版本字段比较并递增。 */
    public S optimisticLock(Object expectedVersion) {
        lock = incrementVersion(expectedVersion);
        return self();
    }

    WriteSpec spec() {
        WriteSpec spec = WriteSpec.update(form(), values, where().build()).withScope(scope());
        return lock == null ? spec : spec.withLock(lock);
    }

    private void arithmetic(EntityProperty<T, ? extends Number> property, UpdateDelta delta) {
        values.put(writableField(property, "an arithmetic update").columnName(), delta);
    }

    private EntityFieldMetadata writableField(EntityProperty<T, ?> property, String operation) {
        return writableField(where().column(property), operation);
    }

    private EntityFieldMetadata writableField(String property, String operation) {
        EntityFieldMetadata field = metadata().field(property);
        if (!field.updatable() || field.version() || field.logicDelete()) {
            throw new MappingException("entity field is not writable by " + operation + ": "
                                               + metadata().type().getName() + "." + field.name());
        }
        return field;
    }
}
