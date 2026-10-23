package com.flying.orm.rdb.mapping;

import java.util.Map;
import java.util.function.Supplier;

/**
 * 为声明了自动填充的实体字段提供值。
 *
 * <p>填充器属于单套 ORM 客户端，必须可以被 JDBC、R2DBC 和批量写入并发调用。返回值只进入本次参数快照，
 * 默认不修改实体；声明 {@code @TableField(fillBack = true)} 时会在 SQL 参数准备阶段回填 Java 值。
 * 回填不是数据库执行成功的凭据，执行失败也不会撤销应用对象的变化。</p>
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
@FunctionalInterface
public interface EntityFieldFiller {

    /**
     * @param entity 当前实体
     * @param field 当前字段的不可变元数据
     * @param operation 本次写入方式
     * @param currentValue 实体当前值
     * @return 本次 SQL 参数使用的值
     */
    Object fill(Object entity, EntityFieldMetadata field, Operation operation, Object currentValue);

    /**
     * 按 Java 属性名配置填充函数，填充时机仍由字段注解决定。
     * 注册表会复制；函数只在实际填充时调用，须支持并发调用，可以返回 null。
     * 同名属性共用函数；需要区分实体、操作或当前值时，直接实现 {@link #fill}。
     *
     * @param suppliers 属性名到取值函数的映射，不是数据库列名
     * @return 属性未注册时拒绝填充的填充器
     */
    static EntityFieldFiller byProperty(Map<String, ? extends Supplier<?>> suppliers) {
        Map<String, ? extends Supplier<?>> values = Map.copyOf(suppliers);
        return (entity, field, operation, currentValue) -> {
            Supplier<?> supplier = values.get(field.name());
            if (supplier == null) {
                throw new IllegalStateException("no field value supplier registered: " + field.name());
            }
            return supplier.get();
        };
    }

    /** 没有配置填充器时使用拒绝实现，避免注解看似生效、实际悄悄写入空值。 */
    static EntityFieldFiller none() {
        return (entity, field, operation, currentValue) -> {
            throw new IllegalStateException("field fill needs an explicitly configured EntityFieldFiller: "
                                                    + entity.getClass().getName() + "." + field.name());
        };
    }

    enum Operation {
        INSERT,
        UPDATE,
        UPSERT
    }
}
