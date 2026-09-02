package com.flying.orm.rdb.mapping;

/**
 * 为声明了自动填充的实体字段提供值。
 *
 * <p>填充器属于单套 ORM 客户端，必须可以被 JDBC、R2DBC 和批量写入并发调用。返回值只进入本次参数快照，
 * 不通过反射强改实体；需要实体本身同步值时，填充器可以在应用明确允许的情况下自行处理。</p>
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
