package com.flying.orm.rdb.operator;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.lambda.EntityProperty;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.internal.mapping.EntityPropertyResolver;
import com.flying.orm.rdb.protection.ProtectedConditions;

import java.util.List;
import java.util.Objects;

/**
 * 为实体 Lambda DML 提供嵌套条件组 DSL。实例只在一次构建回调内有效，字段会先解析为实体元数据中的列名，
 * 值仍进入统一条件归一化和参数绑定链路。调用方不能通过本类型注入 SQL 片段。
 *
 * @param <T> 实体类型
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class EntityCondition<T> {

    private final EntityMetadata<T> metadata;
    private final ConditionGroup.Builder builder;

    EntityCondition(EntityMetadata<T> metadata, ConditionGroup.Builder builder) {
        this.metadata = Objects.requireNonNull(metadata, "entity metadata must not be null");
        this.builder = Objects.requireNonNull(builder, "condition builder must not be null");
    }

    /** 添加严格等值条件。 */
    public EntityCondition<T> where(EntityProperty<T, ?> property, Object value) {
        return term(property, "=", value);
    }

    /** 追加条件；在 OR 组内，各次调用以 OR 连接，在 AND 组内以 AND 连接。 */
    public EntityCondition<T> and(EntityProperty<T, ?> property, String operator, Object value) {
        return term(property, operator, value);
    }

    /** 添加自定义或标准 term，字段和值均不作为原始 SQL。 */
    public EntityCondition<T> term(EntityProperty<T, ?> property, String operator, Object value) {
        builder.where(column(property), Objects.requireNonNull(operator, "condition operator must not be null"), value);
        return this;
    }

    /** 添加 IN 集合条件。 */
    public EntityCondition<T> in(EntityProperty<T, ?> property, Iterable<?> values) {
        return term(property, "in", values);
    }

    /** 添加闭区间 BETWEEN 条件。 */
    public EntityCondition<T> between(EntityProperty<T, ?> property, Object start, Object end) {
        return term(property, "between", List.of(
                Objects.requireNonNull(start, "between start must not be null"),
                Objects.requireNonNull(end, "between end must not be null")));
    }

    /** 在嵌套条件组中精确匹配声明了 EXACT 的加密字段。 */
    public EntityCondition<T> exactEncrypted(EntityProperty<T, ?> property, Object value) {
        return term(property, ProtectedConditions.EXACT, value);
    }

    /** 在嵌套条件组中后缀匹配声明了固定长度的加密字段。 */
    public EntityCondition<T> suffixEncrypted(EntityProperty<T, ?> property, Object value) {
        return term(property, ProtectedConditions.SUFFIX, value);
    }

    /** 在嵌套条件组中包含匹配声明了 CONTAINS 的加密字段。 */
    public EntityCondition<T> containsEncrypted(EntityProperty<T, ?> property, Object value) {
        return term(property, ProtectedConditions.CONTAINS, value);
    }

    /** 添加 IS NULL 条件。 */
    public EntityCondition<T> isNull(EntityProperty<T, ?> property) {
        builder.whereNull(column(property));
        return this;
    }

    /** 添加 IS NOT NULL 条件。 */
    public EntityCondition<T> isNotNull(EntityProperty<T, ?> property) {
        builder.whereNotNull(column(property));
        return this;
    }

    private String column(EntityProperty<T, ?> property) {
        return EntityPropertyResolver.column(metadata, property);
    }
}
