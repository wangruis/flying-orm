package com.flying.orm.rdb.operator;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.lambda.EntityProperty;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.internal.mapping.EntityPropertyResolver;

import java.util.Objects;
import java.util.List;
import java.util.function.Consumer;

/** 当前实体 DML 命令共享的严格 AND 条件累加器。 */
final class EntityWhereBuilder<T> {

    private final EntityMetadata<T> metadata;
    private final ConditionGroup.Builder builder;

    EntityWhereBuilder(EntityMetadata<T> metadata, SqlRenderer renderer) {
        this.metadata = Objects.requireNonNull(metadata, "entity metadata must not be null");
        this.builder = Objects.requireNonNull(renderer, "sql renderer must not be null").conditions();
    }

    void equal(EntityProperty<T, ?> property, Object value) {
        builder.where(column(property), "=", value);
    }

    void term(EntityProperty<T, ?> property, String operator, Object value) {
        builder.where(column(property), Objects.requireNonNull(operator, "condition operator must not be null"), value);
    }

    void isNull(EntityProperty<T, ?> property) {
        builder.whereNull(column(property));
    }

    void isNotNull(EntityProperty<T, ?> property) {
        builder.whereNotNull(column(property));
    }

    void in(EntityProperty<T, ?> property, Iterable<?> values) {
        builder.where(column(property), "in", values);
    }

    void notIn(EntityProperty<T, ?> property, Iterable<?> values) {
        builder.where(column(property), "not-in", values);
    }

    void between(EntityProperty<T, ?> property, Object start, Object end) {
        builder.where(column(property), "between", List.of(
                Objects.requireNonNull(start, "between start must not be null"),
                Objects.requireNonNull(end, "between end must not be null")));
    }

    void or(Consumer<EntityCondition<T>> consumer) {
        Objects.requireNonNull(consumer, "entity OR condition must not be null");
        builder.or(nested -> consumer.accept(new EntityCondition<>(metadata, nested)));
    }

    void andGroup(Consumer<EntityCondition<T>> consumer) {
        Objects.requireNonNull(consumer, "entity AND condition must not be null");
        builder.and(nested -> consumer.accept(new EntityCondition<>(metadata, nested)));
    }

    String column(EntityProperty<T, ?> property) {
        return EntityPropertyResolver.column(metadata, property);
    }

    ConditionGroup build() {
        return builder.build();
    }
}
