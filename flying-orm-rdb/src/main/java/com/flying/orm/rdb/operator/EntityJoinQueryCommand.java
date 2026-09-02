package com.flying.orm.rdb.operator;

import com.flying.orm.core.lambda.EntityProperty;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.internal.mapping.EntityPropertyResolver;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.EntityModelRegistry;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 把实体 getter 解析为 DynamicForm JOIN 命令的共享适配层。
 *
 * @param <R> 根实体类型
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
final class EntityJoinQueryCommand<R> {

    private final EntityModelRegistry models;
    private final EntityMetadata<R> root;
    private final JoinQueryCommand dynamic;
    private final Map<Class<?>, EntityMetadata<?>> joined = new LinkedHashMap<>();
    private EntityMetadata<?> lastJoined;

    EntityJoinQueryCommand(EntityModelRegistry models, SqlRenderer renderer, Class<R> rootType) {
        this.models = Objects.requireNonNull(models, "entity model registry must not be null");
        this.root = this.models.metadata(Objects.requireNonNull(rootType, "join root entity type must not be null"));
        this.dynamic = new JoinQueryCommand(root.toDynamicForm(), renderer);
        joined.put(root.type(), root);
    }

    <J> EntityJoinQueryCommand<R> join(com.flying.orm.core.join.JoinType type,
                                      Class<J> joinedType,
                                      EntityProperty<R, ?> left,
                                      EntityProperty<J, ?> right) {
        EntityMetadata<J> rightMetadata = models.metadata(Objects.requireNonNull(
                joinedType, "joined entity type must not be null"));
        if (joined.containsKey(rightMetadata.type())) {
            throw new IllegalArgumentException("join entity source must not be duplicated");
        }
        dynamic.join(type,
                     rightMetadata.toDynamicForm(),
                     EntityPropertyResolver.column(root, left),
                     EntityPropertyResolver.column(rightMetadata, right));
        joined.put(rightMetadata.type(), rightMetadata);
        lastJoined = rightMetadata;
        return this;
    }

    <J> EntityJoinQueryCommand<R> andOn(EntityProperty<R, ?> left, EntityProperty<J, ?> right) {
        if (lastJoined == null) {
            throw new IllegalStateException("join ON extension requires a preceding join");
        }
        dynamic.andOn(EntityPropertyResolver.column(root, left), column(lastJoined, right));
        return this;
    }

    <S> EntityJoinQueryCommand<R> select(Class<S> type, EntityProperty<S, ?> property, String alias) {
        EntityMetadata<S> metadata = metadata(type);
        String field = EntityPropertyResolver.column(metadata, property);
        if (alias == null) {
            dynamic.select(metadata.toDynamicForm(), field);
        } else {
            dynamic.selectAs(metadata.toDynamicForm(), field, alias);
        }
        return this;
    }

    <S> EntityJoinQueryCommand<R> where(Class<S> type,
                                       EntityProperty<S, ?> property,
                                       String operator,
                                       Object value) {
        EntityMetadata<S> metadata = metadata(type);
        dynamic.where(metadata.toDynamicForm(), EntityPropertyResolver.column(metadata, property), operator, value);
        return this;
    }

    <S> EntityJoinQueryCommand<R> scope(Class<S> type, DataScope scope) {
        EntityMetadata<S> metadata = metadata(type);
        dynamic.scope(metadata.toDynamicForm(), scope);
        return this;
    }

    <S> EntityJoinQueryCommand<R> orderBy(Class<S> type,
                                         EntityProperty<S, ?> property,
                                         PageSort.Direction direction) {
        EntityMetadata<S> metadata = metadata(type);
        dynamic.orderBy(metadata.toDynamicForm(), EntityPropertyResolver.column(metadata, property), direction);
        return this;
    }

    EntityJoinQueryCommand<R> declaredDisplay() {
        dynamic.declaredDisplay();
        return this;
    }

    EntityJoinQueryCommand<R> masked() {
        dynamic.masked();
        return this;
    }

    EntityJoinQueryCommand<R> showSensitive() {
        dynamic.showSensitive();
        return this;
    }

    com.flying.orm.core.join.JoinQuerySpec spec() {
        return dynamic.spec();
    }

    @SuppressWarnings("unchecked")
    private <S> EntityMetadata<S> metadata(Class<S> type) {
        EntityMetadata<?> metadata = joined.get(Objects.requireNonNull(type, "join entity type must not be null"));
        if (metadata == null) {
            throw new IllegalArgumentException("join entity source is not part of the query");
        }
        return (EntityMetadata<S>) metadata;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private static String column(EntityMetadata<?> metadata, EntityProperty<?, ?> property) {
        return EntityPropertyResolver.column((EntityMetadata) metadata, (EntityProperty) property);
    }
}
