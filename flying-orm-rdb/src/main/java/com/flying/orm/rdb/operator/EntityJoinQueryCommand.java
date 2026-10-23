package com.flying.orm.rdb.operator;

import com.flying.orm.core.condition.TermRegistry;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.join.JoinCondition;
import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.lambda.EntityProperty;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.internal.mapping.EntityPropertyResolver;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.internal.value.ExpressionFieldValue;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.rdb.aggregate.AggregateSpec;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.BiFunction;
import java.util.function.Consumer;

/**
 * 把实体 getter 解析为 DynamicForm JOIN 命令的共享适配层。
 *
 * @param <R> 根实体类型
 * @param <Q> 具体查询类型
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public abstract class EntityJoinQueryCommand<R, Q extends EntityJoinQueryCommand<R, Q>>
        extends JoinQueryCommand<Q> {

    private final EntityModelRegistry models;
    private final EntityMetadata<R> root;
    private final TermRegistry terms;
    private final SqlRenderer renderer;
    private final Map<Class<?>, EntityMetadata<?>> joined = new LinkedHashMap<>();
    private EntityMetadata<?> lastJoined;

    EntityJoinQueryCommand(EntityModelRegistry models, SqlRenderer renderer, Class<R> rootType) {
        this(Objects.requireNonNull(models, "entity model registry must not be null"), renderer,
                models.metadata(Objects.requireNonNull(rootType, "join root entity type must not be null")));
    }

    private EntityJoinQueryCommand(EntityModelRegistry models, SqlRenderer renderer, EntityMetadata<R> root) {
        super(root.toDynamicForm(), renderer);
        this.models = models;
        this.root = root;
        this.renderer = renderer;
        this.terms = renderer.terms();
        joined.put(root.type(), root);
    }

    <J> Q join(JoinType type,
            Class<J> joinedType,
            EntityProperty<R, ?> left,
            EntityProperty<J, ?> right) {
        EntityMetadata<J> rightMetadata = models.metadata(Objects.requireNonNull(
                joinedType, "joined entity type must not be null"));
        if (joined.containsKey(rightMetadata.type())) {
            throw new IllegalArgumentException("join entity class is ambiguous; "
                    + "use JoinQuerySpec with explicit JoinSource references for self joins");
        }
        super.join(type,
                     rightMetadata.toDynamicForm(),
                     EntityPropertyResolver.column(root, left),
                     EntityPropertyResolver.column(rightMetadata, right));
        joined.put(rightMetadata.type(), rightMetadata);
        lastJoined = rightMetadata;
        return self();
    }

    /** 为最近加入的实体追加等值 ON 条件。 */
    public <J> Q andOn(EntityProperty<R, ?> left, EntityProperty<J, ?> right) {
        if (lastJoined == null) {
            throw new IllegalStateException("join ON extension requires a preceding join");
        }
        super.andOn(EntityPropertyResolver.column(root, left), column(lastJoined, right));
        return self();
    }

    <J> Q join(JoinType type, Class<J> joinedType,
            BiFunction<EntityJoinSource<R>, EntityJoinSource<J>, JoinCondition> on) {
        Objects.requireNonNull(on, "join ON factory must not be null");
        EntityMetadata<J> metadata = models.metadata(Objects.requireNonNull(joinedType, "joined entity type must not be null"));
        if (joined.containsKey(metadata.type())) {
            throw new IllegalArgumentException("join entity class is ambiguous; "
                    + "use JoinQuerySpec with explicit JoinSource references for self joins");
        }
        super.join(type, metadata.toDynamicForm(), (left, right) -> on.apply(
                new EntityJoinSource<>(left, root, models, renderer),
                new EntityJoinSource<>(right, metadata, models, renderer)));
        joined.put(metadata.type(), metadata);
        lastJoined = metadata;
        return self();
    }

    private <S> Q selectField(Class<S> type, EntityProperty<S, ?> property, String alias) {
        EntityMetadata<S> metadata = metadata(type);
        String field = EntityPropertyResolver.column(metadata, property);
        if (alias == null) {
            super.select(metadata.toDynamicForm(), field);
        } else {
            super.selectAs(metadata.toDynamicForm(), field, alias);
        }
        return self();
    }

    /** 引用已加入查询的实体字段。 */
    public <S> ValueExpression field(Class<S> type, EntityProperty<S, ?> property) {
        EntityMetadata<S> metadata = metadata(type);
        return super.field(metadata.toDynamicForm(), EntityPropertyResolver.column(metadata, property));
    }

    /** 创建保留实体值映射的优先级表达式。 */
    public <S> ValueExpression priority(Class<S> type, EntityProperty<S, ?> property, Object... values) {
        EntityMetadata<S> metadata = metadata(type);
        String column = EntityPropertyResolver.column(metadata, property);
        if (values.length == 0) throw new IllegalArgumentException("priority requires at least one preferred value");
        ValueExpression[] arguments = new ValueExpression[values.length + 1];
        arguments[0] = super.field(metadata.toDynamicForm(), column);
        DynamicField field = metadata.toDynamicForm().field(column);
        EntityValues<S> entityValues = models.entityValues(type);
        for (int index = 0; index < values.length; index++) {
            arguments[index + 1] = ValueExpression.value(new ExpressionFieldValue(
                    field, entityValues.normalizeWriteValue(column, values[index])));
        }
        return ValueExpression.call("priority", arguments);
    }

    /** 按实体映射追加参数化条件。 */
    public <S> Q where(Class<S> type,
            EntityProperty<S, ?> property,
            String operator,
            Object value) {
        EntityMetadata<S> metadata = metadata(type);
        String field = EntityPropertyResolver.column(metadata, property);
        Object normalized = metadata.hasEnumConditionValues()
                ? models.entityValues(type).normalizeConditionValue(field, operator, value, terms)
                : value;
        super.where(metadata.toDynamicForm(), field, operator, normalized);
        return self();
    }

    /** 为指定实体追加数据范围。 */
    public <S> Q scope(Class<S> type, DataScope scope) {
        EntityMetadata<S> metadata = metadata(type);
        super.scope(metadata.toDynamicForm(), scope);
        return self();
    }

    /** 按指定实体字段排序。 */
    public <S> Q orderBy(Class<S> type,
            EntityProperty<S, ?> property,
            PageSort.Direction direction) {
        EntityMetadata<S> metadata = metadata(type);
        super.orderBy(metadata.toDynamicForm(), EntityPropertyResolver.column(metadata, property), direction);
        return self();
    }

    /** 使用来源明确的复杂 ON 追加 INNER JOIN。 */
    public <J> Q join(Class<J> type,
            BiFunction<EntityJoinSource<R>, EntityJoinSource<J>, JoinCondition> on) {
        return join(JoinType.INNER, type, on);
    }

    /** 使用来源明确的复杂 ON 追加 LEFT JOIN。 */
    public <J> Q leftJoin(Class<J> type,
            BiFunction<EntityJoinSource<R>, EntityJoinSource<J>, JoinCondition> on) {
        return join(JoinType.LEFT, type, on);
    }

    /** 使用来源明确的复杂 ON 追加 RIGHT JOIN。 */
    public <J> Q rightJoin(Class<J> type,
            BiFunction<EntityJoinSource<R>, EntityJoinSource<J>, JoinCondition> on) {
        return join(JoinType.RIGHT, type, on);
    }

    /** 使用来源明确的复杂 ON 追加 FULL OUTER JOIN；需方言声明支持。 */
    public <J> Q fullJoin(Class<J> type,
            BiFunction<EntityJoinSource<R>, EntityJoinSource<J>, JoinCondition> on) {
        return join(JoinType.FULL, type, on);
    }

    /** 追加 FULL OUTER JOIN；需方言声明支持。 */
    public <J> Q fullJoin(Class<J> type,
            EntityProperty<R, ?> left, EntityProperty<J, ?> right) {
        return join(JoinType.FULL, type, left, right);
    }

    /** 追加 INNER JOIN。 */
    public <J> Q join(Class<J> type,
            EntityProperty<R, ?> left,
            EntityProperty<J, ?> right) {
        return join(JoinType.INNER, type, left, right);
    }

    /** 追加 LEFT OUTER JOIN。 */
    public <J> Q leftJoin(Class<J> type,
            EntityProperty<R, ?> left,
            EntityProperty<J, ?> right) {
        return join(JoinType.LEFT, type, left, right);
    }

    /** 追加 RIGHT OUTER JOIN。 */
    public <J> Q rightJoin(Class<J> type,
            EntityProperty<R, ?> left,
            EntityProperty<J, ?> right) {
        return join(JoinType.RIGHT, type, left, right);
    }

    /** 使用稳定默认别名选择实体字段。 */
    public <S> Q select(Class<S> type, EntityProperty<S, ?> property) {
        return selectField(type, property, null);
    }

    /** 使用显式结果别名选择实体字段。 */
    public <S> Q selectAs(Class<S> type,
            EntityProperty<S, ?> property,
            String alias) {
        return selectField(type, property, alias);
    }

    public <S> Q orderByFirst(Class<S> type, EntityProperty<S, ?> property, Object... values) {
        return orderBy(priority(type, property, values), PageSort.Direction.ASC);
    }

    public <S> Q orderByLast(Class<S> type, EntityProperty<S, ?> property, Object... values) {
        return orderBy(ValueExpression.lastMatch(priority(type, property, values)), PageSort.Direction.ASC);
    }

    public <S> Q orderByAsc(Class<S> type, EntityProperty<S, ?> property) {
        return orderBy(type, property, PageSort.Direction.ASC);
    }

    /** 追加降序字段。 */
    public <S> Q orderByDesc(Class<S> type, EntityProperty<S, ?> property) {
        return orderBy(type, property, PageSort.Direction.DESC);
    }

    AggregateSpec entityAggregateSpec(Consumer<EntityAggregate> consumer) {
        Objects.requireNonNull(consumer, "aggregate configuration must not be null");
        return super.aggregateSpec(report -> {
            EntityAggregate aggregate = new EntityAggregate(report, joined, true, models::entityValues);
            consumer.accept(aggregate);
            aggregate.finish();
        });
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
