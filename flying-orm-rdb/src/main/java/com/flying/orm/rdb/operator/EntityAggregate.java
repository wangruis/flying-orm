package com.flying.orm.rdb.operator;

import com.flying.orm.core.lambda.EntityProperty;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.page.ExpressionSort;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.rdb.aggregate.AggregateExpression;
import com.flying.orm.rdb.aggregate.AggregateFunction;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.aggregate.GroupSelection;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.internal.mapping.EntityPropertyResolver;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.internal.value.ExpressionFieldValue;
import com.flying.orm.rdb.mapping.EntityFieldMetadata;
import com.flying.orm.rdb.mapping.EntityMetadata;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;

/**
 * 实体单表与关联报表共用的配置入口，只把实体属性转换为现有聚合模型。
 *
 * <p>选择方法同时注册输出并返回类型化句柄，可交给 AggregateRow.get。分组、HAVING、
 * 排序和执行选项均由原聚合内核处理；不生成另一套 SQL，也不触发实体加载事件。</p>
 *
 * @author wangr
 * @version v4.1.2
 */
public final class EntityAggregate {

    private final AggregateSpec.Builder builder;
    private final Map<Class<?>, EntityMetadata<?>> sources;
    private final boolean joined;
    private final Function<Class<?>, EntityValues<?>> valuePlans;
    private Map<String, UnaryOperator<Object>> resultValues = Map.of();
    private List<ExpressionSort> sorts;

    EntityAggregate(AggregateSpec.Builder builder, Map<Class<?>, EntityMetadata<?>> sources, boolean joined,
                    Function<Class<?>, EntityValues<?>> valuePlans) {
        this.builder = builder;
        this.sources = sources;
        this.joined = joined;
        this.valuePlans = valuePlans;
    }

    /** 单实体报表直接引用分组属性；多实体报表使用带 Class 的重载指定来源。 */
    public <S> GroupSelection group(EntityProperty<S, ?> property, String alias) {
        return group(singleSourceType(), property, alias);
    }

    /** 单实体属性转为计算表达式，复用已有列映射。 */
    public <S> ValueExpression field(EntityProperty<S, ?> property) {
        return field(singleSourceType(), property);
    }

    /** 单实体字段非空计数。 */
    public <S> AggregateExpression<Long> count(EntityProperty<S, ?> property, String alias) {
        return count(singleSourceType(), property, alias);
    }

    /** 单实体字段去重计数。 */
    public <S> AggregateExpression<Long> countDistinct(EntityProperty<S, ?> property, String alias) {
        return countDistinct(singleSourceType(), property, alias);
    }

    /** 单实体数值字段求和。 */
    public <S> AggregateExpression<BigDecimal> sum(EntityProperty<S, ?> property, String alias) {
        return sum(singleSourceType(), property, alias);
    }

    /** 单实体数值字段平均值。 */
    public <S> AggregateExpression<BigDecimal> avg(EntityProperty<S, ?> property, String alias) {
        return avg(singleSourceType(), property, alias);
    }

    /** 单实体数值字段总体标准差。 */
    public <S> AggregateExpression<BigDecimal> stddevPop(EntityProperty<S, ?> property, String alias) {
        return stddevPop(singleSourceType(), property, alias);
    }

    /** 单实体数值字段样本标准差。 */
    public <S> AggregateExpression<BigDecimal> stddevSample(EntityProperty<S, ?> property, String alias) {
        return stddevSample(singleSourceType(), property, alias);
    }

    /** 单实体数值字段总体方差。 */
    public <S> AggregateExpression<BigDecimal> variancePop(EntityProperty<S, ?> property, String alias) {
        return variancePop(singleSourceType(), property, alias);
    }

    /** 单实体数值字段样本方差。 */
    public <S> AggregateExpression<BigDecimal> varianceSample(EntityProperty<S, ?> property, String alias) {
        return varianceSample(singleSourceType(), property, alias);
    }

    /** 单实体最小值，结果类型及 codec 沿用显式来源入口。 */
    public <S, V> AggregateExpression<V> min(EntityProperty<S, ?> property, String alias, Class<V> resultType) {
        return min(singleSourceType(), property, alias, resultType);
    }

    /** 单实体最大值，结果类型及 codec 沿用显式来源入口。 */
    public <S, V> AggregateExpression<V> max(EntityProperty<S, ?> property, String alias, Class<V> resultType) {
        return max(singleSourceType(), property, alias, resultType);
    }

    @SuppressWarnings("unchecked")
    private <S> Class<S> singleSourceType() {
        if (sources.size() != 1) {
            throw new IllegalArgumentException("multi-entity aggregate requires an explicit entity type");
        }
        // 属性所属类型仍由原有 EntityPropertyResolver 校验，不按同名字段猜测来源。
        return (Class<S>) sources.keySet().iterator().next();
    }

    /** 注册分组字段，按实体映射解析实际列名。 */
    public <S> GroupSelection group(Class<S> type, EntityProperty<S, ?> property, String alias) {
        EntityMetadata<S> metadata = metadata(type);
        GroupSelection group = GroupSelection.of(EntityPropertyResolver.column(metadata, property), alias);
        if (joined) builder.group(metadata.toDynamicForm(), group);
        else builder.group(group);
        bindResultValue(alias, metadata, group.field());
        return group;
    }

    /** 按实体字段构成的表达式分组；直接字段保留原枚举值语义。 */
    public GroupSelection group(ValueExpression operand, String alias) {
        GroupSelection group = GroupSelection.of(operand, alias);
        builder.group(group);
        if (operand instanceof ValueExpression.Field field) {
            for (EntityMetadata<?> metadata : sources.values()) {
                if (field.source() == null || field.source().form() == metadata.toDynamicForm()) {
                    bindResultValue(alias, metadata, field.identity().name());
                    break;
                }
            }
        }
        return group;
    }

    /** 统计过滤后的关系行数，包含字段为 NULL 的行。 */
    public AggregateExpression<Long> countRows(String alias) {
        AggregateExpression<Long> expression = AggregateExpression.countRows(alias);
        builder.aggregate(expression);
        return expression;
    }

    /** Getter 解析为真实列及关联来源，用于同一报表内的参数化计算。 */
    public <S> ValueExpression field(Class<S> type, EntityProperty<S, ?> property) {
        EntityMetadata<S> metadata = metadata(type);
        return builder.field(metadata.toDynamicForm(), EntityPropertyResolver.column(metadata, property));
    }

    public AggregateExpression<BigDecimal> sum(ValueExpression operand, String alias) {
        return add(AggregateExpression.sum(operand, alias));
    }

    public AggregateExpression<BigDecimal> avg(ValueExpression operand, String alias) {
        return add(AggregateExpression.avg(operand, alias));
    }

    public AggregateExpression<BigDecimal> stddevPop(ValueExpression operand, String alias) {
        return add(AggregateExpression.stddevPop(operand, alias));
    }

    public AggregateExpression<BigDecimal> stddevSample(ValueExpression operand, String alias) {
        return add(AggregateExpression.stddevSample(operand, alias));
    }

    public AggregateExpression<BigDecimal> variancePop(ValueExpression operand, String alias) {
        return add(AggregateExpression.variancePop(operand, alias));
    }

    public AggregateExpression<BigDecimal> varianceSample(ValueExpression operand, String alias) {
        return add(AggregateExpression.varianceSample(operand, alias));
    }

    public AggregateExpression<Long> count(ValueExpression operand, String alias) {
        return add(AggregateExpression.count(operand, alias));
    }

    public AggregateExpression<Long> countDistinct(ValueExpression operand, String alias) {
        return add(AggregateExpression.countDistinct(operand, alias));
    }

    public <V> AggregateExpression<V> min(ValueExpression operand, String alias,
                                          LogicalType logicalType, Class<V> resultType) {
        return add(AggregateExpression.min(operand, alias, logicalType, resultType));
    }

    public <V> AggregateExpression<V> max(ValueExpression operand, String alias,
                                          LogicalType logicalType, Class<V> resultType) {
        return add(AggregateExpression.max(operand, alias, logicalType, resultType));
    }

    /** 已注册聚合函数复用实体 Getter 来源、字段治理和类型化结果。 */
    public <V> AggregateExpression<V> function(String id, String alias, LogicalType logicalType,
                                               Class<V> resultType, ValueExpression... arguments) {
        return add(AggregateExpression.function(id, alias, logicalType, resultType, arguments));
    }

    private <V> AggregateExpression<V> add(AggregateExpression<V> expression) {
        builder.aggregate(expression);
        if (preservesFieldValue(expression) && expression.operand() instanceof ValueExpression.Field field) {
            for (EntityMetadata<?> metadata : sources.values()) {
                if (field.source() == null || field.source().form() == metadata.toDynamicForm()) {
                    bindResultValue(expression.alias(), metadata, field.identity().name());
                    break;
                }
            }
        }
        return expression;
    }

    /** 统计字段非 NULL 的行数。 */
    public <S> AggregateExpression<Long> count(Class<S> type, EntityProperty<S, ?> property, String alias) {
        return add(type, property, field -> AggregateExpression.count(field.columnName(), alias));
    }

    /** 统计字段去重后的非 NULL 值数量。 */
    public <S> AggregateExpression<Long> countDistinct(Class<S> type, EntityProperty<S, ?> property, String alias) {
        return add(type, property, field -> AggregateExpression.countDistinct(field.columnName(), alias));
    }

    /** 数值求和，返回统一的 BigDecimal。 */
    public <S> AggregateExpression<BigDecimal> sum(Class<S> type, EntityProperty<S, ?> property, String alias) {
        return add(type, property, field -> AggregateExpression.sum(field.columnName(), alias));
    }

    /** 数值平均值，返回统一的 BigDecimal。 */
    public <S> AggregateExpression<BigDecimal> avg(Class<S> type, EntityProperty<S, ?> property, String alias) {
        return add(type, property, field -> AggregateExpression.avg(field.columnName(), alias));
    }

    /** 总体标准差，按 Getter 映射数值字段。 */
    public <S> AggregateExpression<BigDecimal> stddevPop(Class<S> type, EntityProperty<S, ?> property, String alias) {
        return add(type, property, field -> AggregateExpression.stddevPop(field.columnName(), alias));
    }

    /** 样本标准差，按 Getter 映射数值字段。 */
    public <S> AggregateExpression<BigDecimal> stddevSample(Class<S> type, EntityProperty<S, ?> property, String alias) {
        return add(type, property, field -> AggregateExpression.stddevSample(field.columnName(), alias));
    }

    /** 总体方差，按 Getter 映射数值字段。 */
    public <S> AggregateExpression<BigDecimal> variancePop(Class<S> type, EntityProperty<S, ?> property, String alias) {
        return add(type, property, field -> AggregateExpression.variancePop(field.columnName(), alias));
    }

    /** 样本方差，按 Getter 映射数值字段。 */
    public <S> AggregateExpression<BigDecimal> varianceSample(Class<S> type, EntityProperty<S, ?> property, String alias) {
        return add(type, property, field -> AggregateExpression.varianceSample(field.columnName(), alias));
    }

    /** 最小值沿用字段 codec，结果类型由调用方声明并由聚合内核核对。 */
    public <S, V> AggregateExpression<V> min(Class<S> type, EntityProperty<S, ?> property,
                                            String alias, Class<V> resultType) {
        return add(type, property, field -> AggregateExpression.min(
                field.columnName(), alias, field.databaseType().logicalType(), resultType));
    }

    /** 最大值沿用字段 codec，结果类型由调用方声明并由聚合内核核对。 */
    public <S, V> AggregateExpression<V> max(Class<S> type, EntityProperty<S, ?> property,
                                            String alias, Class<V> resultType) {
        return add(type, property, field -> AggregateExpression.max(
                field.columnName(), alias, field.databaseType().logicalType(), resultType));
    }

    /** HAVING 引用本次声明的分组或聚合别名；值仍通过参数绑定。 */
    public EntityAggregate having(ConditionGroup having) {
        builder.having(having);
        return this;
    }

    /** 使用与动态报表相同的条件配置入口。 */
    public EntityAggregate having(Consumer<ConditionGroup.Builder> configuration) {
        builder.having(configuration);
        return this;
    }

    /** 按报表结果别名排序，不继承普通实体查询的字段排序。 */
    public EntityAggregate orderBy(PageSort... sorts) {
        for (PageSort sort : sorts) orderBy(ValueExpression.field(sort.field()), sort.direction());
        return this;
    }

    /** 排序表达式引用已声明的报表结果别名，不增加返回列。 */
    public EntityAggregate orderBy(ValueExpression expression, PageSort.Direction direction) {
        ExpressionSort sort = new ExpressionSort(expression, direction);
        if (sorts == null) sorts = new ArrayList<>();
        sorts.add(sort);
        return this;
    }

    /** 指定结果值依次排前；分组及直接字段 MIN/MAX 保留实体枚举映射。 */
    public EntityAggregate orderByFirst(String alias, Object... values) {
        return orderBy(ValueExpression.field(alias).priority(values), PageSort.Direction.ASC);
    }

    /** 指定结果值依次排后；字段 codec 仍由聚合内核处理。 */
    public EntityAggregate orderByLast(String alias, Object... values) {
        return orderBy(ValueExpression.lastMatch(ValueExpression.field(alias).priority(values)), PageSort.Direction.ASC);
    }

    /** 沿用现有聚合执行选项，不引入额外限制。 */
    public EntityAggregate executionOptions(SqlExecutionOptions options) {
        builder.executionOptions(options);
        return this;
    }

    private <S, V> AggregateExpression<V> add(Class<S> type, EntityProperty<S, ?> property,
                                              Function<EntityFieldMetadata, AggregateExpression<V>> factory) {
        EntityMetadata<S> metadata = metadata(type);
        String column = EntityPropertyResolver.column(metadata, property);
        AggregateExpression<V> expression = factory.apply(metadata.field(column));
        if (joined) builder.aggregate(metadata.toDynamicForm(), expression);
        else builder.aggregate(expression);
        if (preservesFieldValue(expression)) bindResultValue(expression.alias(), metadata, column);
        return expression;
    }

    private static boolean preservesFieldValue(AggregateExpression<?> expression) {
        return expression.function() == AggregateFunction.MIN || expression.function() == AggregateFunction.MAX;
    }

    private void bindResultValue(String alias, EntityMetadata<?> metadata, String column) {
        if (!metadata.hasEnumConditionValues()) return;
        EntityValues<?> values = valuePlans.apply(metadata.type());
        if (values == null) return;
        if (resultValues.isEmpty()) resultValues = new LinkedHashMap<>();
        resultValues.put(FieldIdentity.of(alias).key(), value -> values.normalizeWriteValue(column, value));
    }

    /** 回调结束后一次映射，允许排序声明在分组之前，且不向规格泄漏实体计划。 */
    void finish() {
        if (sorts == null) return;
        for (ExpressionSort sort : sorts) {
            ValueExpression expression = resultValues.isEmpty() ? sort.expression()
                    : ExpressionFieldValue.mapPriorityValues(sort.expression(), field -> field.source() == null
                            ? resultValues.get(field.identity().key()) : null);
            builder.orderBy(expression, sort.direction());
        }
    }

    @SuppressWarnings("unchecked")
    private <S> EntityMetadata<S> metadata(Class<S> type) {
        EntityMetadata<?> metadata = sources.get(Objects.requireNonNull(type, "aggregate entity type must not be null"));
        if (metadata == null) {
            throw new IllegalArgumentException("aggregate entity source is not part of the query");
        }
        return (EntityMetadata<S>) metadata;
    }
}
