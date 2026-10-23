package com.flying.orm.rdb.operator;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.LogicalOperator;
import com.flying.orm.core.condition.TermRegistry;
import com.flying.orm.core.lambda.EntityProperty;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.mapping.EntityMetadata;
import com.flying.orm.rdb.internal.mapping.EntityPropertyResolver;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.protection.ProtectedConditions;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * 为实体 Lambda DML 统一保存根条件和嵌套条件组。根实例只服务一次命令构建，嵌套实例只在对应回调内有效；
 * 字段会先解析为实体元数据中的列名，值仍进入统一条件归一化和参数绑定链路。调用方不能通过本类型注入 SQL 片段。
 *
 * @param <T> 实体类型
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class EntityCondition<T> implements ConditionDsl<EntityProperty<T, ?>, EntityCondition<T>> {

    private final EntityMetadata<T> metadata;
    private final ConditionGroup.Builder builder;
    private final EntityValues<T> entityValues;
    private final TermRegistry terms;

    private EntityCondition(EntityMetadata<T> metadata, ConditionGroup.Builder builder,
                            EntityValues<T> entityValues, TermRegistry terms) {
        this.metadata = Objects.requireNonNull(metadata, "entity metadata must not be null");
        this.builder = Objects.requireNonNull(builder, "condition builder must not be null");
        this.entityValues = entityValues;
        this.terms = terms;
    }

    EntityCondition(EntityMetadata<T> metadata, SqlRenderer renderer, EntityValues<T> entityValues) {
        this(metadata, Objects.requireNonNull(renderer, "sql renderer must not be null").conditions(),
                entityValues, renderer.terms());
    }

    /** 添加可选标准或业务条件；值为空时不生成节点，严格 where 的行为不变。 */
    public EntityCondition<T> whereIfPresent(EntityProperty<T, ?> property, String operator, Object value) {
        builder.whereIfPresent(column(property), operator, value);
        return this;
    }

    /** 追加标准或已注册的业务条件，连接方式由当前 AND/OR 组决定。 */
    public EntityCondition<T> where(EntityProperty<T, ?> property, String operator, Object value) {
        builder.add(column(property), operator, value);
        return this;
    }

    /** 按实体属性名或映射列名追加条件，与 Getter 共用值转换和参数绑定。 */
    public EntityCondition<T> where(String property, String operator, Object value) {
        builder.add(metadata.field(property).columnName(), operator, value);
        return this;
    }

    public EntityCondition<T> where(String property, Object value) { return where(property, "=", value); }

    /** 可选条件只省略空值，不改变实体字段解析规则。 */
    public EntityCondition<T> whereIfPresent(String property, String operator, Object value) {
        builder.whereIfPresent(metadata.field(property).columnName(), operator, value);
        return this;
    }

    public EntityCondition<T> whereIfPresent(String property, Object value) {
        return whereIfPresent(property, "=", value);
    }

    /** 在嵌套条件组中精确匹配声明了 EXACT 的加密字段。 */
    public EntityCondition<T> exactEncrypted(EntityProperty<T, ?> property, Object value) {
        return where(property, ProtectedConditions.EXACT, value);
    }

    /** 在嵌套条件组中后缀匹配声明了固定长度的加密字段。 */
    public EntityCondition<T> suffixEncrypted(EntityProperty<T, ?> property, Object value) {
        return where(property, ProtectedConditions.SUFFIX, value);
    }

    /** 在嵌套条件组中包含匹配声明了 CONTAINS 的加密字段。 */
    public EntityCondition<T> containsEncrypted(EntityProperty<T, ?> property, Object value) {
        return where(property, ProtectedConditions.CONTAINS, value);
    }

    /** 追加括号包裹的 OR 条件组，组内可以继续嵌套。 */
    public EntityCondition<T> or(Consumer<EntityCondition<T>> consumer) {
        Objects.requireNonNull(consumer, "entity OR condition must not be null");
        builder.or(nested -> consumer.accept(new EntityCondition<>(metadata, nested, entityValues, terms)));
        return this;
    }

    /** 追加括号包裹的 AND 条件组，组内可以继续嵌套。 */
    public EntityCondition<T> andGroup(Consumer<EntityCondition<T>> consumer) {
        Objects.requireNonNull(consumer, "entity AND condition must not be null");
        builder.and(nested -> consumer.accept(new EntityCondition<>(metadata, nested, entityValues, terms)));
        return this;
    }

    String column(EntityProperty<T, ?> property) {
        return EntityPropertyResolver.column(metadata, property);
    }

    TermRegistry terms() {
        return terms;
    }

    Object conditionValue(String column, String operator, Object value) {
        return entityValues == null ? value : entityValues.normalizeConditionScalar(column, operator, value, terms);
    }

    ConditionGroup build() {
        ConditionGroup where = builder.build();
        return entityValues == null ? where : entityValues.normalizeCondition(where, terms);
    }

    EntityCondition<T> copy() {
        ConditionGroup raw = builder.build();
        ConditionGroup.Builder copied = raw.operator() == LogicalOperator.AND
                ? ConditionGroup.and(terms) : ConditionGroup.or(terms);
        raw.children().forEach(copied::add);
        return new EntityCondition<>(metadata, copied, entityValues, terms);
    }
}
