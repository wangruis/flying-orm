package com.flying.orm.rdb.operator;

import com.flying.orm.core.page.PageQuery;
import com.flying.orm.rdb.form.spec.QueryInput;
import com.flying.orm.core.condition.StructuredConditionCompiler;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.lambda.EntityProperty;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.aggregate.AggregateSpec;
import com.flying.orm.rdb.mapping.EntityQueryDefaults;
import com.flying.orm.rdb.internal.condition.ConditionNodes;

import java.util.List;
import java.util.Arrays;
import java.util.Objects;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * 实体查询的执行方式无关计划。
 *
 * <p>投影、分组、排序和条件都在执行前归并成 {@link QuerySpec}。这样同步和响应式门面使用同一份
 * 字段验证与参数顺序；所有实体读取终端遵循同一投影规则，显式选列时返回部分实体。</p>
 *
 * @param <T> 实体类型
 * @param <S> 同步或响应式查询终端
 * @author wangr
 * @version v1.0
 */
public abstract class EntityQueryCommand<T, S extends EntityQueryCommand<T, S>>
        extends EntityCommand<T, S> implements ProtectedEntityQuery<T, S> {

    private final FieldScope defaultFields;
    private QuerySpec.Builder query;

    EntityQueryCommand(EntityDmlModel<T> model, SqlRenderer renderer, FieldScope defaultFields) {
        super(model, renderer);
        this.defaultFields = defaultFields;
        this.query = QuerySpec.builder(model.form());
    }

    EntityQueryCommand(EntityQueryCommand<T, ?> source) {
        super(source);
        this.defaultFields = source.defaultFields;
        this.query = source.query.copy();
    }

    public S orderByAsc(EntityProperty<T, ?> property) {
        orderBy(where().column(property), PageSort.Direction.ASC);
        return self();
    }

    public S orderByDesc(EntityProperty<T, ?> property) {
        orderBy(where().column(property), PageSort.Direction.DESC);
        return self();
    }

    @SafeVarargs
    public final S select(EntityProperty<T, ?>... properties) {
        query.select(columns(properties));
        return self();
    }

    /** 按实体属性名或映射列名选列。 */
    public S select(String... properties) {
        query.select(Arrays.stream(properties).map(this::column).toArray(String[]::new));
        return self();
    }

    /** 未指定字段时保留默认投影，消除两种 varargs 入口的空调用歧义。 */
    public S select() { return self(); }

    public S selectExpression(Function<EntityExpressions<T>, ValueExpression> expression, String alias) {
        ValueExpression value = Objects.requireNonNull(Objects.requireNonNull(expression,
                "projection expression must not be null").apply(new EntityExpressions<>(this)),
                "projection expression must not return null");
        query.expression(alias, value);
        return self();
    }

    void orderBy(String output, PageSort.Direction direction) {
        query.orderBy(ValueExpression.field(output), direction);
    }

    public S orderByExpression(Function<EntityExpressions<T>, ValueExpression> expression, PageSort.Direction direction) {
        query.orderBy(Objects.requireNonNull(expression, "sort expression must not be null")
                .apply(new EntityExpressions<>(this)), direction);
        return self();
    }

    @SafeVarargs
    public final S exclude(EntityProperty<T, ?>... properties) {
        Objects.requireNonNull(properties, "entity properties must not be null");
        for (EntityProperty<T, ?> property : properties) {
            query.exclude(where().column(property));
        }
        return self();
    }

    /** 按实体属性名或映射列名排除结果字段。 */
    public S exclude(String... properties) {
        for (String property : properties) query.exclude(column(property));
        return self();
    }

    public S exclude() { return self(); }

    @SafeVarargs
    public final S groupBy(EntityProperty<T, ?>... properties) {
        query.groupBy(columns(properties));
        return self();
    }

    /** 按实体属性名或映射列名分组，复用相同的查询规格与执行终端。 */
    public S groupBy(String... properties) {
        query.groupBy(Arrays.stream(properties).map(this::column).toArray(String[]::new));
        return self();
    }

    public S groupBy() { return self(); }

    @Override
    public S sensitiveDisplay(SensitiveDisplayMode mode) {
        query.display(mode);
        return self();
    }

    public S filter(StructuredConditionInput input, StructuredConditionPolicy policy) {
        StructuredConditionPolicy safePolicy = Objects.requireNonNull(policy, "structured condition policy must not be null")
                .withTerms(where().terms());
        StructuredConditionCompiler.validateStructure(input, safePolicy);
        StructuredConditionInput mapped = ConditionNodes.rewrite(input, term -> {
            if (term.field() == null || term.field().isBlank()) {
                return term;
            }
            String column = column(term.field());
            return column.equals(term.field()) ? term
                    : new StructuredConditionInput(column, term.operator(), term.value(), term.logic(), term.terms());
        });
        StructuredConditionPolicy mappedPolicy = safePolicy.mapFields(this::column);
        if (metadata().hasEnumConditionValues()) {
            mappedPolicy = mappedPolicy.mapValues(where()::conditionValue);
        }
        query.filter(mapped, mappedPolicy);
        return self();
    }

    private String column(String property) {
        return metadata().findField(property).map(field -> field.columnName()).orElse(property);
    }

    public S input(QueryInput input, StructuredConditionPolicy policy) {
        Objects.requireNonNull(input, "query input must not be null");
        query.input(new QueryInput(null,
                input.select().stream().map(this::column).toList(),
                input.exclude().stream().map(this::column).toList(), input.sorts(), input.page()), policy);
        if (input.filter() != null) filter(input.filter(), policy);
        return self();
    }

    PageQuery inputPage() {
        return query.page().orElseThrow(() -> new IllegalStateException("query input has no page"));
    }

    QuerySpec entitySpec() {
        if (query.hasExpressions()) {
            throw new IllegalStateException("computed entity projection requires a result type or executeRows()");
        }
        if (query.hasProjection()) {
            return projectedSpec();
        }
        if (query.hasGroups()) {
            throw new IllegalStateException("grouped entity query requires select(entity properties)");
        }
        QuerySpec spec = configuredQuery().build(this::column);
        return EntityQueryDefaults.applyProjection(spec, metadata(), defaultFields, spec.excludedFields());
    }

    QuerySpec projectedSpec() {
        if (!query.hasProjection()) {
            if (query.hasExclusions()) {
                return entitySpec();
            }
            throw new IllegalStateException("executeRows() requires select(entity properties)");
        }
        return configuredQuery().build(this::column);
    }

    QuerySpec resultSpec() {
        return query.hasProjection() ? projectedSpec() : entitySpec();
    }

    QuerySpec countSpec() {
        return configuredQuery().buildCount();
    }

    KeysetPageQuery keysetQuery(KeysetPageQuery page) {
        List<KeysetSort> mapped = page.sorts().stream().map(sort -> {
            String field = column(sort.field());
            return field.equals(sort.field()) ? sort : new KeysetSort(field, sort.direction(), sort.nullOrder());
        }).toList();
        return mapped.equals(page.sorts()) ? page : new KeysetPageQuery(page.size(), mapped, page.position());
    }

    AggregateSpec aggregateSpec(Consumer<EntityAggregate> consumer) {
        Objects.requireNonNull(consumer, "aggregate configuration must not be null");
        AggregateSpec.Builder report = AggregateSpec.builder(configuredQuery().buildConditions());
        EntityAggregate aggregate = new EntityAggregate(report, Map.of(metadata().type(), metadata()),
                false, ignored -> entityValues());
        consumer.accept(aggregate);
        aggregate.finish();
        return report.build();
    }

    /** 使用默认条件策略导入完整查询形状，保留服务端条件和 Scope。 */
    public S input(QueryInput input) { return input(input, StructuredConditionPolicy.defaults()); }

    /** 替换外部过滤，保留服务端条件和 Scope。 */
    public S filter(StructuredConditionInput input) { return filter(input, StructuredConditionPolicy.defaults()); }

    /** 按属性名、列名或计算别名排序。 */
    public S orderByAsc(String output) { orderBy(output, PageSort.Direction.ASC); return self(); }
    public S orderByDesc(String output) { orderBy(output, PageSort.Direction.DESC); return self(); }

    /** 指定值依序优先，值经过实体字段映射。 */
    public S orderByFirst(EntityProperty<T, ?> property, Object... values) {
        return orderByExpression(expression -> expression.priority(property, values), PageSort.Direction.ASC);
    }

    /** 指定值依序排在最后，值经过实体字段映射。 */
    public S orderByLast(EntityProperty<T, ?> property, Object... values) {
        return orderByExpression(expression -> ValueExpression.lastMatch(expression.priority(property, values)),
                PageSort.Direction.ASC);
    }

    private QuerySpec.Builder configuredQuery() {
        return query.where(where().build()).scope(scope());
    }

    private String[] columns(EntityProperty<T, ?>[] properties) {
        Objects.requireNonNull(properties, "entity properties must not be null");
        String[] columns = new String[properties.length];
        for (int index = 0; index < properties.length; index++) columns[index] = where().column(properties[index]);
        return columns;
    }
}
