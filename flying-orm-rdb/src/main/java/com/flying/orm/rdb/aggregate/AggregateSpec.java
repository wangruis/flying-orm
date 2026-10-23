package com.flying.orm.rdb.aggregate;

import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.page.ExpressionSort;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.QuerySpec;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.LinkedHashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;

/**
 * 一次聚合查询的不可变规格。
 *
 * <p>单表输入复用 QuerySpec，关联输入复用 JoinQuerySpec 的关系与来源 Scope。分组、聚合、HAVING
 * 和结果排序共用同一模型；来源绑定不改变类型化表达式句柄。别名在 build 时一次性去重。</p>
 *
 * @author wangr
 * @version v3.2
 */
public final class AggregateSpec {

    private final QuerySpec query;
    private final JoinQuerySpec join;
    private final Map<String, JoinSource> sources;
    private final List<PageSort> sorts;
    private final Map<String, ValueExpression> sortExpressions;
    private final SqlExecutionOptions options;
    private final AggregateRowLayout layout;
    private final ConditionGroup having;

    private AggregateSpec(Builder builder) {
        query = builder.query;
        join = builder.join;
        sources = Map.copyOf(builder.sources);
        options = builder.options == null && query != null
                ? query.executionOptions().orElse(null) : builder.options;
        List<GroupSelection> groups = List.copyOf(builder.groups);
        List<AggregateExpression<?>> aggregates = List.copyOf(builder.aggregates);
        having = builder.having;
        if (aggregates.isEmpty() && groups.isEmpty()) {
            throw new IllegalArgumentException("aggregate spec requires at least one group or aggregate expression");
        }
        Set<String> names = validateAliases(groups, aggregates);
        layout = AggregateRowLayout.of(groups, aggregates);
        if (builder.sorts == null) {
            sorts = query == null ? List.of() : query.sorts();
            sortExpressions = Map.of();
        } else {
            for (ExpressionSort sort : builder.sorts) {
                if (sort.expression() instanceof ValueExpression.Field field) names.add(field.identity().key());
            }
            List<PageSort> ordered = new ArrayList<>(builder.sorts.size());
            Map<String, ValueExpression> expressions = new LinkedHashMap<>();
            int index = 0;
            for (ExpressionSort sort : builder.sorts) {
                String name;
                if (sort.expression() instanceof ValueExpression.Field field && field.source() == null) {
                    name = field.identity().name();
                } else {
                    do {
                        name = "__fo_order_" + index++;
                    } while (!names.add(FieldIdentity.of(name).key()));
                    expressions.put(name, sort.expression());
                }
                ordered.add(new PageSort(name, sort.direction()));
            }
            sorts = List.copyOf(ordered);
            sortExpressions = Map.copyOf(expressions);
        }
    }

    public static Builder builder(QuerySpec query) {
        return new Builder(query);
    }

    private AggregateSpec(AggregateSpec source, DataScope scope) {
        query = source.query().withScope(source.query.scope().and(scope));
        join = source.join;
        sources = source.sources;
        sorts = source.sorts;
        sortExpressions = source.sortExpressions;
        options = source.options;
        layout = source.layout;
        having = source.having;
    }

    /** 叠加单表查询范围，保留聚合、HAVING、排序和执行选项；关联查询须在各 JoinSource 上设置范围。 */
    public AggregateSpec withQueryScope(DataScope scope) {
        return new AggregateSpec(this, Objects.requireNonNull(scope, "aggregate query scope must not be null"));
    }

    /** 复用 JOIN 的来源、ON、业务条件和 Scope；普通投影/排序不作为聚合输出。 */
    public static Builder builder(JoinQuerySpec relation) {
        return new Builder(relation);
    }

    /** 返回单表输入；关联规格应读取 joinQuery，不会伪造单表输入。 */
    public QuerySpec query() {
        if (query == null) {
            throw new IllegalStateException("joined aggregate uses joinQuery instead of a single-table query");
        }
        return query;
    }

    public Optional<JoinQuerySpec> joinQuery() {
        return Optional.ofNullable(join);
    }

    public List<PageSort> sorts() {
        return sorts;
    }

    /** 排序项对应的计算表达式；键仅作规格内引用，不是返回列。 */
    public Map<String, ValueExpression> sortExpressions() {
        return sortExpressions;
    }

    public Optional<SqlExecutionOptions> executionOptions() {
        return Optional.ofNullable(options);
    }

    public SensitiveDisplayMode sensitiveDisplayMode() {
        return join == null ? query.sensitiveDisplayMode() : join.sensitiveDisplayMode();
    }

    /** 聚合结果别名对应的关系来源；单表查询返回 null。 */
    public JoinSource source(String alias) {
        return join == null ? null : sources.getOrDefault(FieldIdentity.of(alias).key(), join.root());
    }

    public List<GroupSelection> groups() {
        return layout.groups();
    }

    public List<AggregateExpression<?>> aggregates() {
        return layout.aggregates();
    }

    /** 结果列只随规格改变；重复执行共享布局，不共享请求或结果数据。 */
    public AggregateRowLayout layout() {
        return layout;
    }

    public Optional<ConditionGroup> having() {
        return Optional.ofNullable(having);
    }

    private static Set<String> validateAliases(List<GroupSelection> groups,
                                        List<AggregateExpression<?>> aggregates) {
        Set<String> aliases = new HashSet<>();
        for (GroupSelection group : groups) {
            if (!aliases.add(FieldIdentity.of(group.alias()).key())) {
                throw new IllegalArgumentException("aggregate result aliases must be unique");
            }
        }
        for (AggregateExpression<?> aggregate : aggregates) {
            if (!aliases.add(FieldIdentity.of(aggregate.alias()).key())) {
                throw new IllegalArgumentException("aggregate result aliases must be unique");
            }
        }
        return aliases;
    }

    /** 只在一次配置调用中使用的构建器。 */
    public static final class Builder {

        private final QuerySpec query;
        private final JoinQuerySpec join;
        private final Map<String, JoinSource> sources = new LinkedHashMap<>();
        private List<ExpressionSort> sorts;
        private SqlExecutionOptions options;
        private final List<GroupSelection> groups = new ArrayList<>();
        private final List<AggregateExpression<?>> aggregates = new ArrayList<>();
        private ConditionGroup having;

        private Builder(QuerySpec query) {
            this.query = Objects.requireNonNull(query, "aggregate query must not be null");
            this.join = null;
        }

        private Builder(JoinQuerySpec join) {
            this.query = null;
            this.join = Objects.requireNonNull(join, "aggregate join relation must not be null");
        }

        /** 关联分组保留来源身份；未指定来源的 group 使用根源。 */
        public Builder group(JoinSource source, GroupSelection group) {
            bindSource(source, Objects.requireNonNull(group, "aggregate group must not be null").alias());
            return group(group);
        }

        public Builder group(DynamicForm form, GroupSelection group) {
            return group(source(form), group);
        }

        /** 聚合函数使用指定源的字段；表达式仍可用于 AggregateRow.get。 */
        public Builder aggregate(JoinSource source, AggregateExpression<?> aggregate) {
            bindSource(source, Objects.requireNonNull(aggregate, "aggregate expression must not be null").alias());
            return aggregate(aggregate);
        }

        public Builder aggregate(DynamicForm form, AggregateExpression<?> aggregate) {
            return aggregate(source(form), aggregate);
        }

        public Builder orderBy(PageSort... order) {
            for (PageSort sort : order) orderBy(ValueExpression.field(sort.field()), sort.direction());
            return this;
        }

        /** 只引用本次声明的结果别名；不增加返回列，按调用顺序排序。 */
        public Builder orderBy(ValueExpression expression, PageSort.Direction direction) {
            ExpressionSort sort = new ExpressionSort(expression, direction);
            if (sorts == null) {
                sorts = new ArrayList<>();
                if (query != null) {
                    for (PageSort inherited : query.sorts()) {
                        sorts.add(new ExpressionSort(ValueExpression.field(inherited.field()), inherited.direction()));
                    }
                }
            }
            sorts.add(sort);
            return this;
        }

        /** 指定结果值依次排前，其他值排后。 */
        public Builder orderByFirst(String alias, Object... values) {
            return orderBy(ValueExpression.field(alias).priority(values), PageSort.Direction.ASC);
        }

        /** 指定结果值依次排后，其他值排前。 */
        public Builder orderByLast(String alias, Object... values) {
            return orderBy(ValueExpression.lastMatch(ValueExpression.field(alias).priority(values)), PageSort.Direction.ASC);
        }

        public Builder executionOptions(SqlExecutionOptions options) {
            this.options = Objects.requireNonNull(options, "aggregate execution options must not be null");
            return this;
        }

        private void bindSource(JoinSource source, String alias) {
            if (join == null || !join.sources().contains(Objects.requireNonNull(source, "aggregate source must not be null"))) {
                throw new IllegalArgumentException("aggregate source must belong to its join relation");
            }
            sources.put(FieldIdentity.of(alias).key(), source);
        }

        private JoinSource source(DynamicForm form) {
            Objects.requireNonNull(form, "aggregate source form must not be null");
            JoinSource found = null;
            if (join != null) {
                for (JoinSource candidate : join.sources()) {
                    if (candidate.form() != form) continue;
                    if (found != null) {
                        throw new IllegalArgumentException("ambiguous aggregate source; use an explicit JoinSource for self joins");
                    }
                    found = candidate;
                }
            }
            if (found == null) throw new IllegalArgumentException("aggregate source form must belong to its join relation");
            return found;
        }

        /** 为表达式解析字段来源；同表多次关联须直接使用 JoinSource.field。 */
        public ValueExpression field(DynamicForm form, String name) {
            if (join != null) return ValueExpression.field(source(form).field(name));
            if (query.form() != form) {
                throw new IllegalArgumentException("expression form must belong to the aggregate query");
            }
            return ValueExpression.field(form.field(name).name());
        }

        public Builder group(GroupSelection group) {
            groups.add(Objects.requireNonNull(group, "aggregate group must not be null"));
            return this;
        }

        /** 注册字段分组；关联查询使用根来源，指定其他来源时使用带来源的重载。 */
        public Builder group(String field, String alias) {
            return group(GroupSelection.of(field, alias));
        }

        /** 按函数或计算值分组，字段来源与参数由共享表达式规划器解析。 */
        public Builder group(ValueExpression operand, String alias) {
            return group(GroupSelection.of(operand, alias));
        }

        /** 统计关系行数，不因字段为 NULL 而漏计。 */
        public Builder countRows(String alias) {
            return aggregate(AggregateExpression.countRows(alias));
        }

        /** 统计字段非 NULL 值的数量；关联查询使用根来源。 */
        public Builder count(String field, String alias) {
            return aggregate(AggregateExpression.count(field, alias));
        }

        /** 统计字段去重后的非 NULL 值数量；关联查询使用根来源。 */
        public Builder countDistinct(String field, String alias) {
            return aggregate(AggregateExpression.countDistinct(field, alias));
        }

        /** 数值字段求和，结果为 BigDecimal；关联查询使用根来源。 */
        public Builder sum(String field, String alias) {
            return aggregate(AggregateExpression.sum(field, alias));
        }

        /** 数值字段平均值，结果为 BigDecimal；关联查询使用根来源。 */
        public Builder avg(String field, String alias) {
            return aggregate(AggregateExpression.avg(field, alias));
        }

        /** 总体标准差，结果为 BigDecimal。 */
        public Builder stddevPop(String field, String alias) {
            return aggregate(AggregateExpression.stddevPop(field, alias));
        }

        /** 样本标准差，结果为 BigDecimal。 */
        public Builder stddevSample(String field, String alias) {
            return aggregate(AggregateExpression.stddevSample(field, alias));
        }

        /** 总体方差，结果为 BigDecimal。 */
        public Builder variancePop(String field, String alias) {
            return aggregate(AggregateExpression.variancePop(field, alias));
        }

        /** 样本方差，结果为 BigDecimal。 */
        public Builder varianceSample(String field, String alias) {
            return aggregate(AggregateExpression.varianceSample(field, alias));
        }

        public Builder aggregate(AggregateExpression<?> aggregate) {
            aggregates.add(Objects.requireNonNull(
                    aggregate, "aggregate expression must not be null"));
            return this;
        }

        public Builder having(ConditionGroup having) {
            this.having = Objects.requireNonNull(having, "aggregate having must not be null");
            return this;
        }

        /** 配置整棵 HAVING 条件树，替换此前 HAVING；嵌套 AND/OR 和值绑定沿用通用条件模型。 */
        public Builder having(Consumer<ConditionGroup.Builder> configuration) {
            ConditionGroup.Builder condition = ConditionGroup.and();
            Objects.requireNonNull(configuration, "aggregate having configuration must not be null").accept(condition);
            return having(condition.build());
        }

        public AggregateSpec build() {
            return new AggregateSpec(this);
        }
    }
}
