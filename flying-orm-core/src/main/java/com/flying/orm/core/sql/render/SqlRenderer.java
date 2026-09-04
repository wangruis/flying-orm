package com.flying.orm.core.sql.render;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionNode;
import com.flying.orm.core.condition.LogicalOperator;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.condition.TermRegistry;
import com.flying.orm.core.internal.Names;
import com.flying.orm.core.internal.condition.ConditionExecutionViews;
import com.flying.orm.core.internal.value.OwnedBindableValues;

import java.util.Objects;
import java.util.function.UnaryOperator;

/**
 * SQL 条件渲染器负责把条件 AST 转成参数化 SQL 片段，并严格保持占位符与参数顺序一致。
 *
 * <p>它不访问数据库，也不保存一次渲染过程中的可变状态。每次调用都使用局部 StringBuilder 和参数列表，
 * 因而同一个实例可以被多个请求并发复用。业务扩展条件通过只读的 {@link SqlTermRegistry} 接入，
 * handler 只能返回参数化 {@link SqlFragment}，不能把前端值直接拼进 SQL。</p>
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public final class SqlRenderer implements SqlRenderContext {

    private final SqlTermRegistry termRegistry;
    private final long standardConditionTermMask;
    private final ValueCodecRegistry valueCodecs;
    private final UnaryOperator<String> identifierRenderer;
    private final UnaryOperator<String> structureIdentifierRenderer;

    /** 仍以实例身份隔离；只有启用 descriptor 时才把冻结声明混入现有缓存键哈希。 */
    private final int cacheHashCode;

    private SqlRenderer(SqlTermRegistry termRegistry,
                        ValueCodecRegistry valueCodecs,
                        UnaryOperator<String> identifierRenderer,
                        UnaryOperator<String> structureIdentifierRenderer) {
        this.termRegistry = Objects.requireNonNull(termRegistry, "sql term registry must not be null");
        this.standardConditionTermMask = ConditionExecutionViews.standardTermMask(termRegistry.conditionTerms());
        this.valueCodecs = Objects.requireNonNull(valueCodecs, "value codec registry must not be null");
        this.identifierRenderer = Objects.requireNonNull(identifierRenderer,
                                                         "sql identifier renderer must not be null");
        this.structureIdentifierRenderer = Objects.requireNonNull(
                structureIdentifierRenderer, "sql structure identifier renderer must not be null");
        int identityHash = System.identityHashCode(this);
        if (this.termRegistry.hasDescriptors()) {
            identityHash = 31 * identityHash + this.termRegistry.descriptorFingerprint().hashCode();
        }
        if (this.valueCodecs.hasDescriptors()) {
            identityHash = 31 * identityHash + this.valueCodecs.descriptorFingerprint().hashCode();
        }
        this.cacheHashCode = identityHash;
    }

    /** SqlRenderer 继续按对象身份判等；哈希只在构造时计算一次，不触碰请求值。 */
    @Override
    public int hashCode() {
        return cacheHashCode;
    }

    /** 与既有 Object 语义一致：不同装配实例即使 descriptor 相同也不能共享 renderer 缓存身份。 */
    @Override
    public boolean equals(Object other) {
        return this == other;
    }

    /**
     * 创建 SQL 渲染器构建器。
     *
     * @return SQL 渲染器构建器
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * 创建与当前渲染器使用同一套 term 规则的条件构建器。
     *
     * <p>自定义 handler 的 id 和值形状已经包含在里面，调用方不需要再单独拼一份 TermRegistry。</p>
     *
     * @return 新的、只供本次条件构造使用的 builder
     */
    public ConditionGroup.Builder conditions() {
        return ConditionGroup.and(terms());
    }

    /**
     * 返回与 SQL handler 完全对应的只读值形状注册表。前端条件和参数条件可以复用它，
     * 不需要再为同一批扩展 term 手写第二份形状配置。
     *
     * @return 当前 renderer 真正能够渲染的 term 及其值形状
     */
    public TermRegistry terms() {
        return termRegistry.conditionTerms();
    }

    /**
     * 渲染 where 条件片段，供 query、update、delete 等 SQL 形态复用。
     *
     * @param where 条件组
     * @return SQL 条件片段
     */
    public SqlFragment renderWhere(ConditionGroup where) {
        RenderAccumulator accumulator = new RenderAccumulator();
        renderGroup(where, false, accumulator, null, null);
        return accumulator.publish();
    }

    /** 内部组合边界：只为关系子查询提供已渲染的外层表达式及其所属关系，不改变普通条件字段。 */
    public SqlFragment renderWhere(ConditionGroup where,
                                   UnaryOperator<String> correlatedFieldRenderer,
                                   UnaryOperator<String> outerQualifierRenderer) {
        RenderAccumulator accumulator = new RenderAccumulator();
        renderGroup(where, false, accumulator,
                    Objects.requireNonNull(correlatedFieldRenderer, "correlated field renderer must not be null"),
                    Objects.requireNonNull(outerQualifierRenderer, "outer qualifier renderer must not be null"));
        return accumulator.publish();
    }

    /** 装配时确定的关系 term 标记；未启用的普通 CRUD 不创建关联上下文。 */
    public boolean hasCorrelatedTerms() {
        return termRegistry.hasCorrelatedTerms();
    }

    @Override
    public String identifier(String name) {
        String safeName = SqlIdentifiers.requireIdentifier(name, "sql identifier");
        return Objects.requireNonNull(identifierRenderer.apply(safeName),
                                      "rendered sql identifier must not be null");
    }

    /**
     * 输出安全的查询投影。普通字段和限定字段逐段走方言引用，{@code *} 与 {@code alias.*} 只保留星号本身。
     * 任意函数、别名表达式或 SQL 片段都会在这里被拒绝。
     *
     * @param name 普通字段、限定字段、{@code *} 或 {@code alias.*}
     * @return 可以直接放进 select 列表的安全文本
     */
    public String projection(String name) {
        String projection = Names.requireText(name, "sql projection");
        if ("*".equals(projection)) {
            return projection;
        }
        if (projection.endsWith(".*")) {
            String qualifier = projection.substring(0, projection.length() - 2);
            return identifier(qualifier) + ".*";
        }
        return identifier(projection);
    }

    /**
     * 条件参数在生成 SQL 请求前统一走应用 codec。渲染器构造后只读，这里没有共享可变状态。
     */
    @Override
    public Object parameter(Object value) {
        return SqlFragment.encodedParameter(valueCodecs.write(value));
    }

    /** 供条件计划缓存 O(1) 判断标准结构 term 是否被覆盖。 */
    public long standardConditionTermMask() {
        return standardConditionTermMask;
    }

    String structureIdentifier(String name) {
        String safeName = SqlIdentifiers.requireIdentifier(name, "sql structure identifier");
        return Objects.requireNonNull(structureIdentifierRenderer.apply(safeName),
                                      "rendered sql structure identifier must not be null");
    }

    /**
     * 返回当前渲染器使用的只读 codec 注册表，表单和实体映射可直接复用同一个实例。
     *
     * @return 当前 codec 注册表
     */
    public ValueCodecRegistry valueCodecs() {
        return valueCodecs;
    }

    /**
     * 保留当前 term 注册表，只替换参数转换规则。返回新实例，原渲染器和正在执行的请求不受影响。
     *
     * @param valueCodecs 新的应用级 codec 注册表
     * @return 使用新 codec 的只读渲染器
     */
    public SqlRenderer withValueCodecs(ValueCodecRegistry valueCodecs) {
        return new SqlRenderer(termRegistry, valueCodecs, identifierRenderer, structureIdentifierRenderer);
    }

    /**
     * 保留 term 和参数 codec，只替换表名、字段名的输出规则。
     *
     * <p>方言化表单渲染会调用这个方法，让 select 列、where 条件和 DDL 使用完全相同的引用方式。
     * 返回的是新实例，没有修改共享对象，因此已缓存的渲染器和并发请求互不影响。</p>
     *
     * @param identifierRenderer 已通过安全校验的名字到方言标识符的转换器
     * @return 使用新标识符规则的只读渲染器
     */
    public SqlRenderer withIdentifierRenderer(UnaryOperator<String> identifierRenderer) {
        UnaryOperator<String> safeRenderer = Objects.requireNonNull(
                identifierRenderer, "sql identifier renderer must not be null");
        return new SqlRenderer(termRegistry, valueCodecs, safeRenderer, safeRenderer);
    }

    /**
     * 保留方言的表名、别名和关系列渲染，只替换当前条件叶子字段的限定规则。
     *
     * <p>这是 JOIN 和受保护候选查询的内部组合边界。普通调用方应使用
     * {@link #withIdentifierRenderer(UnaryOperator)} 同时配置所有标识符。</p>
     *
     * @param identifierRenderer 当前条件字段的安全渲染器
     * @return 只替换条件字段规则的新渲染器
     */
    public SqlRenderer withFieldIdentifierRenderer(UnaryOperator<String> identifierRenderer) {
        return new SqlRenderer(termRegistry,
                               valueCodecs,
                               Objects.requireNonNull(identifierRenderer,
                                                      "sql field identifier renderer must not be null"),
                               structureIdentifierRenderer);
    }

    private boolean renderNode(ConditionNode node, RenderAccumulator accumulator,
                               UnaryOperator<String> correlatedFields,
                               UnaryOperator<String> outerQualifiers) {
        // AST 目前只有叶子 term 和逻辑分组。遇到未知实现立即失败，不能悄悄漏掉安全条件。
        if (node instanceof TermCondition term) {
            SqlTermHandler handler = termRegistry.handler(term.operator());
            if (handler instanceof SimpleSqlTermHandler simple
                    && simple.appendTo(term, this, accumulator)) {
                return true;
            }
            if (correlatedFields != null && handler instanceof RelationExistsTermHandler relation) {
                return accumulator.append(relation.render(
                        term, this, correlatedFields.apply(term.field()), outerQualifiers.apply(term.field())));
            }
            return accumulator.append(handler.render(term, this));
        }
        if (node instanceof ConditionGroup group) {
            return renderGroup(group, true, accumulator, correlatedFields, outerQualifiers);
        }
        throw new IllegalArgumentException("unsupported condition node: " + node.getClass().getName());
    }

    private boolean renderGroup(ConditionGroup group, boolean nested, RenderAccumulator accumulator,
                                UnaryOperator<String> correlatedFields,
                                UnaryOperator<String> outerQualifiers) {
        Objects.requireNonNull(group, "condition group must not be null");
        if (group.children().isEmpty()) {
            return false;
        }

        String delimiter = group.operator() == LogicalOperator.AND ? " and " : " or ";
        int groupSqlStart = accumulator.sqlLength();
        int groupParameterStart = accumulator.parameterCount();
        if (nested) {
            accumulator.appendSql("(");
        }
        boolean rendered = false;
        for (ConditionNode child : group.children()) {
            int childSqlStart = accumulator.sqlLength();
            int childParameterStart = accumulator.parameterCount();
            if (rendered) {
                accumulator.appendSql(delimiter);
            }
            if (renderNode(child, accumulator, correlatedFields, outerQualifiers)) {
                rendered = true;
            } else {
                accumulator.rollback(childSqlStart, childParameterStart);
            }
        }
        if (!rendered) {
            accumulator.rollback(groupSqlStart, groupParameterStart);
            return false;
        }
        if (nested) {
            accumulator.appendSql(")");
        }
        return true;
    }

    /** 一次 render 调用独占的 SQL 与参数累加器。 */
    private static final class RenderAccumulator implements SqlTermOutput {

        private final StringBuilder sql = new StringBuilder();

        private final OwnedBindableValues.Buffer parameters = OwnedBindableValues.buffer();

        @Override
        public void appendSql(String value) {
            sql.append(value);
        }

        @Override
        public void addParameter(Object value) {
            parameters.add(SqlFragment.bindableParameter(value));
        }

        private boolean append(SqlFragment fragment) {
            if (fragment.sql().isBlank()) {
                return false;
            }
            sql.append(fragment.sql());
            parameters.addAll(fragment.parameters());
            return true;
        }

        private int sqlLength() {
            return sql.length();
        }

        private int parameterCount() {
            return parameters.size();
        }

        private void rollback(int sqlLength, int parameterCount) {
            sql.setLength(sqlLength);
            parameters.truncate(parameterCount);
        }

        private SqlFragment publish() {
            return new SqlFragment(sql.toString(), parameters.publish());
        }
    }

    /**
     * SQL 渲染器构建器。构建器是一次性配置对象，不承诺并发安全；build 后的渲染器只读、可共享。
     *
     * @author wangr
     * @date 2026-07-21
     * @version v1.0
     */
    public static final class Builder {

        private final SqlTermRegistry.Builder terms = SqlTermRegistry.builder();

        private ValueCodecRegistry valueCodecs = ValueCodecRegistry.standard();

        private Builder() {
        }

        /**
         * 添加 SQL term handler。
         *
         * @param handler SQL term handler
         * @return 当前构建器
         */
        public Builder addTerm(SqlTermHandler handler) {
            // 重名 operator 的处理规则交给 registry，所有入口保持同一套冲突检查。
            terms.add(handler);
            return this;
        }

        /**
         * 添加 SQL term 命名包，适合一次注册某个业务领域的多个条件算子。
         *
         * @param termPackage SQL term 命名包
         * @return 当前构建器
         */
        public Builder addTermPackage(SqlTermPackage termPackage) {
            Objects.requireNonNull(termPackage, "sql term package must not be null").handlers().forEach(terms::add);
            return this;
        }

        /**
         * 添加默认 SQL term handler 集合。
         *
         * @return 当前构建器
         */
        public Builder addDefaultTerms() {
            SqlTermHandler.defaults().forEach(terms::add);
            return this;
        }

        /**
         * 设置条件参数使用的 codec 注册表。它会在 {@code build()} 时固定下来，后续并发渲染只读。
         *
         * @param valueCodecs 应用级 codec 注册表
         * @return 当前构建器
         */
        public Builder valueCodecs(ValueCodecRegistry valueCodecs) {
            this.valueCodecs = Objects.requireNonNull(valueCodecs, "value codec registry must not be null");
            return this;
        }

        /**
         * 构建 SQL 渲染器。
         *
         * @return SQL 渲染器
         */
        public SqlRenderer build() {
            UnaryOperator<String> identifiers = UnaryOperator.identity();
            return new SqlRenderer(terms.build(), valueCodecs, identifiers, identifiers);
        }
    }
}
