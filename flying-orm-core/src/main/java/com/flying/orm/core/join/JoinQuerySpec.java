package com.flying.orm.core.join;

import com.flying.orm.core.expression.ValueExpression;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionNode;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.internal.Names;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * 轻量多表查询的不可变结构规格。
 *
 * <p>规格只保存已经通过 DynamicForm 校验的数据源、字段引用、结构化 ON、条件、Scope、投影和排序，
 * 不保存 SQL、连接或订阅状态。调用方通过单次使用的 {@link Builder} 组装，执行链可以安全地在线程间共享
 * 构建完成的快照。</p>
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public final class JoinQuerySpec {

    private static final ConditionGroup EMPTY = ConditionGroup.and().build();

    private final JoinSource root;
    private final List<JoinSource> sources;
    private final List<JoinClause> joins;
    private final List<JoinProjection> projections;
    private final List<JoinOrder> orders;
    private final List<JoinFieldRef> groups;
    private final Map<JoinSource, ConditionGroup> conditions;
    private final JoinCondition where;
    private final Map<JoinSource, DataScope> scopes;
    private final SensitiveDisplayMode sensitiveDisplayMode;

    private JoinQuerySpec(Builder builder) {
        this.root = builder.root;
        this.sources = List.copyOf(builder.sources);
        this.joins = List.copyOf(builder.joins);
        this.projections = List.copyOf(builder.projections);
        this.orders = List.copyOf(builder.orders);
        this.groups = builder.groups == null ? List.of() : List.copyOf(builder.groups);
        this.conditions = Map.copyOf(builder.conditions);
        if (conditions.isEmpty()) {
            this.where = builder.where;
        } else {
            List<JoinCondition> combined = new ArrayList<>();
            for (JoinSource source : sources) {
                ConditionGroup condition = conditions.get(source);
                if (condition != null && !condition.children().isEmpty()) combined.add(JoinCondition.where(source, condition));
            }
            if (builder.where != null) combined.add(builder.where);
            this.where = combined.isEmpty() ? null : combined.size() == 1 ? combined.getFirst()
                    : JoinCondition.and(combined.toArray(JoinCondition[]::new));
        }
        this.scopes = Map.copyOf(builder.scopes);
        this.sensitiveDisplayMode = builder.sensitiveDisplayMode;
    }

    /** 创建以给定 DynamicForm 为根源的单次构建器。 */
    public static Builder builder(DynamicForm rootForm) {
        return new Builder(rootForm);
    }

    /** @return 根数据源。 */
    public JoinSource root() {
        return root;
    }

    /** @return 根源与连接源组成的不可变有序列表。 */
    public List<JoinSource> sources() {
        return sources;
    }

    /** @return 不可变有序连接列表。 */
    public List<JoinClause> joins() {
        return joins;
    }

    /** @return 不可变显式投影列表。 */
    public List<JoinProjection> projections() {
        return projections;
    }

    /** @return 不可变排序列表。 */
    public List<JoinOrder> orders() {
        return orders;
    }

    /** @return 来源明确的分组字段。 */
    public List<JoinFieldRef> groups() { return groups; }

    /** 返回目标源的业务条件；没有条件时返回空 AND 组。 */
    public ConditionGroup where(JoinSource source) {
        JoinSource safeSource = Objects.requireNonNull(source, "join condition source must not be null");
        return conditions.getOrDefault(safeSource, EMPTY);
    }

    /** 跨来源的业务条件，保留 AND/OR 分组；未声明时为 null。 */
    public JoinCondition where() { return where; }

    /** 返回目标源的显式 Scope；没有声明时不额外收窄。 */
    public DataScope scope(JoinSource source) {
        JoinSource safeSource = Objects.requireNonNull(source, "join scope source must not be null");
        return scopes.getOrDefault(safeSource, DataScope.none());
    }

    /** @return 本次 JOIN 结果使用的敏感字段显示策略。 */
    public SensitiveDisplayMode sensitiveDisplayMode() {
        return sensitiveDisplayMode;
    }

    /**
     * JOIN AST 的单次可变构建器。
     *
     * <p>构建器不持有 SQL 或数据库资源，不应跨线程复用；{@link #build()} 返回的规格完全不可变。</p>
     *
     * @author wangr
     * @date 2026-08-09
     * @version v1.0
     */
    public static final class Builder {

        private final JoinSource root;
        private final List<JoinSource> sources = new ArrayList<>();
        private final List<JoinClause> joins = new ArrayList<>();
        private final List<JoinProjection> projections = new ArrayList<>();
        private final List<JoinOrder> orders = new ArrayList<>();
        private Set<JoinFieldRef> groups;
        private final Map<JoinSource, ConditionGroup> conditions = new LinkedHashMap<>();
        private JoinCondition where;
        private final Map<JoinSource, DataScope> scopes = new LinkedHashMap<>();
        private final Set<String> aliases = new LinkedHashSet<>();
        /** 仅回退别名按源建立物理序号索引；短别名路径不分配或扫描字段。 */
        private Map<JoinSource, Map<String, Integer>> fieldOrdinals;
        private Map<String, QueryRelation> relations;
        private Map<String, JoinSource> relatedSources;
        private SensitiveDisplayMode sensitiveDisplayMode = SensitiveDisplayMode.DECLARED;

        private Builder(DynamicForm rootForm) {
            this.root = new JoinSource(0, Objects.requireNonNull(rootForm, "join root form must not be null"));
            sources.add(root);
        }

        /** @return 根数据源，后续 ON、条件和投影使用该稳定引用。 */
        public JoinSource root() {
            return root;
        }

        /** 注册本次查询关系，可覆盖模型的同名声明；不改变模型，未引用时不解析目标。 */
        public Builder relation(String name, QueryRelation relation) {
            String key = Names.key(name, "query relation name");
            if (relatedSources != null && relatedSources.containsKey(key)) {
                throw new IllegalStateException("query relation has already been resolved: " + name);
            }
            if (relations == null) relations = new LinkedHashMap<>();
            if (relations.putIfAbsent(key, Objects.requireNonNull(relation, "query relation must not be null")) != null) {
                throw new IllegalArgumentException("duplicate query relation: " + name);
            }
            return this;
        }

        /** 解析声明关系的最终来源；重复引用只添加一次关系链。 */
        public JoinSource related(String name) {
            String key = Names.key(name, "query relation name");
            QueryRelation local = relations == null ? null : relations.get(key);
            QueryRelation relation = local == null ? root.form().relations().get(key) : local;
            if (relation == null) throw new IllegalArgumentException("unknown query relation: " + name);
            if (relatedSources == null) relatedSources = new LinkedHashMap<>();
            return relatedSources.computeIfAbsent(key, ignored -> relation.join(this, root));
        }

        /** 选择根字段或“关系名.字段”；“关系名.*”展开目标字段，沿用现有稳定结果别名。 */
        public Builder select(String path) {
            int separator = Objects.requireNonNull(path, "query selection must not be null").indexOf('.');
            JoinSource source = separator < 0 ? root : related(path.substring(0, separator));
            String field = path.substring(separator + 1);
            if (field.equals("*")) {
                for (DynamicField selected : source.form().fields()) select(source, selected.name());
                return this;
            }
            return select(source, field);
        }

        /** 关联选列与显式来源选列共用投影校验、解码及别名规则。 */
        public Builder selectAs(String path, String alias) {
            JoinFieldRef field = field(path);
            return selectAs(field.source(), field.field(), alias);
        }

        /** 解析根字段或“关系名.字段”；条件、投影和排序共享来源身份。 */
        public JoinFieldRef field(String path) {
            int separator = Objects.requireNonNull(path, "query field must not be null").indexOf('.');
            return new JoinFieldRef(separator < 0 ? root : related(path.substring(0, separator)),
                    path.substring(separator + 1));
        }

        /** 设置跨来源条件；限定字段按关系声明解析，不按来源拆散 OR。 */
        public Builder where(ConditionGroup condition) {
            ConditionFrame frame = new ConditionFrame(Objects.requireNonNull(condition, "join where must not be null"), null);
            while (true) {
                if (frame.index == frame.group.children().size()) {
                    JoinCondition result = frame.children.isEmpty() ? null
                            : new JoinCondition.Group(frame.group.operator(), frame.children);
                    frame = frame.parent;
                    if (frame == null) {
                        where = result;
                        return this;
                    }
                    if (result != null) frame.children.add(result);
                } else {
                    ConditionNode node = frame.group.children().get(frame.index++);
                    if (node instanceof ConditionGroup group) {
                        frame = new ConditionFrame(group, frame);
                    } else {
                        TermCondition term = (TermCondition) node;
                        JoinFieldRef field = field(term.field());
                        TermCondition bound = field.field().equals(term.field()) ? term
                                : TermCondition.of(field.field(), term.operator(), term.value());
                        frame.children.add(JoinCondition.where(field.source(), ConditionGroup.and().add(bound).build()));
                    }
                }
            }
        }

        private static final class ConditionFrame {
            private final ConditionGroup group;
            private final ConditionFrame parent;
            private final List<JoinCondition> children = new ArrayList<>();
            private int index;

            private ConditionFrame(ConditionGroup group, ConditionFrame parent) {
                this.group = group;
                this.parent = parent;
            }
        }

        /** 使用新来源引用构建完整 ON；回调失败时不发布新来源。 */
        public JoinSource join(JoinType type, DynamicForm form, Function<JoinSource, JoinCondition> on) {
            JoinSource joined = new JoinSource(sources.size(), Objects.requireNonNull(form, "joined form must not be null"));
            JoinCondition condition = Objects.requireNonNull(
                    Objects.requireNonNull(on, "join ON factory must not be null").apply(joined),
                    "join ON condition must not be null");
            requireOnSources(joined, condition);
            JoinClause clause = new JoinClause(type, joined, condition);
            sources.add(joined);
            joins.add(clause);
            return joined;
        }

        /** 为指定连接追加一个完整 AND 条件，保留其中的 OR 分组。 */
        public Builder andOn(JoinSource joinedSource, JoinCondition condition) {
            JoinSource joined = requireSource(joinedSource);
            JoinCondition safeCondition = Objects.requireNonNull(condition, "join ON condition must not be null");
            requireOnSources(joined, safeCondition);
            for (int index = 0; index < joins.size(); index++) {
                JoinClause clause = joins.get(index);
                if (clause.source().equals(joined)) {
                    JoinCondition combined = JoinCondition.and(clause.condition(), safeCondition);
                    joins.set(index, new JoinClause(clause.type(), joined, combined));
                    return this;
                }
            }
            throw new IllegalArgumentException("join ON source must be a joined source");
        }

        private void requireOnSources(JoinSource joined, JoinCondition condition) {
            condition.forEachLeaf(leaf -> {
                if (leaf instanceof JoinCondition.Comparison comparison) {
                    requireOnSource(joined, comparison.left().source());
                    requireOnSource(joined, comparison.right().source());
                } else if (leaf instanceof JoinCondition.Values values) {
                    requireOnSource(joined, values.source());
                }
            });
        }

        private void requireOnSource(JoinSource joined, JoinSource source) {
            if (!source.equals(joined) && (source.ordinal() >= joined.ordinal() || !sources.contains(source))) {
                throw new IllegalArgumentException("join ON source must be current or a preceding query source");
            }
        }

        /**
         * 加入一个新数据源并创建首个等值 ON 条件。
         *
         * <p>同一表单或物理表可加入多次；每次返回的来源序号独立，后续操作须使用对应来源引用。</p>
         *
         * @return 新加入的数据源，可用于继续追加复合 ON、条件和投影
         */
        public JoinSource join(JoinType type,
                               DynamicForm form,
                               JoinSource leftSource,
                               String leftField,
                               String rightField) {
            JoinSource safeLeft = requireSource(leftSource);
            return join(type, form, joined -> JoinCondition.compare(
                    safeLeft.field(leftField), "=", joined.field(rightField)));
        }

        /** 为已经加入的连接源追加复合键等值条件。 */
        public Builder andOn(JoinSource joinedSource,
                             JoinSource leftSource,
                             String leftField,
                             String rightField) {
            JoinSource safeJoined = requireSource(joinedSource);
            JoinSource safeLeft = requireSource(leftSource);
            if (safeLeft.ordinal() >= safeJoined.ordinal()) {
                throw new IllegalArgumentException("join ON left source must precede the joined source");
            }
            return andOn(safeJoined, JoinCondition.compare(
                    safeLeft.field(leftField), "=", safeJoined.field(rightField)));
        }

        /** 设置某个源的参数化业务条件。 */
        public Builder where(JoinSource source, ConditionGroup where) {
            conditions.put(requireSource(source), Objects.requireNonNull(where, "join where must not be null"));
            return this;
        }

        /** 为某个源设置本次显式数据范围。 */
        public Builder scope(JoinSource source, DataScope scope) {
            scopes.put(requireSource(source), Objects.requireNonNull(scope, "join data scope must not be null"));
            return this;
        }

        /** 使用稳定的内部源前缀选择字段。 */
        public Builder select(JoinSource source, String field) {
            JoinSource safeSource = requireSource(source);
            JoinFieldRef reference = new JoinFieldRef(safeSource, field);
            String candidate = "s" + safeSource.ordinal() + "_" + reference.field();
            if (candidate.length() > JoinProjection.MAX_PORTABLE_ALIAS_LENGTH
                    || !candidate.matches("[A-Za-z_][A-Za-z0-9_]*")) {
                candidate = "s" + safeSource.ordinal() + "_f" + fieldOrdinal(reference);
            }
            if (aliases.contains(candidate.toLowerCase(Locale.ROOT))
                    && projections.stream().noneMatch(projection -> reference.equals(projection.field()))) {
                String prefix = "s" + safeSource.ordinal() + "_p";
                int suffix = 0;
                do {
                    candidate = prefix + suffix++;
                } while (aliases.contains(candidate.toLowerCase(Locale.ROOT)));
            }
            return selectAs(safeSource, reference.field(), candidate);
        }

        /** 使用调用方声明的安全结果别名选择字段。 */
        public Builder selectAs(JoinSource source, String field, String alias) {
            return addProjection(new JoinProjection(new JoinFieldRef(requireSource(source), field), alias));
        }

        /** 选择来源明确的参数化计算结果，来源必须已经加入本查询。 */
        public Builder select(ValueExpression expression, String alias) {
            requireExpressionSources(expression);
            return addProjection(new JoinProjection(null, alias, expression));
        }

        private void requireExpressionSources(ValueExpression expression) {
            Objects.requireNonNull(expression, "join expression must not be null").forEachField(field -> {
                if (field.source() == null) throw new IllegalArgumentException("join expression requires a source");
                requireSource(field.source());
            });
        }

        private Builder addProjection(JoinProjection projection) {
            String normalizedAlias = projection.alias().toLowerCase(Locale.ROOT);
            if (!aliases.add(normalizedAlias)) {
                throw new IllegalArgumentException("join projection alias must be unique");
            }
            projections.add(projection);
            return this;
        }

        /** 按根字段或声明的关系字段分组。 */
        public Builder groupBy(String... paths) {
            if (groups == null) groups = new LinkedHashSet<>();
            for (String path : paths) groups.add(field(path));
            return this;
        }

        /** 按已加入查询的来源字段分组。 */
        public Builder groupBy(JoinSource source, String... fields) {
            if (groups == null) groups = new LinkedHashSet<>();
            for (String field : fields) groups.add(new JoinFieldRef(requireSource(source), field));
            return this;
        }

        /** 追加结构化排序字段。 */
        public Builder orderBy(String path, PageSort.Direction direction) {
            JoinFieldRef field = field(path);
            return orderBy(field.source(), field.field(), direction);
        }

        /** 追加结构化排序字段。 */
        public Builder orderBy(JoinSource source, String field, PageSort.Direction direction) {
            orders.add(new JoinOrder(new JoinFieldRef(requireSource(source), field), direction));
            return this;
        }

        /** 追加来源明确的表达式排序，不增加结果列。 */
        public Builder orderBy(ValueExpression expression, PageSort.Direction direction) {
            requireExpressionSources(expression);
            if (expression instanceof ValueExpression.Field field) {
                return orderBy(field.source(), field.identity().name(), direction);
            }
            orders.add(new JoinOrder(null, direction, expression));
            return this;
        }

        /** 按每个字段自己的声明决定显示完整值或脱敏值。 */
        public Builder declaredDisplay() {
            sensitiveDisplayMode = SensitiveDisplayMode.DECLARED;
            return this;
        }

        /** 强制本次 JOIN 中所有已声明脱敏字段输出脱敏值。 */
        public Builder masked() {
            sensitiveDisplayMode = SensitiveDisplayMode.MASKED;
            return this;
        }

        /** 可信服务端代码显式要求本次 JOIN 返回完整敏感字段。 */
        public Builder showSensitive() {
            sensitiveDisplayMode = SensitiveDisplayMode.FULL;
            return this;
        }

        /** 创建可并发共享的查询规格。 */
        public JoinQuerySpec build() {
            if (projections.isEmpty()) {
                throw new IllegalStateException("join query must select at least one field");
            }
            return new JoinQuerySpec(this);
        }

        /** 创建供聚合等消费者使用的关系输入，不要求普通行投影。 */
        public JoinQuerySpec buildRelation() {
            return new JoinQuerySpec(this);
        }

        private int fieldOrdinal(JoinFieldRef reference) {
            if (fieldOrdinals == null) {
                fieldOrdinals = new HashMap<>();
            }
            Map<String, Integer> ordinals = fieldOrdinals.get(reference.source());
            if (ordinals == null) {
                List<DynamicField> fields = reference.source().form().fields();
                ordinals = new HashMap<>(Names.mapCapacity(fields.size()));
                int ordinal = 0;
                for (DynamicField field : fields) {
                    ordinals.put(field.name(), ordinal++);
                }
                fieldOrdinals.put(reference.source(), ordinals);
            }
            return ordinals.get(reference.field());
        }

        private JoinSource requireSource(JoinSource source) {
            JoinSource safeSource = Objects.requireNonNull(source, "join source must not be null");
            if (!sources.contains(safeSource)) {
                throw new IllegalArgumentException("join source is not part of the query");
            }
            return safeSource;
        }
    }
}
