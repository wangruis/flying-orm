package com.flying.orm.core.join;

import com.flying.orm.core.condition.ConditionGroup;
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

/**
 * 轻量多表查询的不可变结构规格。
 *
 * <p>规格只保存已经通过 DynamicForm 校验的数据源、字段引用、等值 ON、条件、Scope、投影和排序，
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
    private final Map<JoinSource, ConditionGroup> conditions;
    private final Map<JoinSource, DataScope> scopes;
    private final SensitiveDisplayMode sensitiveDisplayMode;

    private JoinQuerySpec(Builder builder) {
        this.root = builder.root;
        this.sources = List.copyOf(builder.sources);
        this.joins = List.copyOf(builder.joins);
        this.projections = List.copyOf(builder.projections);
        this.orders = List.copyOf(builder.orders);
        this.conditions = Map.copyOf(builder.conditions);
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

    /** 返回目标源的业务条件；没有条件时返回空 AND 组。 */
    public ConditionGroup where(JoinSource source) {
        JoinSource safeSource = Objects.requireNonNull(source, "join condition source must not be null");
        return conditions.getOrDefault(safeSource, EMPTY);
    }

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
        private final Map<JoinSource, ConditionGroup> conditions = new LinkedHashMap<>();
        private final Map<JoinSource, DataScope> scopes = new LinkedHashMap<>();
        private final Set<String> aliases = new LinkedHashSet<>();
        /** 仅回退别名按源建立物理序号索引；短别名路径不分配或扫描字段。 */
        private Map<JoinSource, Map<String, Integer>> fieldOrdinals;
        private SensitiveDisplayMode sensitiveDisplayMode = SensitiveDisplayMode.DECLARED;

        private Builder(DynamicForm rootForm) {
            this.root = new JoinSource(0, Objects.requireNonNull(rootForm, "join root form must not be null"));
            sources.add(root);
        }

        /** @return 根数据源，后续 ON、条件和投影使用该稳定引用。 */
        public JoinSource root() {
            return root;
        }

        /**
         * 加入一个新数据源并创建首个等值 ON 条件。
         *
         * @return 新加入的数据源，可用于继续追加复合 ON、条件和投影
         */
        public JoinSource join(JoinType type,
                               DynamicForm form,
                               JoinSource leftSource,
                               String leftField,
                               String rightField) {
            JoinSource safeLeft = requireSource(leftSource);
            DynamicForm safeForm = Objects.requireNonNull(form, "joined form must not be null");
            if (sources.stream().anyMatch(source -> source.form().table().equals(safeForm.table()))) {
                throw new IllegalArgumentException("join source must not be duplicated");
            }
            JoinSource joined = new JoinSource(sources.size(), safeForm);
            JoinFieldPair firstOn = new JoinFieldPair(new JoinFieldRef(safeLeft, leftField),
                                                      new JoinFieldRef(joined, rightField));
            JoinClause clause = new JoinClause(type, joined, List.of(firstOn));
            sources.add(joined);
            joins.add(clause);
            return joined;
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
            for (int index = 0; index < joins.size(); index++) {
                JoinClause clause = joins.get(index);
                if (clause.source().equals(safeJoined)) {
                    List<JoinFieldPair> on = new ArrayList<>(clause.on());
                    on.add(new JoinFieldPair(new JoinFieldRef(safeLeft, leftField),
                                             new JoinFieldRef(safeJoined, rightField)));
                    joins.set(index, new JoinClause(clause.type(), safeJoined, on));
                    return this;
                }
            }
            throw new IllegalArgumentException("join ON source must be a joined source");
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
            return selectAs(safeSource, reference.field(), candidate);
        }

        /** 使用调用方声明的安全结果别名选择字段。 */
        public Builder selectAs(JoinSource source, String field, String alias) {
            JoinProjection projection = new JoinProjection(new JoinFieldRef(requireSource(source), field), alias);
            String normalizedAlias = projection.alias().toLowerCase(Locale.ROOT);
            if (!aliases.add(normalizedAlias)) {
                throw new IllegalArgumentException("join projection alias must be unique");
            }
            projections.add(projection);
            return this;
        }

        /** 追加结构化排序字段。 */
        public Builder orderBy(JoinSource source, String field, PageSort.Direction direction) {
            orders.add(new JoinOrder(new JoinFieldRef(requireSource(source), field), direction));
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
