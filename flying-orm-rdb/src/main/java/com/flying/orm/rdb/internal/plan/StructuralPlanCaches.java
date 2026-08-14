package com.flying.orm.rdb.internal.plan;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionNode;
import com.flying.orm.core.condition.LogicalOperator;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.cache.BoundedCacheRegion;
import com.flying.orm.rdb.cache.OrmCachePolicy;
import com.flying.orm.rdb.cache.OrmCacheSnapshot;
import com.flying.orm.rdb.metadata.MetadataCacheInvalidator;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * 缓存可跨请求复用的 SQL 结构计划与条件结构计划。
 *
 * <p>SQL 计划的键只描述方言、表、操作、字段和条件等结构形状，不保存完整 SQL、参数值、实体实例、
 * 租户值、DataScope 值或条件树。命中缓存时不会调用完整 SQL 编译器；自定义 term 无法证明结构稳定时
 * 由调用方显式旁路，避免错误复用。</p>
 *
 * @author wangr
 * @date 2026-08-04
 * @version v1.0
 */
public final class StructuralPlanCaches implements MetadataCacheInvalidator {

    private static final Set<String> STRUCTURAL_TERMS = Set.of(
            "=", "!=", "<>", ">", ">=", "<", "<=", "like", "not-like",
            "like-ignore-case", "not-like-ignore-case",
            "in", "not-in", "between", "not-between", "is-null", "is-not-null");

    private static final int MAX_COLLECTION_VALUES = 1_000;

    private final BoundedCacheRegion<SqlPlanSpec, SqlStructurePlan> sqlPlans;
    private final BoundedCacheRegion<ConditionPlanKey, ConditionPlan> conditionPlans;

    private StructuralPlanCaches(OrmCachePolicy policy) {
        OrmCachePolicy safePolicy = Objects.requireNonNull(policy, "orm cache policy must not be null");
        this.sqlPlans = BoundedCacheRegion.create(safePolicy.sqlPlans(), StructuralPlanCaches::sqlWeight);
        this.conditionPlans = BoundedCacheRegion.create(safePolicy.conditionPlans(),
                                                        StructuralPlanCaches::conditionWeight);
    }

    /**
     * 按缓存策略创建结构计划缓存。
     *
     * @param policy 统一缓存策略
     * @return 独立的结构计划缓存实例
     */
    public static StructuralPlanCaches create(OrmCachePolicy policy) {
        return new StructuralPlanCaches(policy);
    }

    /**
     * 编译或复用完整 SQL 结构及 primitive 参数槽布局。
     *
     * <p>编译器仅在缓存未命中时调用；缓存关闭时每次调用。实现会验证编译结果的操作和表身份，阻止错误
     * 计划进入缓存。</p>
     *
     * @param spec 不含请求数据的 SQL 结构规格
     * @param compiler 完整 SQL 编译器
     * @return 可跨同形状请求复用的结构计划
     */
    public SqlStructurePlan sqlPlan(SqlPlanSpec spec, Supplier<SqlStructurePlan> compiler) {
        SqlPlanSpec safeSpec = Objects.requireNonNull(spec, "sql plan spec must not be null");
        Supplier<SqlStructurePlan> safeCompiler = Objects.requireNonNull(
                compiler, "sql plan compiler must not be null");
        return sqlPlans.get(safeSpec, ignored -> validatePlan(safeSpec, safeCompiler.get()));
    }

    /**
     * 按条件 AST 的字段、操作符、分组和集合槽位数复用条件计划，并从当前 AST 直接提取参数。
     *
     * <p>自定义 term 可能按值改变 SQL，无法证明结构稳定时明确旁路缓存，绝不错误复用。</p>
     *
     * @param dialect 方言名称
     * @param where 当前请求条件
     * @param renderer SQL 渲染器
     * @return 条件结构计划与当前请求参数
     */
    public ConditionStructurePlan condition(String dialect, ConditionGroup where, SqlRenderer renderer) {
        String safeDialect = requireText(dialect, "condition cache dialect");
        ConditionGroup safeWhere = stabilizeMultiValues(Objects.requireNonNull(
                where, "where condition must not be null"));
        SqlRenderer safeRenderer = Objects.requireNonNull(renderer, "condition renderer must not be null");
        GroupShape shape = shape(safeWhere);
        if (shape == null) {
            SqlFragment rendered = safeRenderer.renderWhere(safeWhere);
            ConditionPlan plan = new ConditionPlan(rendered.sql(), rendered.parameters().size());
            return new ConditionStructurePlan(plan, rendered.parameters(), "", false);
        }
        ConditionPlanKey key = new ConditionPlanKey(safeDialect, safeRenderer, shape);
        ConditionPlan plan = conditionPlans.get(key, ignored -> {
            SqlFragment rendered = safeRenderer.renderWhere(safeWhere);
            return new ConditionPlan(rendered.sql(), rendered.parameters().size());
        });
        List<Object> parameters = new ArrayList<>(plan.parameterCount());
        collectParameters(safeWhere, safeRenderer, parameters);
        return new ConditionStructurePlan(plan, parameters, signature(shape), true);
    }

    /**
     * 清理指定表的 SQL 计划；带 schema 时精确清理，不带 schema 时清理所有 schema 的同名表。
     *
     * @param table {@code table} 或 {@code schema.table}
     */
    public void invalidateTable(String table) {
        invalidate(table);
    }

    /**
     * 清理指定表的 SQL 计划；不带 schema 的表名匹配所有 schema。
     *
     * @param table {@code table} 或 {@code schema.table}
     */
    @Override
    public void invalidate(String table) {
        String normalized = SqlPlanSpec.normalizeTable(table);
        int separator = normalized.indexOf('.');
        if (separator >= 0) {
            invalidate(normalized.substring(0, separator), normalized.substring(separator + 1));
            return;
        }
        sqlPlans.invalidateIf(key -> key.physicalTable().equals(normalized));
    }

    /**
     * 精确清理指定 schema 下指定表的 SQL 计划，不影响其他 schema 的同名表。
     *
     * @param schema schema 名称
     * @param table 不带 schema 的表名
     */
    @Override
    public void invalidate(String schema, String table) {
        String normalizedSchema = requireIdentifierPart(schema, "plan cache schema");
        String normalizedTable = requireIdentifierPart(table, "plan cache table");
        sqlPlans.invalidateIf(key -> key.schema().equals(normalizedSchema)
                && key.physicalTable().equals(normalizedTable));
    }

    /** 清理 SQL 与条件结构计划。 */
    @Override
    public void invalidateAll() {
        sqlPlans.invalidateAll();
        conditionPlans.invalidateAll();
    }

    /** @return SQL 结构计划缓存统计快照。 */
    public OrmCacheSnapshot sqlSnapshot() {
        return sqlPlans.snapshot();
    }

    /** @return 条件结构计划缓存统计快照。 */
    public OrmCacheSnapshot conditionSnapshot() {
        return conditionPlans.snapshot();
    }

    private static int sqlWeight(SqlPlanSpec key, SqlStructurePlan plan) {
        return 4 + dividedLength(plan.sql().length(), 32)
                + dividedLength(key.table().length(), 16)
                + dividedLength(key.formFingerprint().length(), 32)
                + plan.parameterCount();
    }

    private static int conditionWeight(ConditionPlanKey key, ConditionPlan plan) {
        String sql = plan.sql();
        int placeholders = 0;
        for (int index = 0; index < sql.length(); index++) {
            if (sql.charAt(index) == '?') {
                placeholders++;
            }
        }
        return 2 + dividedLength(sql.length(), 32) + placeholders;
    }

    private static int dividedLength(int length, int divisor) {
        return Math.max(1, (length + divisor - 1) / divisor);
    }

    private static String requireText(String text, String name) {
        String safeText = Objects.requireNonNull(text, name + " must not be null");
        if (safeText.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return safeText;
    }

    private static String requireIdentifierPart(String text, String name) {
        String normalized = requireText(text, name).trim();
        if (normalized.indexOf('.') >= 0) {
            throw new IllegalArgumentException(name + " must not contain '.'");
        }
        return normalized;
    }

    private static SqlStructurePlan validatePlan(SqlPlanSpec spec, SqlStructurePlan plan) {
        SqlStructurePlan safePlan = Objects.requireNonNull(plan, "compiled sql structure plan must not be null");
        if (!spec.operation().equalsIgnoreCase(safePlan.operation())) {
            throw new IllegalArgumentException("compiled SQL plan operation does not match its structure spec");
        }
        if (!spec.table().equals(SqlPlanSpec.normalizeTable(safePlan.table()))) {
            throw new IllegalArgumentException("compiled SQL plan table does not match its structure spec");
        }
        return safePlan;
    }

    private static GroupShape shape(ConditionGroup group) {
        List<NodeShape> children = new ArrayList<>(group.children().size());
        for (ConditionNode child : group.children()) {
            if (child instanceof ConditionGroup nested) {
                GroupShape nestedShape = shape(nested);
                if (nestedShape == null) {
                    return null;
                }
                children.add(nestedShape);
            } else if (child instanceof TermCondition term) {
                if (!STRUCTURAL_TERMS.contains(term.operator())) {
                    return null;
                }
                children.add(new TermShape(term.field(), term.operator(), valueCount(term)));
            } else {
                return null;
            }
        }
        return new GroupShape(group.operator(), List.copyOf(children));
    }

    private static int valueCount(TermCondition term) {
        return switch (term.operator()) {
            case "is-null", "is-not-null" -> 0;
            case "in", "not-in", "between", "not-between" -> multiValues(term.value()).size();
            default -> 1;
        };
    }

    private static String signature(GroupShape shape) {
        StringBuilder result = new StringBuilder(32);
        appendShape(result, shape);
        return result.toString();
    }

    private static void appendShape(StringBuilder target, NodeShape shape) {
        if (shape instanceof GroupShape group) {
            target.append('G').append(group.operator().name()).append('[');
            for (NodeShape child : group.children()) {
                appendShape(target, child);
            }
            target.append(']');
            return;
        }
        TermShape term = (TermShape) shape;
        appendToken(target, term.field());
        appendToken(target, term.operator());
        target.append(term.parameterCount()).append(';');
    }

    private static void appendToken(StringBuilder target, String value) {
        String safeValue = Objects.requireNonNull(value, "condition shape token must not be null");
        target.append(safeValue.length()).append(':').append(safeValue).append(';');
    }

    private static void collectParameters(ConditionGroup group,
                                          SqlRenderer renderer,
                                          List<Object> target) {
        for (ConditionNode child : group.children()) {
            if (child instanceof ConditionGroup nested) {
                collectParameters(nested, renderer, target);
            } else if (child instanceof TermCondition term) {
                switch (term.operator()) {
                    case "is-null", "is-not-null" -> {
                    }
                    case "in", "not-in", "between", "not-between" -> multiValues(term.value()).stream()
                            .map(renderer.valueCodecs()::write)
                            .forEach(target::add);
                    default -> target.add(renderer.valueCodecs().write(term.value()));
                }
            }
        }
    }

    private static List<Object> multiValues(Object value) {
        if (value instanceof List<?> list) {
            requireCollectionSize(list.size());
            return new ArrayList<>(list);
        }
        if (value instanceof Iterable<?> iterable) {
            List<Object> result = new ArrayList<>();
            for (Object item : iterable) {
                requireCollectionSize(result.size() + 1);
                result.add(item);
            }
            return result;
        }
        if (value != null && value.getClass().isArray()) {
            int length = Array.getLength(value);
            requireCollectionSize(length);
            List<Object> result = new ArrayList<>(length);
            for (int index = 0; index < length; index++) {
                result.add(Array.get(value, index));
            }
            return result;
        }
        throw new IllegalArgumentException("multi-value condition must use iterable or array");
    }

    private static void requireCollectionSize(int size) {
        if (size > MAX_COLLECTION_VALUES) {
            throw new IllegalArgumentException(
                    "multi-value condition must not contain more than " + MAX_COLLECTION_VALUES + " values");
        }
    }

    /**
     * 只对标准多值 term 快照可能只能消费一次的 Iterable；List、数组和标量继续复用原对象。
     * 这样 shape、SQL 编译和参数抽取观察的是同一份不可变值，同时不增加普通热路径分配。
     */
    private static ConditionGroup stabilizeMultiValues(ConditionGroup group) {
        ConditionGroup.Builder rebuilt = group.operator() == LogicalOperator.AND
                ? ConditionGroup.and()
                : ConditionGroup.or();
        boolean changed = false;
        for (ConditionNode child : group.children()) {
            ConditionNode stableChild = child;
            if (child instanceof ConditionGroup nested) {
                stableChild = stabilizeMultiValues(nested);
                changed |= stableChild != nested;
            } else if (child instanceof TermCondition term
                    && STRUCTURAL_TERMS.contains(term.operator())
                    && ("in".equals(term.operator()) || "not-in".equals(term.operator())
                        || "between".equals(term.operator()) || "not-between".equals(term.operator()))
                    && term.value() instanceof Iterable<?> iterable
                    && !(term.value() instanceof List<?>)) {
                List<Object> snapshot = multiValues(iterable);
                stableChild = TermCondition.of(term.field(), term.operator(), List.copyOf(snapshot));
                changed = true;
            }
            rebuilt.add(stableChild);
        }
        return changed ? rebuilt.build() : group;
    }

    private sealed interface NodeShape permits GroupShape, TermShape {
    }

    private record GroupShape(LogicalOperator operator, List<NodeShape> children) implements NodeShape {
    }

    private record TermShape(String field, String operator, int parameterCount) implements NodeShape {
    }

    private record ConditionPlanKey(String dialect, SqlRenderer renderer, GroupShape shape) {
        private ConditionPlanKey {
            dialect = requireText(dialect, "condition cache dialect");
            renderer = Objects.requireNonNull(renderer, "condition cache renderer must not be null");
            shape = Objects.requireNonNull(shape, "condition shape must not be null");
        }
    }
}
