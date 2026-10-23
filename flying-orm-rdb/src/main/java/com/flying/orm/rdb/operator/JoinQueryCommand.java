package com.flying.orm.rdb.operator;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.join.JoinQuerySpec;
import com.flying.orm.core.join.JoinSource;
import com.flying.orm.core.join.JoinType;
import com.flying.orm.core.join.JoinCondition;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.join.QueryRelation;
import com.flying.orm.rdb.aggregate.AggregateSpec;

import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.BiFunction;

/**
 * 响应式与同步 DynamicForm JOIN 门面共享的单次命令状态。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
public abstract class JoinQueryCommand<S extends JoinQueryCommand<S>> {

    private final JoinQuerySpec.Builder spec;
    private final SqlRenderer renderer;
    private final Map<DynamicForm, JoinSource> sources = new IdentityHashMap<>();
    private final Map<JoinSource, ConditionGroup.Builder> conditions = new java.util.LinkedHashMap<>();
    private ConditionGroup.Builder where;
    private final JoinSource root;
    private JoinSource lastJoined;
    private JoinQuerySpec built;

    JoinQueryCommand(DynamicForm rootForm, SqlRenderer renderer) {
        this.spec = JoinQuerySpec.builder(Objects.requireNonNull(rootForm, "join root form must not be null"));
        this.renderer = Objects.requireNonNull(renderer, "join condition renderer must not be null");
        this.root = spec.root();
        sources.put(rootForm, root);
    }

    S join(JoinType type, DynamicForm form, String leftField, String rightField) {
        requireMutable();
        DynamicForm safeForm = Objects.requireNonNull(form, "joined form must not be null");
        if (sources.containsKey(safeForm)) {
            throw new IllegalArgumentException("join form identity is ambiguous; "
                    + "use JoinQuerySpec with explicit JoinSource references for self joins");
        }
        JoinSource joined = spec.join(type, safeForm, root, leftField, rightField);
        sources.put(safeForm, joined);
        lastJoined = joined;
        return self();
    }

    /** 为最近加入的源追加根源到该源的复合等值 ON。 */
    public S andOn(String leftField, String rightField) {
        requireMutable();
        if (lastJoined == null) {
            throw new IllegalStateException("join ON extension requires a preceding join");
        }
        spec.andOn(lastJoined, root, leftField, rightField);
        return self();
    }

    S join(JoinType type, DynamicForm form,
                          BiFunction<JoinSource, JoinSource, JoinCondition> on) {
        requireMutable();
        Objects.requireNonNull(form, "joined form must not be null");
        Objects.requireNonNull(on, "join ON factory must not be null");
        if (sources.containsKey(form)) {
            throw new IllegalArgumentException("join form identity is ambiguous; "
                    + "use JoinQuerySpec with explicit JoinSource references for self joins");
        }
        JoinSource joined = spec.join(type, form, source -> on.apply(root, source));
        sources.put(form, joined);
        lastJoined = joined;
        return self();
    }

    /** 注册查询关系；引用该关系的字段时才加入关联。 */
    public S relation(String name, QueryRelation relation) {
        requireMutable();
        spec.relation(name, relation);
        return self();
    }

    /** 选择根字段或“关系名.字段”，支持“*”展开。 */
    public S select(String... paths) {
        requireMutable();
        for (String path : paths) spec.select(path);
        return self();
    }

    /** 为根字段或“关系名.字段”指定结果别名。 */
    public S selectAs(String path, String alias) {
        requireMutable();
        spec.selectAs(path, alias);
        return self();
    }

    /** 使用稳定默认结果别名选择字段。 */
    public S select(DynamicForm form, String field) {
        requireMutable();
        spec.select(source(form), field);
        return self();
    }

    /** 引用已加入查询的来源字段。 */
    public ValueExpression field(DynamicForm form, String field) {
        return ValueExpression.field(source(form).field(field));
    }

    /** 选择参数化计算结果；与普通投影保持声明顺序。 */
    public S select(ValueExpression expression, String alias) {
        requireMutable();
        spec.select(expression, alias);
        return self();
    }

    /** 使用显式结果别名选择字段。 */
    public S selectAs(DynamicForm form, String field, String alias) {
        requireMutable();
        spec.selectAs(source(form), field, alias);
        return self();
    }

    /** 为指定源追加参数化 AND 条件。 */
    public S where(DynamicForm form, String field, String operator, Object value) {
        requireMutable();
        JoinSource source = source(form);
        conditions.computeIfAbsent(source, ignored -> ConditionGroup.and(renderer.terms()))
                  .where(field, operator, value);
        return self();
    }

    /** 追加可含“关系名.字段”的 AND 条件组，保留内部 OR 分组。 */
    public S where(ConditionGroup condition) {
        requireMutable();
        if (where == null) where = ConditionGroup.and(renderer.terms());
        where.add(Objects.requireNonNull(condition, "join where must not be null"));
        return self();
    }

    /** 按根字段或关系字段排序。 */
    public S orderBy(String path, PageSort.Direction direction) {
        requireMutable();
        spec.orderBy(path, direction);
        return self();
    }

    /** 为指定源追加本次可信数据范围。 */
    public S scope(DynamicForm form, DataScope scope) {
        requireMutable();
        spec.scope(source(form), scope);
        return self();
    }

    /** 按指定来源字段排序。 */
    public S orderBy(DynamicForm form, String field, PageSort.Direction direction) {
        requireMutable();
        spec.orderBy(source(form), field, direction);
        return self();
    }

    /** 追加升序字段。 */
    public S orderBy(ValueExpression expression, PageSort.Direction direction) {
        requireMutable();
        spec.orderBy(expression, direction);
        return self();
    }

    /** 使用字段声明的默认展示策略。 */
    public S declaredDisplay() {
        requireMutable();
        spec.declaredDisplay();
        return self();
    }

    /** 强制对已声明 masked 的投影字段执行脱敏。 */
    public S masked() {
        requireMutable();
        spec.masked();
        return self();
    }

    /** 显式请求完整敏感值；上层仍必须自行完成权限判断。 */
    public S showSensitive() {
        requireMutable();
        spec.showSensitive();
        return self();
    }

    protected abstract S self();

    /** 使用来源明确的复杂 ON 追加 INNER JOIN。 */
    public S join(DynamicForm form,
            BiFunction<JoinSource, JoinSource, JoinCondition> on) {
        return join(JoinType.INNER, form, on);
    }

    /** 使用来源明确的复杂 ON 追加 LEFT JOIN。 */
    public S leftJoin(DynamicForm form,
            BiFunction<JoinSource, JoinSource, JoinCondition> on) {
        return join(JoinType.LEFT, form, on);
    }

    /** 使用来源明确的复杂 ON 追加 RIGHT JOIN。 */
    public S rightJoin(DynamicForm form,
            BiFunction<JoinSource, JoinSource, JoinCondition> on) {
        return join(JoinType.RIGHT, form, on);
    }

    /** 使用来源明确的复杂 ON 追加 FULL OUTER JOIN；需方言声明支持。 */
    public S fullJoin(DynamicForm form,
            BiFunction<JoinSource, JoinSource, JoinCondition> on) {
        return join(JoinType.FULL, form, on);
    }

    /** 追加 FULL OUTER JOIN；需方言声明支持。 */
    public S fullJoin(DynamicForm form, String leftField, String rightField) {
        return join(JoinType.FULL, form, leftField, rightField);
    }

    /** 追加 INNER JOIN。 */
    public S join(DynamicForm form, String leftField, String rightField) {
        return join(JoinType.INNER, form, leftField, rightField);
    }

    /** 追加 LEFT OUTER JOIN。 */
    public S leftJoin(DynamicForm form, String leftField, String rightField) {
        return join(JoinType.LEFT, form, leftField, rightField);
    }

    /** 追加 RIGHT OUTER JOIN。 */
    public S rightJoin(DynamicForm form, String leftField, String rightField) {
        return join(JoinType.RIGHT, form, leftField, rightField);
    }

    /** 按根字段或“关系名.字段”升序。 */
    public S orderByAsc(String path) {
        return orderBy(path, PageSort.Direction.ASC);
    }

    /** 按根字段或“关系名.字段”降序。 */
    public S orderByDesc(String path) {
        return orderBy(path, PageSort.Direction.DESC);
    }

    /** 指定值按声明顺序优先，保留来源字段编码。 */
    public S orderByFirst(DynamicForm form, String field, Object... values) {
        return orderBy(field(form, field).priority(values), PageSort.Direction.ASC);
    }

    /** 指定值按声明顺序排在最后。 */
    public S orderByLast(DynamicForm form, String field, Object... values) {
        return orderBy(ValueExpression.lastMatch(field(form, field).priority(values)), PageSort.Direction.ASC);
    }

    public S orderByAsc(DynamicForm form, String field) {
        return orderBy(form, field, PageSort.Direction.ASC);
    }

    /** 追加降序字段。 */
    public S orderByDesc(DynamicForm form, String field) {
        return orderBy(form, field, PageSort.Direction.DESC);
    }

    JoinQuerySpec spec() {
        if (built == null) {
            prepareConditions();
            built = spec.build();
        }
        return built;
    }

    AggregateSpec aggregateSpec(Consumer<AggregateSpec.Builder> consumer) {
        Objects.requireNonNull(consumer, "aggregate configuration must not be null");
        if (built == null) {
            prepareConditions();
            built = spec.buildRelation();
        }
        AggregateSpec.Builder report = AggregateSpec.builder(built);
        consumer.accept(report);
        return report.build();
    }

    private JoinSource source(DynamicForm form) {
        JoinSource source = sources.get(Objects.requireNonNull(form, "join form must not be null"));
        if (source == null) {
            throw new IllegalArgumentException("join source is not part of the query");
        }
        return source;
    }

    private void prepareConditions() {
        conditions.forEach((source, condition) -> spec.where(source, condition.build()));
        if (where != null) spec.where(where.build());
    }

    private void requireMutable() {
        if (built != null) {
            throw new IllegalStateException("join query has already been built");
        }
    }
}
