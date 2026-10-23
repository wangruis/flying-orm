package com.flying.orm.core.join;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.internal.Names;
import com.flying.orm.core.form.DynamicForm;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Supplier;

/**
 * 可复用的查询关系声明，不是数据库外键或实体级联策略。
 * 关联键按“来源字段、目标字段”声明；选列与 EXISTS 使用同一份键和固定条件。
 * 多对多通过 {@link #then(QueryRelation)} 连接中间关系，不新增 SQL 执行链。
 *
 * @author wangr
 * @version v4.1.2
 */
public final class QueryRelation {
    private final Supplier<DynamicForm> target;
    private final List<Key> keys;
    private final ConditionGroup condition;
    private final JoinType type;
    private final QueryRelation next;

    private QueryRelation(Supplier<DynamicForm> target, List<Key> keys, ConditionGroup condition,
                          JoinType type, QueryRelation next) {
        this.target = target;
        this.keys = keys;
        this.condition = condition;
        this.type = type;
        this.next = next;
    }

    public static QueryRelation of(DynamicForm target, String sourceField, String targetField) {
        Objects.requireNonNull(target, "relation target must not be null");
        return of(() -> target, sourceField, targetField);
    }

    /** 目标按每次查询构建解析一次，可用于自关联和延迟注册的表单。 */
    public static QueryRelation of(Supplier<DynamicForm> target, String sourceField, String targetField) {
        return new QueryRelation(Objects.requireNonNull(target, "relation target supplier must not be null"),
                List.of(new Key(sourceField, targetField)), ConditionGroup.and().build(), JoinType.LEFT, null);
    }

    public QueryRelation andKey(String sourceField, String targetField) {
        List<Key> combined = new ArrayList<>(keys);
        combined.add(new Key(sourceField, targetField));
        return new QueryRelation(target, List.copyOf(combined), condition, type, next);
    }

    /** 固定条件在自动关联时置于 ON，保留 LEFT JOIN 的未匹配来源行。 */
    public QueryRelation where(ConditionGroup condition) {
        return new QueryRelation(target, keys, Objects.requireNonNull(condition, "relation condition must not be null"), type, next);
    }

    /** 设置选列时的关联类型；EXISTS 始终要求整条关系链实际存在。 */
    public QueryRelation joinType(JoinType type) {
        return new QueryRelation(target, keys, condition, Objects.requireNonNull(type, "relation join type must not be null"), next);
    }

    /** 将当前目标作为下一关系的来源；最终关系的字段可由关系名直接选取。 */
    public QueryRelation then(QueryRelation relation) {
        Objects.requireNonNull(relation, "next relation must not be null");
        return new QueryRelation(target, keys, condition, type, next == null ? relation : next.then(relation));
    }

    JoinSource join(JoinQuerySpec.Builder query, JoinSource source) {
        return join(query, source, false);
    }

    private JoinSource join(JoinQuerySpec.Builder query, JoinSource source, boolean exists) {
        JoinSource current = source;
        for (QueryRelation step = this; step != null; step = step.next) {
            QueryRelation relation = step;
            JoinSource left = current;
            current = query.join(exists ? JoinType.INNER : step.type, step.target.get(), right -> relation.on(left, right));
        }
        return current;
    }

    private JoinCondition on(JoinSource left, JoinSource right) {
        List<JoinCondition> conditions = new ArrayList<>(keys.size() + 1);
        for (Key key : keys) conditions.add(JoinCondition.compare(left.field(key.source()), "=", right.field(key.target())));
        if (!condition.children().isEmpty()) conditions.add(JoinCondition.where(right, condition));
        return conditions.size() == 1 ? conditions.getFirst() : JoinCondition.and(conditions.toArray(JoinCondition[]::new));
    }

    /** 生成供现有 relationExists/relationNotExists 使用的相关子查询，投影别名对应外层字段。 */
    public JoinQuerySpec query(ConditionGroup where) {
        Objects.requireNonNull(where, "relation query condition must not be null");
        JoinQuerySpec.Builder query = JoinQuerySpec.builder(target.get());
        JoinSource root = query.root();
        for (Key key : keys) query.selectAs(root, key.target(), key.source());
        query.where(root, condition);
        JoinSource selected = next == null ? root : next.join(query, root, true);
        query.where(selected, selected.equals(root)
                ? ConditionGroup.and().add(condition).add(where).build() : where);
        return query.build();
    }

    private record Key(String source, String target) {
        private Key {
            source = Names.requireText(source, "field name");
            target = Names.requireText(target, "field name");
        }
    }
}
