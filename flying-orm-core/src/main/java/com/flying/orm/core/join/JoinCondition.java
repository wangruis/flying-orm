package com.flying.orm.core.join;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.LogicalOperator;

import java.util.ArrayDeque;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * 来源明确的不可变关联条件，可用于 ON 或跨来源 WHERE。字段比较不接受 SQL，值条件沿用参数化 ConditionGroup。
 *
 * @author wangr
 * @version v4.1.2
 */
public sealed interface JoinCondition {

    /** 比较两个来源字段；支持 {@code =、<>、!=、>、>=、<、<=}。 */
    static JoinCondition compare(JoinFieldRef left, String operator, JoinFieldRef right) {
        return new Comparison(left, operator, right);
    }

    /** 将一个来源内的参数条件绑定到明确来源，所处 ON/WHERE 由查询规格决定。 */
    static JoinCondition where(JoinSource source, ConditionGroup condition) {
        return new Values(source, condition);
    }

    static JoinCondition and(JoinCondition... conditions) {
        return new Group(LogicalOperator.AND, List.of(conditions));
    }

    static JoinCondition or(JoinCondition... conditions) {
        return new Group(LogicalOperator.OR, List.of(conditions));
    }

    /** 按 SQL 顺序遍历叶子；深层分组不占用调用栈。 */
    default void forEachLeaf(Consumer<JoinCondition> consumer) {
        Objects.requireNonNull(consumer, "join condition visitor must not be null");
        anyLeaf(node -> {
            consumer.accept(node);
            return false;
        });
    }

    /** 按 SQL 顺序检查叶子，首次匹配即停止；深层分组不占用调用栈。 */
    default boolean anyLeaf(Predicate<JoinCondition> predicate) {
        Objects.requireNonNull(predicate, "join condition predicate must not be null");
        if (!(this instanceof Group)) return predicate.test(this);
        ArrayDeque<JoinCondition> pending = new ArrayDeque<>();
        pending.add(this);
        while (!pending.isEmpty()) {
            JoinCondition node = pending.removeLast();
            if (node instanceof Group group) {
                for (int index = group.children().size() - 1; index >= 0; index--) {
                    pending.addLast(group.children().get(index));
                }
            } else if (predicate.test(node)) {
                return true;
            }
        }
        return false;
    }

    /** 两个已注册字段的比较，不把右字段当成绑定值。 */
    record Comparison(JoinFieldRef left, String operator, JoinFieldRef right) implements JoinCondition {
        public Comparison {
            left = Objects.requireNonNull(left, "join comparison left field must not be null");
            right = Objects.requireNonNull(right, "join comparison right field must not be null");
            operator = Objects.requireNonNull(operator, "join comparison operator must not be null").trim();
            operator = switch (operator) {
                case "=", "<>", ">", ">=", "<", "<=" -> operator;
                case "!=" -> "<>";
                default -> throw new IllegalArgumentException("unsupported join field comparison operator");
            };
        }
    }

    /** 值快照及业务扩展由现有 ConditionGroup 所有者负责。 */
    record Values(JoinSource source, ConditionGroup condition) implements JoinCondition {
        public Values {
            source = Objects.requireNonNull(source, "join condition source must not be null");
            condition = Objects.requireNonNull(condition, "join value condition must not be null");
        }
    }

    /** 显式逻辑分组；空组没有连接含义，调用方应传入实际条件。 */
    record Group(LogicalOperator operator, List<JoinCondition> children) implements JoinCondition {
        public Group {
            operator = Objects.requireNonNull(operator, "join logical operator must not be null");
            children = List.copyOf(children);
            if (children.isEmpty()) {
                throw new IllegalArgumentException("join condition group must not be empty");
            }
        }
    }
}
