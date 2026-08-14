package com.flying.orm.core.condition;

import com.flying.orm.core.internal.condition.ConditionValueNormalizer;
import com.flying.orm.core.internal.condition.ConditionValuePolicy;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * 条件组保存一组结构化条件节点，允许嵌套表达 and/or 组合。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
public final class ConditionGroup implements ConditionNode {

    private static final int MAX_AST_DEPTH = 64;

    private static final int MAX_AST_NODES = 10_000;

    private final LogicalOperator operator;

    private final List<ConditionNode> children;

    private ConditionGroup(LogicalOperator operator, List<ConditionNode> children) {
        this.operator = Objects.requireNonNull(operator, "logical operator must not be null");
        this.children = List.copyOf(children);
    }

    /**
     * 创建 AND 条件组构建器。
     *
     * @return AND 条件组构建器
     */
    public static Builder and() {
        return new Builder(LogicalOperator.AND, TermRegistry.empty());
    }

    /**
     * 创建带业务 term 元数据的 AND 条件组构建器。
     */
    public static Builder and(TermRegistry terms) {
        return new Builder(LogicalOperator.AND, terms);
    }

    /**
     * 创建 OR 条件组构建器。
     *
     * @return OR 条件组构建器
     */
    public static Builder or() {
        return new Builder(LogicalOperator.OR, TermRegistry.empty());
    }

    /**
     * 创建带业务 term 元数据的 OR 条件组构建器。
     */
    public static Builder or(TermRegistry terms) {
        return new Builder(LogicalOperator.OR, terms);
    }

    /**
     * 返回当前条件组的逻辑操作符。
     *
     * @return 逻辑操作符
     */
    public LogicalOperator operator() {
        return operator;
    }

    /**
     * 返回只读子条件集合。
     *
     * @return 只读子条件集合
     */
    public List<ConditionNode> children() {
        return children;
    }

    /**
     * 条件组构建器，提供参数驱动 where 和嵌套 and/or 入口。
     *
     * @author wangr
     * @date 2026-07-21
     * @version v1.0
     */
    public static final class Builder {

        private final LogicalOperator operator;

        private final TermRegistry terms;

        private final List<ConditionNode> children = new ArrayList<>();

        private Builder(LogicalOperator operator, TermRegistry terms) {
            this.operator = Objects.requireNonNull(operator, "logical operator must not be null");
            this.terms = Objects.requireNonNull(terms, "term registry must not be null");
        }

        /**
         * 添加一个参数驱动 term 条件。
         *
         * @param field    字段名或属性名
         * @param operator term id
         * @param value    条件参数值
         * @return 当前构建器
         */
        public Builder where(String field, String operator, Object value) {
            return addTerm(field, operator, value, ConditionValuePolicy.REJECT_EMPTY);
        }

        /**
         * 添加可选条件。值清理后为空时不生成 AST 节点。
         */
        public Builder whereIfPresent(String field, String operator, Object value) {
            return addTerm(field, operator, value, ConditionValuePolicy.IGNORE_EMPTY);
        }

        /**
         * 添加数据库 IS NULL 条件，不绑定参数。
         */
        public Builder whereNull(String field) {
            return where(field, "is-null", null);
        }

        /**
         * 添加数据库 IS NOT NULL 条件，不绑定参数。
         */
        public Builder whereNotNull(String field) {
            return where(field, "is-not-null", null);
        }

        /**
         * 添加一个嵌套 AND 条件组。
         *
         * @param consumer 嵌套组构建逻辑
         * @return 当前构建器
         */
        public Builder and(Consumer<Builder> consumer) {
            return nested(LogicalOperator.AND, consumer);
        }

        /**
         * 添加一个嵌套 OR 条件组。
         *
         * @param consumer 嵌套组构建逻辑
         * @return 当前构建器
         */
        public Builder or(Consumer<Builder> consumer) {
            return nested(LogicalOperator.OR, consumer);
        }

        /**
         * 添加已构建的条件节点。
         *
         * @param node 条件节点
         * @return 当前构建器
         */
        public Builder add(ConditionNode node) {
            children.add(Objects.requireNonNull(node, "condition node must not be null"));
            return this;
        }

        /**
         * 构建只读条件组。
         *
         * @return 条件组
         */
        public ConditionGroup build() {
            ConditionGroup group = new ConditionGroup(operator, children);
            validateTree(group);
            return group;
        }

        private Builder nested(LogicalOperator nestedOperator, Consumer<Builder> consumer) {
            Objects.requireNonNull(consumer, "nested condition consumer must not be null");
            Builder builder = new Builder(nestedOperator, terms);
            consumer.accept(builder);
            ConditionGroup nested = builder.build();
            if (!nested.children().isEmpty()) {
                children.add(nested);
            }
            return this;
        }

        private Builder addTerm(String field,
                                String operator,
                                Object value,
                                ConditionValuePolicy policy) {
            String normalizedOperator = ConditionNames.normalize(operator, "condition operator");
            ConditionValueShape shape = TermRegistry.standard()
                                             .find(normalizedOperator)
                                             .or(() -> terms.find(normalizedOperator))
                                             .map(TermHandler::shape)
                                             .orElse(ConditionValueShape.SCALAR);
            ConditionValueNormalizer.Result result = ConditionValueNormalizer.normalize(shape, value, policy);
            if (result.present()) {
                children.add(TermCondition.of(field, normalizedOperator, result.value()));
            }
            return this;
        }

        private static void validateTree(ConditionGroup root) {
            Deque<NodeDepth> pending = new ArrayDeque<>();
            pending.push(new NodeDepth(root, 1));
            int nodes = 0;
            while (!pending.isEmpty()) {
                NodeDepth current = pending.pop();
                if (++nodes > MAX_AST_NODES) {
                    throw new IllegalArgumentException(
                            "condition AST node count must not exceed " + MAX_AST_NODES);
                }
                if (current.depth() > MAX_AST_DEPTH) {
                    throw new IllegalArgumentException(
                            "condition AST depth must not exceed " + MAX_AST_DEPTH);
                }
                if (current.node() instanceof ConditionGroup group) {
                    for (ConditionNode child : group.children()) {
                        pending.push(new NodeDepth(child, current.depth() + 1));
                    }
                }
            }
        }

        private record NodeDepth(ConditionNode node, int depth) {
        }
    }
}
