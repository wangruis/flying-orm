package com.flying.orm.core.condition;

import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.internal.Names;
import com.flying.orm.core.internal.condition.ConditionValueNormalizer;
import com.flying.orm.core.internal.condition.ConditionValuePolicy;
import com.flying.orm.core.internal.condition.ConditionExecutionView;
import com.flying.orm.core.internal.condition.ConditionExecutionViews;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
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

    private final LogicalOperator operator;

    private final List<ConditionNode> children;

    private volatile ConditionExecutionView executionView;

    private ConditionGroup(LogicalOperator operator, List<ConditionNode> children) {
        this.operator = Objects.requireNonNull(operator, "logical operator must not be null");
        this.children = List.copyOf(children);
    }

    private ConditionGroup(LogicalOperator operator, List<ConditionNode> children, Owned owned) {
        this.operator = Objects.requireNonNull(operator, "logical operator must not be null");
        this.children = Collections.unmodifiableList(
                Objects.requireNonNull(children, "condition children must not be null"));
    }

    /** 包内编译链发布已经受节点预算约束、且不会再修改的子节点列表。 */
    static ConditionGroup owned(LogicalOperator operator, List<ConditionNode> children) {
        return new ConditionGroup(operator, children, Owned.INSTANCE);
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

    /** 仅供 SQL 编译内核按需读取并复用条件执行视图。 */
    public ConditionExecutionView executionView() {
        ConditionExecutionView current = executionView;
        if (current != null) {
            return current;
        }
        // 后序预热子组，编译当前组时只读取已发布的子视图；深度不再占用调用栈。
        ArrayDeque<ExecutionFrame> pending = new ArrayDeque<>();
        pending.addLast(new ExecutionFrame(this));
        while (!pending.isEmpty()) {
            ExecutionFrame frame = pending.getLast();
            if (frame.group.executionView != null) {
                pending.removeLast();
            } else if (frame.index < frame.group.children.size()) {
                ConditionNode child = frame.group.children.get(frame.index++);
                if (child instanceof ConditionGroup group && group.executionView == null) {
                    pending.addLast(new ExecutionFrame(group));
                }
            } else {
                frame.group.publishExecutionView();
                pending.removeLast();
            }
        }
        return executionView;
    }

    private void publishExecutionView() {
        // 只锁当前已经完成子组准备的节点，不沿树持有嵌套锁。
        synchronized (this) {
            if (executionView == null) {
                executionView = ConditionExecutionViews.compile(
                        operator, children, TermCondition::ownedValue);
            }
        }
    }

    private static final class ExecutionFrame {
        private final ConditionGroup group;
        private int index;

        private ExecutionFrame(ConditionGroup group) {
            this.group = group;
        }
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
            return where(FieldIdentity.of(field), operator, value);
        }

        /**
         * 添加一个复用既有字段身份的参数驱动条件。
         *
         * @param identity 字段身份
         * @param operator term id
         * @param value    条件参数值
         * @return 当前构建器
         */
        public Builder where(FieldIdentity identity, String operator, Object value) {
            return addTerm(identity, operator, value, ConditionValuePolicy.REJECT_EMPTY);
        }

        /**
         * 添加可选条件。值清理后为空时不生成 AST 节点。
         */
        public Builder whereIfPresent(String field, String operator, Object value) {
            return whereIfPresent(FieldIdentity.of(field), operator, value);
        }

        /**
         * 添加复用既有字段身份的可选条件。值清理后为空时不生成 AST 节点。
         */
        public Builder whereIfPresent(FieldIdentity identity, String operator, Object value) {
            return addTerm(identity, operator, value, ConditionValuePolicy.IGNORE_EMPTY);
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
            return snapshot();
        }

        private Builder nested(LogicalOperator nestedOperator, Consumer<Builder> consumer) {
            Objects.requireNonNull(consumer, "nested condition consumer must not be null");
            Builder builder = new Builder(nestedOperator, terms);
            consumer.accept(builder);
            ConditionGroup nested = builder.snapshot();
            if (!nested.children().isEmpty()) {
                children.add(nested);
            }
            return this;
        }

        private ConditionGroup snapshot() {
            return new ConditionGroup(operator, children);
        }

        private Builder addTerm(FieldIdentity identity,
                                String operator,
                                Object value,
                                ConditionValuePolicy policy) {
            String normalizedOperator = Names.key(operator, "condition operator");
            ConditionValueShape shape = TermRegistry.standard()
                                             .find(normalizedOperator)
                                             .or(() -> terms.find(normalizedOperator))
                                             .map(TermHandler::shape)
                                             .orElse(ConditionValueShape.SCALAR);
            ConditionValueNormalizer.Result result = ConditionValueNormalizer.normalize(
                    shape,
                    value,
                    policy,
                    (scalar, index) -> TermCondition.snapshotScalar(normalizedOperator, scalar),
                    Integer.MAX_VALUE,
                    Integer.MAX_VALUE);
            if (result.present()) {
                children.add(TermCondition.owned(identity, normalizedOperator, result.value()));
            }
            return this;
        }

    }

    private enum Owned {
        INSTANCE
    }
}
