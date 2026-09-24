package com.flying.orm.rdb.internal.condition;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionNode;
import com.flying.orm.core.condition.LogicalOperator;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.rdb.internal.InternalApi;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

/**
 * 按条件出现顺序遍历和改写 AST，不消耗与树深度成正比的线程栈。
 *
 * <p>共享子树按每次出现独立访问；安全校验和扩展回调不能因对象复用而被跳过。</p>
 *
 * @author wangr
 * @version v4.1.1
 */
@InternalApi
public final class ConditionNodes {

    private ConditionNodes() {
    }

    /** 包含组和叶子的惰性先序遍历，仅保存当前路径上的迭代器。 */
    public static Iterable<ConditionNode> preorder(ConditionNode root) {
        ConditionNode safeRoot = Objects.requireNonNull(root, "condition node must not be null");
        return () -> new Iterator<>() {
            private final ArrayDeque<Iterator<ConditionNode>> path = new ArrayDeque<>();
            {
                path.push(List.of(safeRoot).iterator());
            }

            @Override
            public boolean hasNext() {
                while (!path.isEmpty() && !path.peek().hasNext()) path.pop();
                return !path.isEmpty();
            }

            @Override
            public ConditionNode next() {
                if (!hasNext()) throw new NoSuchElementException();
                ConditionNode node = path.peek().next();
                if (node instanceof ConditionGroup group) path.push(group.children().iterator());
                return node;
            }
        };
    }

    /** 原始结构化输入的先序遍历；只在实际访问到某个子节点时校验非空。 */
    public static Iterable<StructuredConditionInput> preorder(StructuredConditionInput root) {
        StructuredConditionInput safeRoot = Objects.requireNonNull(root, "structured condition input must not be null");
        return () -> new Iterator<>() {
            private final ArrayDeque<Iterator<StructuredConditionInput>> path = new ArrayDeque<>();
            {
                path.push(List.of(safeRoot).iterator());
            }

            @Override
            public boolean hasNext() {
                while (!path.isEmpty() && !path.peek().hasNext()) path.pop();
                return !path.isEmpty();
            }

            @Override
            public StructuredConditionInput next() {
                if (!hasNext()) throw new NoSuchElementException();
                StructuredConditionInput node = Objects.requireNonNull(path.peek().next(),
                        "structured condition child must not be null");
                if (!node.terms().isEmpty()) path.push(node.terms().iterator());
                return node;
            }
        };
    }

    /** 访问每个叶子，回调异常立即结束遍历。 */
    public static void forEachTerm(ConditionNode root, Consumer<TermCondition> action) {
        Objects.requireNonNull(action, "condition term action must not be null");
        for (ConditionNode node : preorder(root)) {
            if (node instanceof TermCondition term) action.accept(term);
        }
    }

    /** 命中第一个叶子后立即返回，不访问后续兄弟节点。 */
    public static boolean anyTerm(ConditionNode root, Predicate<TermCondition> predicate) {
        Objects.requireNonNull(predicate, "condition term predicate must not be null");
        for (ConditionNode node : preorder(root)) {
            if (node instanceof TermCondition term && predicate.test(term)) return true;
        }
        return false;
    }

    /** 保留逻辑分组和顺序；只有子节点身份发生变化的组才重新构造。 */
    public static ConditionGroup rewrite(ConditionGroup root, UnaryOperator<TermCondition> mapper) {
        Objects.requireNonNull(root, "condition group must not be null");
        Objects.requireNonNull(mapper, "condition term mapper must not be null");
        ArrayDeque<RewriteFrame> path = new ArrayDeque<>();
        path.push(new RewriteFrame(root));
        while (true) {
            RewriteFrame frame = path.peek();
            if (frame.index == frame.group.children().size()) {
                ConditionGroup result = frame.finish();
                path.pop();
                if (path.isEmpty()) return result;
                path.peek().accept(result);
            } else {
                ConditionNode child = frame.group.children().get(frame.index);
                if (child instanceof ConditionGroup group) {
                    path.push(new RewriteFrame(group));
                } else {
                    frame.accept(Objects.requireNonNull(mapper.apply((TermCondition) child),
                            "condition term mapper must not return null"));
                }
            }
        }
    }

    /**
     * 适配结构化 term，保留未变分组。与内置适配器一致，field/operator 任一非 null 即按 term
     * 处理；非法混合节点的 children 不在适配阶段下降，形状错误仍交给编译器报告。
     */
    public static StructuredConditionInput rewrite(StructuredConditionInput root,
                                                   UnaryOperator<StructuredConditionInput> mapper) {
        Objects.requireNonNull(root, "structured condition input must not be null");
        Objects.requireNonNull(mapper, "structured condition mapper must not be null");
        if (isStructuredTerm(root)) return mapStructuredTerm(root, mapper);
        ArrayDeque<StructuredRewriteFrame> path = new ArrayDeque<>();
        path.push(new StructuredRewriteFrame(root));
        while (true) {
            StructuredRewriteFrame frame = path.peek();
            if (frame.index == frame.input.terms().size()) {
                StructuredConditionInput result = frame.finish();
                path.pop();
                if (path.isEmpty()) return result;
                path.peek().accept(result);
            } else {
                StructuredConditionInput child = Objects.requireNonNull(frame.input.terms().get(frame.index),
                        "structured condition child must not be null");
                if (isStructuredTerm(child)) {
                    frame.accept(mapStructuredTerm(child, mapper));
                } else {
                    path.push(new StructuredRewriteFrame(child));
                }
            }
        }
    }

    private static boolean isStructuredTerm(StructuredConditionInput input) {
        return input.field() != null || input.operator() != null;
    }

    private static StructuredConditionInput mapStructuredTerm(StructuredConditionInput input,
                                                              UnaryOperator<StructuredConditionInput> mapper) {
        return Objects.requireNonNull(mapper.apply(input), "structured condition mapper must not return null");
    }

    private static final class StructuredRewriteFrame {
        private final StructuredConditionInput input;
        private int index;
        private List<StructuredConditionInput> changed;

        private StructuredRewriteFrame(StructuredConditionInput input) {
            this.input = input;
        }

        private void accept(StructuredConditionInput next) {
            List<StructuredConditionInput> children = input.terms();
            if (changed == null && next != children.get(index)) {
                changed = new ArrayList<>(children.size());
                changed.addAll(children.subList(0, index));
            }
            if (changed != null) changed.add(next);
            index++;
        }

        private StructuredConditionInput finish() {
            return changed == null ? input : new StructuredConditionInput(
                    input.field(), input.operator(), input.value(), input.logic(), changed);
        }
    }

    private static final class RewriteFrame {
        private final ConditionGroup group;
        private int index;
        private List<ConditionNode> changed;

        private RewriteFrame(ConditionGroup group) {
            this.group = group;
        }

        private void accept(ConditionNode next) {
            List<ConditionNode> children = group.children();
            if (changed == null && next != children.get(index)) {
                changed = new ArrayList<>(children.size());
                changed.addAll(children.subList(0, index));
            }
            if (changed != null) changed.add(next);
            index++;
        }

        private ConditionGroup finish() {
            if (changed == null) return group;
            ConditionGroup.Builder builder = group.operator() == LogicalOperator.AND
                    ? ConditionGroup.and() : ConditionGroup.or();
            changed.forEach(builder::add);
            return builder.build();
        }
    }
}
