package com.flying.orm.core.expression;

import com.flying.orm.core.field.FieldIdentity;
import com.flying.orm.core.internal.Names;
import com.flying.orm.core.internal.value.BindableValueSnapshots;
import com.flying.orm.core.join.JoinFieldRef;
import com.flying.orm.core.join.JoinSource;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Objects;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;
import java.util.function.Consumer;

/**
 * 字段、绑定值、函数和算术共用的表达式结构，不生成 SQL、不访问数据库。
 *
 * <p>函数 ID 由后端注册表解释，不能作为 SQL 文本拼接。字段类型、函数参数与结果类型、
 * 来源权限由使用表达式的规划器统一解析。表达式可复用；应用自定义绑定对象遵循现有可信
 * 标量约定，应由应用保证不可变，模型不猜测 JSON/集合或领域对象的语义。</p>
 *
 * @author wangr
 * @version v4.1.2
 */
public sealed interface ValueExpression {

    /** 引用当前查询来源的字段；规划时根据元数据解析，不把名称当 SQL。 */
    static ValueExpression field(String name) {
        return new Field(FieldIdentity.of(name), null);
    }

    /** 引用明确的关联来源，同表自关联不会丢失来源身份。 */
    static ValueExpression field(JoinFieldRef reference) {
        JoinFieldRef safe = Objects.requireNonNull(reference, "expression field reference must not be null");
        return new Field(FieldIdentity.of(safe.field()), safe.source());
    }

    /** 声明一个绑定值；null 也是绑定值，不代表省略表达式。 */
    static ValueExpression value(Object value) {
        return new Value(value);
    }

    /** 调用后端注册的函数；参数顺序和重复出现均原样保留。 */
    static ValueExpression call(String id, ValueExpression... arguments) {
        return new Call(id, List.of(arguments));
    }

    /** 按声明顺序访问全部字段来源；共享节点只访问一次，不受函数最终输出哪些参数影响。 */
    default void forEachField(Consumer<? super Field> consumer) {
        Objects.requireNonNull(consumer, "expression field consumer must not be null");
        Deque<ValueExpression> pending = new ArrayDeque<>();
        Set<ValueExpression> seen = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.push(this);
        while (!pending.isEmpty()) {
            ValueExpression node = pending.pop();
            if (!seen.add(node)) continue;
            if (node instanceof Field field) consumer.accept(field);
            else if (node instanceof Arithmetic arithmetic) {
                pending.push(arithmetic.right());
                pending.push(arithmetic.left());
            } else if (node instanceof Call call) {
                for (int index = call.arguments().size() - 1; index >= 0; index--) pending.push(call.arguments().get(index));
            }
        }
    }

    default ValueExpression add(ValueExpression right) {
        return new Arithmetic(Operator.ADD, this, right);
    }

    default ValueExpression subtract(ValueExpression right) {
        return new Arithmetic(Operator.SUBTRACT, this, right);
    }

    default ValueExpression multiply(ValueExpression right) {
        return new Arithmetic(Operator.MULTIPLY, this, right);
    }

    /** 使用数据库除法语义；不替调用方把除零或 NULL 改写成零。 */
    default ValueExpression divide(ValueExpression right) {
        return new Arithmetic(Operator.DIVIDE, this, right);
    }

    default ValueExpression coalesce(ValueExpression fallback) {
        return call("coalesce", this, fallback);
    }

    /** 按给定值的先后生成优先级；重复值取首次，null 遵循 SQL 等值语义。 */
    default ValueExpression priority(Object... preferredValues) {
        Objects.requireNonNull(preferredValues, "preferred values must not be null");
        if (preferredValues.length == 0) throw new IllegalArgumentException("priority requires at least one preferred value");
        ValueExpression[] arguments = new ValueExpression[preferredValues.length + 1];
        arguments[0] = this;
        for (int index = 0; index < preferredValues.length; index++) arguments[index + 1] = value(preferredValues[index]);
        return call("priority", arguments);
    }

    /** 按声明顺序取首个命中；priority 在未命中之前，lastMatch 分组在未命中之后。 */
    static ValueExpression firstMatch(ValueExpression... priorities) {
        for (ValueExpression expression : priorities) {
            if (expression instanceof Call call && call.id().equals("priority-last-match")) {
                return mixedMatches(priorities);
            }
        }
        return orderedMatches("priority-first-match", priorities);
    }

    /** 合并 priority 表达式，未命中排在前面；命中按声明顺序排列，首个命中生效。 */
    static ValueExpression lastMatch(ValueExpression... priorities) {
        return orderedMatches("priority-last-match", priorities);
    }

    private static ValueExpression orderedMatches(String id, ValueExpression[] priorities) {
        if (priorities.length == 0) throw new IllegalArgumentException("at least one priority expression is required");
        List<ValueExpression> arguments = new ArrayList<>();
        for (ValueExpression expression : priorities) {
            if (!(expression instanceof Call call) || !call.id().equals("priority") || call.arguments().size() < 2) {
                throw new IllegalArgumentException("ordered matches require priority expressions");
            }
            for (int index = 1; index < call.arguments().size(); index++) {
                arguments.add(call.arguments().getFirst());
                arguments.add(call.arguments().get(index));
            }
        }
        return new Call(id, arguments);
    }

    private static ValueExpression mixedMatches(ValueExpression[] priorities) {
        int count = 0;
        for (ValueExpression expression : priorities) {
            if (!(expression instanceof Call call) || call.arguments().size() < 2) {
                throw new IllegalArgumentException("mixed matches require priority or lastMatch groups");
            }
            if (call.id().equals("priority")) count += call.arguments().size() - 1;
            else if (call.id().equals("priority-last-match") && call.arguments().size() % 2 == 0) {
                count += call.arguments().size() / 2;
            } else throw new IllegalArgumentException("mixed matches require priority or lastMatch groups");
        }
        List<ValueExpression> arguments = new ArrayList<>();
        arguments.add(new IntegerConstant(count));
        int rank = 0;
        for (ValueExpression expression : priorities) {
            Call group = (Call) expression;
            boolean last = group.id().equals("priority-last-match");
            for (int index = 1; index < group.arguments().size(); index += last ? 2 : 1) {
                arguments.add(group.arguments().get(last ? index - 1 : 0));
                arguments.add(group.arguments().get(index));
                arguments.add(new IntegerConstant(last ? count + rank + 1 : rank));
                rank++;
            }
        }
        return new Call("priority-mixed-match", arguments);
    }

    /**
     * 结构整数（如排序排名），直接由编译器生成，不经业务值 codec；业务条件值使用 value。
     * 只允许 int，不接受 SQL 文本。
     * @author wangr
     * @version v4.1.2
     */
    record IntegerConstant(int value) implements ValueExpression { }

    /**
     * 字段身份与可选的关联来源；无来源表示由当前查询上下文解析。
     * @author wangr
     * @version v4.1.2
     */
    record Field(FieldIdentity identity, JoinSource source) implements ValueExpression {
        public Field {
            identity = Objects.requireNonNull(identity, "expression field identity must not be null");
            if (source != null) {
                identity = FieldIdentity.of(source.form().field(identity.name()).name());
            }
        }
    }

    /**
     * 尚未按字段或函数参数类型编码的逻辑值，复用核心已有快照规则。
     * @author wangr
     * @version v4.1.2
     */
    record Value(Object value) implements ValueExpression {
        public Value {
            value = BindableValueSnapshots.logicalValue(value);
        }

        @Override
        public Object value() {
            return BindableValueSnapshots.logicalValue(value);
        }
    }

    /**
     * 函数调用结构，ID 不是 SQL 函数文本；注册与类型检查属于规划器。
     * @author wangr
     * @version v4.1.2
     */
    final class Call implements ValueExpression {
        private final String id;
        private final List<ValueExpression> arguments;
        private final int hash;

        public Call(String id, List<ValueExpression> arguments) {
            this.id = Names.key(id, "expression function id");
            this.arguments = List.copyOf(Objects.requireNonNull(arguments, "expression arguments must not be null"));
            this.hash = 31 * this.id.hashCode() + this.arguments.hashCode();
        }

        public String id() {
            return id;
        }

        public List<ValueExpression> arguments() {
            return arguments;
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object other) {
            return structurallyEqual(this, other);
        }
    }

    /**
     * 有序二元算术节点；不提前计算值，也不改变 NULL 或数据库错误语义。
     * @author wangr
     * @version v4.1.2
     */
    final class Arithmetic implements ValueExpression {
        private final Operator operator;
        private final ValueExpression left;
        private final ValueExpression right;
        private final int hash;

        public Arithmetic(Operator operator, ValueExpression left, ValueExpression right) {
            this.operator = Objects.requireNonNull(operator, "expression operator must not be null");
            this.left = Objects.requireNonNull(left, "expression left operand must not be null");
            this.right = Objects.requireNonNull(right, "expression right operand must not be null");
            this.hash = (31 * operator.hashCode() + left.hashCode()) * 31 + right.hashCode();
        }

        public Operator operator() {
            return operator;
        }

        public ValueExpression left() {
            return left;
        }

        public ValueExpression right() {
            return right;
        }

        @Override
        public int hashCode() {
            return hash;
        }

        @Override
        public boolean equals(Object other) {
            return structurallyEqual(this, other);
        }
    }

    private static boolean structurallyEqual(ValueExpression expression, Object other) {
        if (expression == other) return true;
        if (!(other instanceof ValueExpression that)) return false;
        Deque<ValueExpression> pending = new ArrayDeque<>();
        pending.push(expression);
        pending.push(that);
        while (!pending.isEmpty()) {
            ValueExpression right = pending.pop();
            ValueExpression left = pending.pop();
            if (left == right) continue;
            if (left.getClass() != right.getClass() || left.hashCode() != right.hashCode()) return false;
            if (left instanceof Arithmetic a && right instanceof Arithmetic b) {
                if (a.operator != b.operator) return false;
                pending.push(a.left);
                pending.push(b.left);
                pending.push(a.right);
                pending.push(b.right);
            } else if (left instanceof Call a && right instanceof Call b) {
                if (!a.id.equals(b.id) || a.arguments.size() != b.arguments.size()) return false;
                for (int index = 0; index < a.arguments.size(); index++) {
                    pending.push(a.arguments.get(index));
                    pending.push(b.arguments.get(index));
                }
            } else if (!left.equals(right)) {
                return false;
            }
        }
        return true;
    }

    /**
     * 内置算术结构；SQL 符号由渲染器负责，不接受调用方 SQL 片段。
     * @author wangr
     * @version v4.1.2
     */
    enum Operator {
        ADD, SUBTRACT, MULTIPLY, DIVIDE
    }
}
