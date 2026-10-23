package com.flying.orm.core.sql.expression;

import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.type.LogicalType;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.BiFunction;

/**
 * 共享表达式编译器：迭代解析类型与函数，再按最终 SQL 顺序发出参数。
 * 字段解析、值编码和用途治理由现有查询/写入规划器提供，不访问数据库。
 * @author wangr
 * @version v4.1.2
 */
public final class SqlExpressionCompiler {
    private SqlExpressionCompiler() { }

    /** 支持参数上下文类型；第二个参数仅为 NULL 的期望类型，不强转非空值。 */
    public static Operand compile(ValueExpression expression,
                                  Function<ValueExpression.Field, Operand> fields,
                                  BiFunction<ValueExpression.Value, LogicalType, Operand> values,
                                  SqlExpressionFunctions functions, String dialect) {
        Objects.requireNonNull(expression, "expression must not be null");
        Objects.requireNonNull(fields, "expression field resolver must not be null");
        Objects.requireNonNull(values, "expression value resolver must not be null");
        Objects.requireNonNull(functions, "expression functions must not be null");
        Objects.requireNonNull(dialect, "expression dialect must not be null");
        Map<ValueExpression, Node> resolved = new IdentityHashMap<>();
        Deque<Visit> pending = new ArrayDeque<>();
        pending.push(new Visit(expression, false));
        while (!pending.isEmpty()) {
            Visit visit = pending.pop();
            ValueExpression node = visit.expression();
            if (resolved.containsKey(node)) continue;
            if (node instanceof ValueExpression.Field field) {
                resolved.put(node, leaf(fields.apply(field)));
            } else if (node instanceof ValueExpression.IntegerConstant constant) {
                resolved.put(node, new Node(LogicalType.INTEGER, List.of(Integer.toString(constant.value()))));
            } else if (node instanceof ValueExpression.Value value) {
                Operand operand = values.apply(value, null);
                resolved.put(node, operand.type() == null
                        ? new Node(null, List.of(new NullParameter(value))) : leaf(operand));
            } else if (!visit.expanded()) {
                pending.push(new Visit(node, true));
                List<ValueExpression> children = node instanceof ValueExpression.Call call ? call.arguments()
                        : List.of(((ValueExpression.Arithmetic) node).left(), ((ValueExpression.Arithmetic) node).right());
                for (int index = children.size() - 1; index >= 0; index--) pending.push(new Visit(children.get(index), false));
            } else if (node instanceof ValueExpression.Arithmetic arithmetic) {
                Node left = resolved.get(arithmetic.left());
                Node right = resolved.get(arithmetic.right());
                SqlExpressionFunction.Call call = new SqlExpressionFunction.Call(dialect, List.of(left, right));
                call.requireNumericArguments();
                String operator = switch (arithmetic.operator()) {
                    case ADD -> " + ";
                    case SUBTRACT -> " - ";
                    case MULTIPLY -> " * ";
                    case DIVIDE -> " / ";
                };
                LogicalType type = numericType(left.type(), right.type());
                if (type == null) type = LogicalType.DECIMAL;
                if (arithmetic.operator() == ValueExpression.Operator.DIVIDE
                        && (dialect.equals("mysql") || dialect.equals("oracle")) && type != LogicalType.FLOAT) {
                    type = LogicalType.DECIMAL;
                }
                call.sql("(").argument(0, type).sql(operator).argument(1, type).sql(")");
                resolved.put(node, new Node(type, call.parts()));
            } else if (node instanceof ValueExpression.Call function) {
                List<Node> arguments = new ArrayList<>(function.arguments().size());
                for (ValueExpression argument : function.arguments()) arguments.add(resolved.get(argument));
                SqlExpressionFunction.Call call = new SqlExpressionFunction.Call(dialect, arguments);
                LogicalType type = functions.require(function.id()).render(call);
                resolved.put(node, new Node(type, call.parts()));
            }
        }
        Node root = resolved.get(expression);
        return new Operand(emit(root, values), root.type());
    }

    private static Node leaf(Operand operand) {
        return new Node(operand.type(), List.of(operand.fragment()));
    }

    static LogicalType numericType(LogicalType left, LogicalType right) {
        if (left == null) return right;
        if (right == null || left == right) return left;
        if (left == LogicalType.FLOAT || right == LogicalType.FLOAT) return LogicalType.FLOAT;
        if (left == LogicalType.DECIMAL || right == LogicalType.DECIMAL) return LogicalType.DECIMAL;
        if (left == LogicalType.BIG_INTEGER || right == LogicalType.BIG_INTEGER) return LogicalType.BIG_INTEGER;
        return LogicalType.INTEGER;
    }

    private static SqlFragment emit(Node root, BiFunction<ValueExpression.Value, LogicalType, Operand> values) {
        StringBuilder sql = new StringBuilder();
        List<Object> parameters = new ArrayList<>();
        Deque<Emission> pending = new ArrayDeque<>();
        pending.push(new Emission(root, null));
        while (!pending.isEmpty()) {
            Emission emission = pending.pop();
            Object part = emission.part();
            if (part instanceof String text) sql.append(text);
            else if (part instanceof SqlFragment fragment) {
                sql.append(fragment.sql());
                parameters.addAll(fragment.parameters());
            } else if (part instanceof Node node) {
                LogicalType inherited = node.type() == null ? emission.nullType() : null;
                for (int index = node.parts().size() - 1; index >= 0; index--) {
                    pending.push(new Emission(node.parts().get(index), inherited));
                }
            } else if (part instanceof TypedArgument argument) {
                pending.push(new Emission(argument.node(), argument.type()));
            } else if (part instanceof NullParameter parameter) {
                SqlFragment fragment = values.apply(parameter.value(), emission.nullType()).fragment();
                sql.append(fragment.sql());
                parameters.addAll(fragment.parameters());
            }
        }
        return new SqlFragment(sql.toString(), parameters);
    }

    /**
     * 编译结果或已解析叶子；type 为 null 表示尚需上下文类型的全 NULL 表达式。
     * @author wangr
     * @version v4.1.2
     */
    public record Operand(SqlFragment fragment, LogicalType type) {
        public Operand {
            fragment = Objects.requireNonNull(fragment, "expression SQL fragment must not be null");
        }
    }

    record Node(LogicalType type, List<Object> parts) { }
    record TypedArgument(Node node, LogicalType type) { }
    private record NullParameter(ValueExpression.Value value) { }
    private record Emission(Object part, LogicalType nullType) { }
    private record Visit(ValueExpression expression, boolean expanded) { }
}
