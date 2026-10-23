package com.flying.orm.core.sql.expression;

import com.flying.orm.core.type.LogicalType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;

/**
 * 后端注册的函数：声明结果类型，用参数引用生成 SQL，不接触请求值。
 * 实现须无请求级共享状态；SQL 文本只来自可信后端代码。
 * @author wangr
 * @version v4.1.2
 */
@FunctionalInterface
public interface SqlExpressionFunction {
    /** 返回实际 SQL 结果类型；只有全 NULL 表达式可返回 null，随后从调用上下文推导。 */
    LogicalType render(Call call);

    /** 固定参数个数的数值函数；NULL 参数保留数据库语义。 */
    static SqlExpressionFunction numeric(int arguments, LogicalType resultType, Consumer<Call> renderer) {
        Objects.requireNonNull(renderer, "expression function renderer must not be null");
        if (!Objects.requireNonNull(resultType, "function result type must not be null").numeric()) {
            throw new IllegalArgumentException("numeric function must declare a numeric result type");
        }
        return call -> {
            call.requireArity(arguments);
            call.requireNumericArguments();
            renderer.accept(call);
            return resultType;
        };
    }

    /**
     * 一次函数规划的局部上下文；参数引用可重复和重排，绑定顺序随 SQL 出现位置生成。
     * @author wangr
     * @version v4.1.2
     */
    final class Call {
        private final String dialect;
        private final List<SqlExpressionCompiler.Node> arguments;
        private final List<Object> parts = new ArrayList<>();
        private boolean numericArguments;

        Call(String dialect, List<SqlExpressionCompiler.Node> arguments) {
            this.dialect = dialect;
            this.arguments = arguments;
        }

        public String dialect() {
            return dialect;
        }

        public int size() {
            return arguments.size();
        }

        /** NULL 字面量返回 null；OTHER 表示非空但未声明通用语义的值。 */
        public LogicalType type(int index) {
            return arguments.get(index).type();
        }

        public Call sql(String trustedSql) {
            parts.add(Objects.requireNonNull(trustedSql, "function SQL must not be null"));
            return this;
        }

        public Call argument(int index) {
            return argument(index, numericArguments ? LogicalType.DECIMAL : null);
        }

        /** 指定 NULL 输入的类型；非空参数保留自身类型，不作隐式转换。 */
        public Call argument(int index, LogicalType nullType) {
            SqlExpressionCompiler.Node argument = arguments.get(index);
            parts.add(argument.type() == null && nullType != null
                    ? new SqlExpressionCompiler.TypedArgument(argument, nullType) : argument);
            return this;
        }

        public void requireArity(int expected) {
            if (size() != expected) throw new IllegalArgumentException("expression function requires " + expected + " arguments");
        }

        public void requireNumericArguments() {
            numericArguments = true;
            for (SqlExpressionCompiler.Node argument : arguments) {
                if (argument.type() != null && !argument.type().numeric()) {
                    throw new IllegalArgumentException("expression function requires numeric arguments");
                }
            }
        }

        List<Object> parts() {
            return List.copyOf(parts);
        }
    }
}
