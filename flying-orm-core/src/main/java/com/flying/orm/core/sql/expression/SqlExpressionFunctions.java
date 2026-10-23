package com.flying.orm.core.sql.expression;

import com.flying.orm.core.internal.Names;
import com.flying.orm.core.sql.render.SqlIdentifiers;
import com.flying.orm.core.type.LogicalType;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 客户端装配时冻结的函数注册表；函数 ID 不会直接拼入 SQL。
 * @author wangr
 * @version v4.1.2
 */
public final class SqlExpressionFunctions {
    private static final SqlExpressionFunctions DEFAULTS = new Builder().build();
    private final Map<String, SqlExpressionFunction> functions;

    private SqlExpressionFunctions(Map<String, SqlExpressionFunction> functions) {
        this.functions = Map.copyOf(functions);
    }

    public static SqlExpressionFunctions defaults() {
        return DEFAULTS;
    }

    public static Builder builder() {
        return new Builder();
    }

    SqlExpressionFunction require(String id) {
        SqlExpressionFunction function = functions.get(id);
        if (function == null) throw new IllegalArgumentException("unknown expression function: " + id);
        return function;
    }

    private static LogicalType coalesce(SqlExpressionFunction.Call call) {
        if (call.size() < 2) throw new IllegalArgumentException("coalesce requires at least two arguments");
        LogicalType result = null;
        for (int index = 0; index < call.size(); index++) {
            LogicalType type = call.type(index);
            if (type != null) {
                if (result == null) result = type;
                else if (result.numeric() && type.numeric()) {
                    result = SqlExpressionCompiler.numericType(result, type);
                } else if (result != type) {
                    throw new IllegalArgumentException("coalesce arguments must have compatible types");
                }
            }
        }
        call.sql("coalesce(");
        for (int index = 0; index < call.size(); index++) {
            if (index > 0) call.sql(", ");
            call.argument(index, result);
        }
        call.sql(")");
        return result;
    }

    /** 按给定值顺序产生从零开始的优先级；未命中排在最后，NULL 沿用 SQL 等值比较语义。 */
    private static LogicalType priority(SqlExpressionFunction.Call call) {
        if (call.size() < 2) {
            throw new IllegalArgumentException("priority requires a source and at least one preferred value");
        }
        LogicalType type = call.type(0);
        for (int index = 1; type == null && index < call.size(); index++) type = call.type(index);
        call.sql("case ").argument(0, type);
        for (int index = 1; index < call.size(); index++) {
            call.sql(" when ").argument(index, type).sql(" then " + (index - 1));
        }
        call.sql(" else " + (call.size() - 1) + " end");
        return LogicalType.INTEGER;
    }

    private static LogicalType orderedMatches(SqlExpressionFunction.Call call, boolean first) {
        if (call.size() == 0 || call.size() % 2 != 0) {
            throw new IllegalArgumentException("ordered matches require source/value pairs");
        }
        call.sql("case");
        for (int index = 0; index < call.size(); index += 2) {
            LogicalType type = call.type(index);
            if (type == null) type = call.type(index + 1);
            call.sql(" when ").argument(index, type).sql(" = ").argument(index + 1, type)
                    .sql(" then " + (index / 2 + (first ? 0 : 1)));
        }
        call.sql(" else " + (first ? call.size() / 2 : 0) + " end");
        return LogicalType.INTEGER;
    }

    private static LogicalType mixedMatches(SqlExpressionFunction.Call call) {
        if (call.size() < 4 || (call.size() - 1) % 3 != 0) {
            throw new IllegalArgumentException("mixed matches require fallback and source/value/rank triples");
        }
        call.sql("case");
        for (int index = 1; index < call.size(); index += 3) {
            LogicalType type = call.type(index);
            if (type == null) type = call.type(index + 1);
            call.sql(" when ").argument(index, type).sql(" = ").argument(index + 1, type)
                    .sql(" then ").argument(index + 2, LogicalType.INTEGER);
        }
        call.sql(" else ").argument(0, LogicalType.INTEGER).sql(" end");
        return LogicalType.INTEGER;
    }

    /**
     * 默认包含 coalesce 和优先级表达式；同 ID 注册替换旧定义，已构建实例不受后续修改影响。
     * @author wangr
     * @version v4.1.2
     */
    public static final class Builder {
        private final Map<String, SqlExpressionFunction> functions = new LinkedHashMap<>();

        private Builder() {
            functions.put("coalesce", SqlExpressionFunctions::coalesce);
            functions.put("priority", SqlExpressionFunctions::priority);
            functions.put("priority-first-match", call -> orderedMatches(call, true));
            functions.put("priority-last-match", call -> orderedMatches(call, false));
            functions.put("priority-mixed-match", SqlExpressionFunctions::mixedMatches);
        }

        public Builder register(String id, SqlExpressionFunction function) {
            functions.put(Names.key(id, "expression function id"),
                    Objects.requireNonNull(function, "expression function must not be null"));
            return this;
        }

        /**
         * 注册普通函数调用：SQL 名称由后端声明，允许点分限定名；参数按原顺序绑定。
         * 参数类型用于 NULL 的绑定推导，不转换或限制非空值；特殊语法、变参和方言选择使用回调重载。
         */
        public Builder register(String id, String sqlName, LogicalType resultType, LogicalType... argumentTypes) {
            String prefix = SqlIdentifiers.requireIdentifier(sqlName) + "(";
            LogicalType result = Objects.requireNonNull(resultType, "function result type must not be null");
            List<LogicalType> signature = List.of(argumentTypes);
            return register(id, call -> {
                call.requireArity(signature.size());
                call.sql(prefix);
                for (int index = 0; index < signature.size(); index++) {
                    if (index > 0) call.sql(", ");
                    call.argument(index, signature.get(index));
                }
                call.sql(")");
                return result;
            });
        }

        public SqlExpressionFunctions build() {
            return new SqlExpressionFunctions(functions);
        }
    }
}
