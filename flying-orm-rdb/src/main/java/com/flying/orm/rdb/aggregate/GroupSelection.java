package com.flying.orm.rdb.aggregate;

import com.flying.orm.core.internal.Names;
import com.flying.orm.core.expression.ValueExpression;

import java.util.Objects;

/**
 * 分组结果中的字段或计算表达式及其业务别名。
 *
 * @param field 普通字段名，与 expression 二选一，避免字段分组预先创建表达式和折叠键
 * @param expression 计算值或带关联来源的字段表达式
 * @param alias 结果别名
 * @author wangr
 * @version v3.2
 */
public record GroupSelection(String field, ValueExpression expression, String alias) {

    public GroupSelection {
        if ((field == null) == (expression == null)) {
            throw new IllegalArgumentException("group requires one field or expression");
        }
        if (field != null) field = Names.requireText(field, "field name");
        alias = Names.requireText(alias, "field name");
    }

    public static GroupSelection of(String field, String alias) {
        return new GroupSelection(Names.requireText(field, "field name"), null, alias);
    }

    public static GroupSelection of(ValueExpression operand, String alias) {
        Objects.requireNonNull(operand, "group expression must not be null");
        return operand instanceof ValueExpression.Field value && value.source() == null
                ? of(value.identity().name(), alias) : new GroupSelection(null, operand, alias);
    }

    public boolean computed() {
        return expression != null && !(expression instanceof ValueExpression.Field);
    }
}
