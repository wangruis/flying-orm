package com.flying.orm.rdb.operator;

import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.lambda.EntityProperty;
import com.flying.orm.rdb.internal.value.ExpressionFieldValue;

/**
 * 实体表达式的字段和值入口，复用实体列名及枚举存储映射，不产生 SQL。
 * @param <T> 实体类型
 * @author wangr
 * @version v4.1.2
 */
public final class EntityExpressions<T> {
    private final EntityCommand<T, ?> state;

    EntityExpressions(EntityCommand<T, ?> state) {
        this.state = state;
    }

    /** Getter 解析为当前实体的数据库列。 */
    public ValueExpression field(EntityProperty<T, ?> property) {
        return ValueExpression.field(state.where().column(property));
    }

    /** 按属性映射逻辑值（含 @EnumValue）；SQL 编译时继续进行绑定值编码。 */
    public ValueExpression value(EntityProperty<T, ?> property, Object value) {
        String column = state.where().column(property);
        return ValueExpression.value(new ExpressionFieldValue(state.form().field(column), state.writeValue(column, value)));
    }

    /** 优先值按属性持久映射编码（含 @EnumValue），不把枚举名称直接写入 SQL 参数。 */
    public ValueExpression priority(EntityProperty<T, ?> property, Object... preferredValues) {
        java.util.Objects.requireNonNull(preferredValues, "preferred values must not be null");
        if (preferredValues.length == 0) throw new IllegalArgumentException("priority requires at least one preferred value");
        ValueExpression[] arguments = new ValueExpression[preferredValues.length + 1];
        arguments[0] = field(property);
        for (int index = 0; index < preferredValues.length; index++) arguments[index + 1] = value(property, preferredValues[index]);
        return ValueExpression.call("priority", arguments);
    }
}
