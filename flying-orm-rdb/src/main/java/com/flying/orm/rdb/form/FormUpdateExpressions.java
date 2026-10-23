package com.flying.orm.rdb.form;

import com.flying.orm.core.expression.ValueExpression;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.FieldScope;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.scope.ScopeErrorCode;

import java.util.Map;
import java.util.function.Consumer;

/** UPDATE 表达式的来源检查；普通值不分配遍历状态，批量每行使用同一规则。 */
final class FormUpdateExpressions {
    private FormUpdateExpressions() { }

    static boolean present(Map<String, Object> values) {
        return values.values().stream().anyMatch(ValueExpression.class::isInstance);
    }

    static void validate(DynamicForm form, String target, Object value, FieldScope scope) {
        if (!(value instanceof ValueExpression expression)) return;
        requirePlain(form, form.field(target));
        expression.forEachField(source -> {
            if (source.source() != null) {
                throw new IllegalArgumentException("UPDATE expression requires fields from the current form");
            }
            DynamicField field = form.field(source.identity().name());
            requirePlain(form, field);
            if (!scope.canRead(field.name())) {
                throw new ScopeAccessException(ScopeErrorCode.FIELD_NOT_READABLE, form.id(), field.name(),
                        "UPDATE expression source is not readable: " + field.name());
            }
        });
    }

    static void requirePlain(DynamicForm form, DynamicField field) {
        if (form.protections().encrypted(field.name()).isPresent()) {
            throw new IllegalArgumentException("SQL expression cannot compute an encrypted field: " + field.name());
        }
    }

    static void sources(Map<String, Object> values, Consumer<String> consumer) {
        for (Object value : values.values()) {
            if (!(value instanceof ValueExpression expression)) continue;
            expression.forEachField(field -> {
                if (field.source() != null) {
                    throw new IllegalArgumentException("UPDATE expression requires fields from the current form");
                }
                consumer.accept(field.identity().name());
            });
        }
    }
}
