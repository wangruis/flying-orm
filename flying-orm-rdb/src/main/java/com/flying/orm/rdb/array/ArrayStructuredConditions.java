package com.flying.orm.rdb.array;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionCompiler;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.codec.ArrayValueCodec;
import com.flying.orm.rdb.form.StructuredConditionCustomizer;
import com.flying.orm.rdb.form.StructuredConditionResolver;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 把前端数组条件变成强类型内部值。字段类型只从 DynamicForm 读取，前端不能自报元素类型。
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public final class ArrayStructuredConditions implements StructuredConditionResolver, StructuredConditionCustomizer {

    public static final String CONTAINS = "array-contains";
    public static final String CONTAINED_BY = "array-contained-by";
    public static final String OVERLAPS = "array-overlaps";
    public static final String ANY_EQUALS = "array-any-eq";

    private static final Set<String> OPERATORS = Set.of(CONTAINS, CONTAINED_BY, OVERLAPS, ANY_EQUALS);

    private ArrayStructuredConditions() {
    }

    public static ArrayStructuredConditions postgresql() {
        return new ArrayStructuredConditions();
    }

    public ConditionGroup compile(DynamicForm form, StructuredConditionInput input) {
        return compile(form, input, StructuredConditionPolicy.defaults());
    }

    @Override
    public ConditionGroup compile(DynamicForm form,
                                  StructuredConditionInput input,
                                  StructuredConditionPolicy policy) {
        return StructuredConditionResolver.composite(this).compile(form, input, policy);
    }

    @Override
    public StructuredConditionInput adapt(DynamicForm form, StructuredConditionInput input) {
        return adaptNode(Objects.requireNonNull(form, "dynamic form must not be null"),
                         Objects.requireNonNull(input, "structured condition input must not be null"));
    }

    @Override
    public void validate(DynamicForm form,
                         StructuredConditionInput input,
                         StructuredConditionPolicy policy) {
        StructuredConditionCustomizer.super.validate(form, input, policy);
        validateRawValues(input, policy);
    }

    @Override
    public StructuredConditionPolicy customize(StructuredConditionPolicy policy) {
        StructuredConditionPolicy safePolicy = StructuredConditionCustomizer.super.customize(policy);
        for (String operator : OPERATORS) {
            safePolicy = safePolicy.allowOperator(operator);
        }
        return safePolicy;
    }

    private StructuredConditionInput adaptNode(DynamicForm form, StructuredConditionInput input) {
        if (input.field() != null || input.operator() != null) {
            String operator = normalize(input.operator());
            if (!OPERATORS.contains(operator)) {
                return input;
            }
            DynamicField field = form.field(Objects.requireNonNull(input.field(),
                                                                    "array condition field must not be null"));
            if (!ArrayValueCodec.isArrayDataType(field.dataType())) {
                throw new IllegalArgumentException("array operator requires an SQL array field: " + field.name());
            }
            Object value = ANY_EQUALS.equals(operator)
                    ? ArrayValueCodec.writeElement(input.value(), field.dataType())
                    : ArrayConditionValue.of(arrayValues(input.value()), field.dataType());
            return new StructuredConditionInput(input.field(), operator, value, input.logic(), input.terms());
        }

        List<StructuredConditionInput> terms = new ArrayList<>(input.terms().size());
        for (StructuredConditionInput term : input.terms()) {
            terms.add(adaptNode(form, Objects.requireNonNull(term, "structured condition child must not be null")));
        }
        return new StructuredConditionInput(input.field(), input.operator(), input.value(), input.logic(), terms);
    }

    private void validateRawValues(StructuredConditionInput input, StructuredConditionPolicy policy) {
        String operator = normalize(input.operator());
        if (OPERATORS.contains(operator)) {
            StructuredConditionCompiler.validateStructure(
                    StructuredConditionInput.term("_array_value", "in", input.value()), policy);
            return;
        }
        for (StructuredConditionInput term : input.terms()) {
            validateRawValues(Objects.requireNonNull(term, "structured condition child must not be null"), policy);
        }
    }

    private Object arrayValues(Object value) {
        if (!(value instanceof Map<?, ?> map)) {
            return value;
        }
        if (map.size() != 1 || !map.containsKey("values")) {
            throw new IllegalArgumentException("array condition map only supports the values field");
        }
        return map.get("values");
    }

    private String normalize(String operator) {
        return operator == null ? "" : operator.trim().toLowerCase(Locale.ROOT);
    }
}
