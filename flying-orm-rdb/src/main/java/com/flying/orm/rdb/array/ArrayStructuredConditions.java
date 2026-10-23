package com.flying.orm.rdb.array;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.condition.TermRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.codec.ArrayValueCodec;
import com.flying.orm.rdb.form.StructuredConditionCustomizer;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.internal.condition.ConditionNodes;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * 把前端数组条件变成强类型内部值。字段类型只从 DynamicForm 读取，前端不能自报元素类型。
 *
 * @author wangr
 * @date 2026-08-01
 * @version v1.0
 */
public final class ArrayStructuredConditions implements StructuredConditionResolver,
                                                       StructuredConditionCustomizer {

    public static final String CONTAINS = "array-contains";
    public static final String CONTAINED_BY = "array-contained-by";
    public static final String OVERLAPS = "array-overlaps";
    public static final String NOT_CONTAINS = "array-not-contains";
    public static final String NOT_CONTAINED_BY = "array-not-contained-by";
    public static final String NOT_OVERLAPS = "array-not-overlaps";
    public static final String ANY_EQUALS = "array-any-eq";

    private static final Set<String> OPERATORS = Set.of(
            CONTAINS, CONTAINED_BY, OVERLAPS, NOT_CONTAINS, NOT_CONTAINED_BY, NOT_OVERLAPS, ANY_EQUALS);
    private static final TermRegistry GOVERNED_TERMS = ArrayTermHandlers.postgresql().terms();

    private ArrayStructuredConditions() {
    }

    public static ArrayStructuredConditions postgresql() {
        return new ArrayStructuredConditions();
    }

    public ConditionGroup compile(DynamicForm form, StructuredConditionInput input) {
        return compile(form, input, StructuredConditionPolicy.defaults());
    }

    @Override
    public ConditionGroup compileFields(DynamicForm form, Function<String, DynamicField> fields,
                                        StructuredConditionInput input, StructuredConditionPolicy policy) {
        return StructuredConditionResolver.composite(this).compileFields(form, fields, input, policy);
    }

    @Override
    public StructuredConditionInput adaptFields(DynamicForm form, Function<String, DynamicField> fields,
                                                StructuredConditionInput input) {
        return ConditionNodes.rewrite(input, term -> adaptTerm(fields, term));
    }

    @Override
    public StructuredConditionPolicy customize(StructuredConditionPolicy policy) {
        StructuredConditionPolicy safePolicy = StructuredConditionCustomizer.super.customize(policy);
        for (String operator : OPERATORS) {
            safePolicy = safePolicy.allowOperator(operator);
        }
        return safePolicy.withAdditionalTerms(GOVERNED_TERMS);
    }

    private StructuredConditionInput adaptTerm(Function<String, DynamicField> fields,
                                               StructuredConditionInput input) {
        String operator = normalize(input.operator());
        if (!OPERATORS.contains(operator)) {
            return input;
        }
        String name = Objects.requireNonNull(input.field(), "array condition field must not be null");
        DynamicField field = fields.apply(name);
        if (field == null || !field.databaseType().isArray()) {
            throw new IllegalArgumentException("array operator requires an SQL array field: " + name);
        }
        Object value = ANY_EQUALS.equals(operator)
                ? ArrayValueCodec.writeElement(input.value(), field.databaseType())
                : ArrayConditionValue.of(arrayValues(input.value()), field.databaseType());
        return new StructuredConditionInput(input.field(), operator, value, input.logic(), input.terms());
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
