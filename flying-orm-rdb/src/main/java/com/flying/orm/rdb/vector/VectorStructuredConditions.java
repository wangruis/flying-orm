package com.flying.orm.rdb.vector;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.condition.TermRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.form.StructuredConditionCustomizer;
import com.flying.orm.rdb.form.StructuredConditionResolver;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 承接前端结构化 Vector 条件。字段类型和维度只相信 DynamicForm，前端只能提交向量值和数值阈值。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class VectorStructuredConditions implements StructuredConditionResolver, StructuredConditionCustomizer {

    public static final String L2_LESS_THAN = "vector-l2-lt";
    public static final String COSINE_LESS_THAN = "vector-cosine-lt";
    public static final String INNER_PRODUCT_GREATER_THAN = "vector-inner-product-gt";

    private static final Set<String> OPERATORS = Set.of(L2_LESS_THAN,
                                                         COSINE_LESS_THAN,
                                                         INNER_PRODUCT_GREATER_THAN);
    private static final TermRegistry GOVERNED_TERMS = VectorTermHandlers.postgresql().terms();

    private VectorStructuredConditions() {
    }

    public static VectorStructuredConditions postgresql() {
        return new VectorStructuredConditions();
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
    public StructuredConditionPolicy customize(StructuredConditionPolicy policy) {
        StructuredConditionPolicy customized = StructuredConditionCustomizer.super.customize(policy);
        for (String operator : OPERATORS) {
            customized = customized.allowOperator(operator);
        }
        return customized.withAdditionalTerms(GOVERNED_TERMS);
    }

    private StructuredConditionInput adaptNode(DynamicForm form, StructuredConditionInput input) {
        if (input.field() != null || input.operator() != null) {
            String operator = normalize(input.operator());
            if (!OPERATORS.contains(operator)) {
                return input;
            }
            DynamicField field = form.field(Objects.requireNonNull(input.field(),
                                                                    "vector condition field must not be null"));
            if (field.databaseType().isArray() || field.databaseType().logicalType() != LogicalType.VECTOR) {
                throw new IllegalArgumentException("vector operator requires a VECTOR field: " + field.name());
            }
            VectorMetric metric = metric(operator);
            VectorConditionValue conditionValue = conditionValue(input.value(), field.length(), metric);
            return new StructuredConditionInput(input.field(), operator, conditionValue, input.logic(), input.terms());
        }

        List<StructuredConditionInput> terms = new ArrayList<>(input.terms().size());
        for (StructuredConditionInput term : input.terms()) {
            terms.add(adaptNode(form, Objects.requireNonNull(term, "structured condition child must not be null")));
        }
        return new StructuredConditionInput(input.field(), input.operator(), input.value(), input.logic(), terms);
    }

    private static VectorConditionValue conditionValue(Object value,
                                                       Integer dimensions,
                                                       VectorMetric metric) {
        if (!(value instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException("vector condition value must contain vector and distance");
        }
        String thresholdName = metric == VectorMetric.INNER_PRODUCT ? "similarity" : "distance";
        if (map.size() != 2 || !map.containsKey("vector") || !map.containsKey(thresholdName)) {
            throw new IllegalArgumentException("vector condition only supports vector and " + thresholdName);
        }
        Object threshold = map.get(thresholdName);
        if (!(threshold instanceof Number number)) {
            throw new IllegalArgumentException("vector condition " + thresholdName + " must be a number");
        }
        double numericThreshold = number.doubleValue();
        if (metric != VectorMetric.INNER_PRODUCT && numericThreshold < 0D) {
            throw new IllegalArgumentException("vector distance must not be negative");
        }
        return new VectorConditionValue(VectorValueCodec.write(map.get("vector"), dimensions),
                                        numericThreshold,
                                        metric);
    }

    private static VectorMetric metric(String operator) {
        return switch (operator) {
            case L2_LESS_THAN -> VectorMetric.L2;
            case COSINE_LESS_THAN -> VectorMetric.COSINE;
            case INNER_PRODUCT_GREATER_THAN -> VectorMetric.INNER_PRODUCT;
            default -> throw new IllegalArgumentException("unsupported vector operator: " + operator);
        };
    }

    private static String normalize(String operator) {
        return operator == null ? "" : operator.trim().toLowerCase(Locale.ROOT);
    }
}
