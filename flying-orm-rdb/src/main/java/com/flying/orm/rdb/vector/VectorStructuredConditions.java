package com.flying.orm.rdb.vector;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.condition.TermRegistry;
import com.flying.orm.core.condition.TermHandler;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.form.StructuredConditionCustomizer;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.internal.condition.ConditionNodes;

import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/**
 * 承接前端结构化 Vector 条件。字段类型和维度只相信 DynamicForm，前端只能提交向量值和数值阈值。
 * 三种度量的条件名为 vector-l2、vector-cosine、vector-inner-product 加 -eq/ne/lt/le/gt/ge。
 * L2/余弦传 distance，内积传 similarity；均使用真实数值，不替调用方归一化阈值。
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class VectorStructuredConditions implements StructuredConditionResolver, StructuredConditionCustomizer {

    public static final String L2_LESS_THAN = "vector-l2-lt";
    public static final String COSINE_LESS_THAN = "vector-cosine-lt";
    public static final String INNER_PRODUCT_GREATER_THAN = "vector-inner-product-gt";

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
        StructuredConditionPolicy customized = StructuredConditionCustomizer.super.customize(policy);
        for (TermHandler handler : GOVERNED_TERMS.handlers()) {
            customized = customized.allowOperator(handler.id());
        }
        return customized.withAdditionalTerms(GOVERNED_TERMS);
    }

    private StructuredConditionInput adaptTerm(Function<String, DynamicField> fields,
                                               StructuredConditionInput input) {
        String operator = normalize(input.operator());
        if (operator.isEmpty() || GOVERNED_TERMS.find(operator).isEmpty()) {
            return input;
        }
        String name = Objects.requireNonNull(input.field(), "vector condition field must not be null");
        DynamicField field = fields.apply(name);
        if (field == null || field.databaseType().isArray() || field.databaseType().logicalType() != LogicalType.VECTOR) {
            throw new IllegalArgumentException("vector operator requires a VECTOR field: " + name);
        }
        VectorMetric metric = metric(operator);
        VectorConditionValue conditionValue = conditionValue(input.value(), field.length(), metric);
        return new StructuredConditionInput(input.field(), operator, conditionValue, input.logic(), input.terms());
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
        return switch (operator.substring(0, operator.lastIndexOf('-'))) {
            case "vector-l2" -> VectorMetric.L2;
            case "vector-cosine" -> VectorMetric.COSINE;
            case "vector-inner-product" -> VectorMetric.INNER_PRODUCT;
            default -> throw new IllegalArgumentException("unsupported vector operator: " + operator);
        };
    }

    private static String normalize(String operator) {
        return operator == null ? "" : operator.trim().toLowerCase(Locale.ROOT);
    }
}
