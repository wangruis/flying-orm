package com.flying.orm.core.internal.condition;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionNode;
import com.flying.orm.core.condition.LogicalOperator;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.condition.TermRegistry;
import com.flying.orm.core.internal.hash.StableDigest;
import com.flying.orm.core.internal.hash.StableEncoder;
import com.flying.orm.core.internal.value.BindableValueSnapshots;
import com.flying.orm.core.internal.value.OwnedBindableValues;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * 构造并读取 ConditionGroup 唯一的内部执行视图。
 *
 * @author wangr
 * @version v3.1
 */
public final class ConditionExecutionViews {

    private static final StableDigest.Domain SHAPE_DOMAIN = StableDigest.domain("condition-plan-shape/v1");

    private static final List<String> STRUCTURAL_TERMS = List.of(
            "=", "!=", "<>", ">", ">=", "<", "<=", "like", "not-like",
            "like-ignore-case", "not-like-ignore-case", "in", "not-in",
            "between", "not-between", "is-null", "is-not-null");

    private ConditionExecutionViews() {
    }

    public static ConditionExecutionView of(ConditionGroup group) {
        return Objects.requireNonNull(group, "condition group must not be null").executionView();
    }

    /** Binds the precomputed parameter sources without exposing mutable AST-owned values. */
    public static List<Object> bindParameters(ConditionGroup group, ValueCodecRegistry valueCodecs) {
        ConditionExecutionView view = of(group);
        ValueCodecRegistry safeCodecs = Objects.requireNonNull(
                valueCodecs, "condition value codecs must not be null");
        OwnedBindableValues.Buffer parameters = OwnedBindableValues.buffer(view.parameterCount());
        for (Object source : view.parameterSources()) {
            parameters.add(safeCodecs.write(BindableValueSnapshots.logicalValue(source)));
        }
        return parameters.publish();
    }

    public static ConditionExecutionView compile(LogicalOperator operator,
                                                 List<ConditionNode> children,
                                                 Function<TermCondition, Object> ownedValueReader) {
        StableEncoder shape = StableDigest.sha256(SHAPE_DOMAIN);
        List<Object> parameters = new ArrayList<>();
        Mask mask = new Mask();
        appendGroup(shape, Objects.requireNonNull(operator, "logical operator must not be null"),
                    Objects.requireNonNull(children, "condition children must not be null"), parameters,
                    Objects.requireNonNull(ownedValueReader, "owned condition value reader must not be null"),
                    mask);
        return new ConditionExecutionView(
                shape.finishHex(), parameters, mask.required, mask.cacheable);
    }

    public static long standardTermMask(TermRegistry terms) {
        TermRegistry safeTerms = Objects.requireNonNull(terms, "term registry must not be null");
        TermRegistry standard = TermRegistry.standard();
        long mask = 0L;
        for (int index = 0; index < STRUCTURAL_TERMS.size(); index++) {
            String operator = STRUCTURAL_TERMS.get(index);
            Object actual = safeTerms.find(operator).orElse(null);
            Object expected = standard.find(operator).orElse(null);
            if (actual != null && actual == expected) {
                mask |= 1L << index;
            }
        }
        return mask;
    }

    private static void appendGroup(StableEncoder shape,
                                    LogicalOperator operator,
                                    List<ConditionNode> children,
                                    List<Object> parameters,
                                    Function<TermCondition, Object> ownedValueReader,
                                    Mask mask) {
        shape.marker("GROUP_START").text("OPERATOR", operator.name());
        for (ConditionNode child : children) {
            if (child instanceof ConditionGroup group) {
                ConditionExecutionView nested = of(group);
                shape.marker("NESTED_GROUP").text("SHAPE", nested.shapeDigest());
                parameters.addAll(nested.parameterSources());
                mask.required |= nested.requiredStandardTermMask();
                mask.cacheable = mask.cacheable && nested.structurallyCacheable();
            } else if (child instanceof TermCondition term) {
                appendTerm(shape, term, parameters, ownedValueReader, mask);
            } else {
                mask.cacheable = false;
            }
        }
        shape.marker("GROUP_END");
    }

    private static void appendTerm(StableEncoder shape,
                                   TermCondition term,
                                   List<Object> parameters,
                                   Function<TermCondition, Object> ownedValueReader,
                                   Mask mask) {
        Object value = ownedValueReader.apply(term);
        int termIndex = STRUCTURAL_TERMS.indexOf(term.operator());
        if (termIndex < 0) {
            mask.cacheable = false;
            return;
        }
        mask.required |= 1L << termIndex;
        int before = parameters.size();
        switch (term.operator()) {
            case "is-null", "is-not-null" -> {
            }
            case "in", "not-in", "between", "not-between" ->
                    appendMultiValueParameters(parameters, value);
            default -> parameters.add(value);
        }
        shape.marker("TERM")
             .text("FIELD", term.field())
             .text("OPERATOR", term.operator())
             .integer("ARITY", parameters.size() - before);
    }

    private static void appendMultiValueParameters(List<Object> parameters, Object value) {
        if (value == null || !value.getClass().isArray()) {
            parameters.addAll((List<?>) value);
            return;
        }
        int length = Array.getLength(value);
        for (int index = 0; index < length; index++) {
            parameters.add(Array.get(value, index));
        }
    }

    private static final class Mask {
        private long required;
        private boolean cacheable = true;
    }
}
