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
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Consumer;

/**
 * 构造并读取 ConditionGroup 唯一的内部执行视图。
 *
 * @author wangr
 * @version v3.1
 */
public final class ConditionExecutionViews {

    private static final StableDigest.Domain SHAPE_DOMAIN = StableDigest.domain("condition-plan-shape/v2");

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
        view.parameterSources().forEach(new ParameterBinder(parameters, safeCodecs));
        return parameters.publish();
    }

    /** Per execution state; shared source segments never carry encoded or mutable request values. */
    private static final class ParameterBinder implements Consumer<Object> {
        private final OwnedBindableValues.Buffer parameters;
        private final ValueCodecRegistry codecs;
        private Class<?> previousType;
        private boolean requiresSnapshot;

        private ParameterBinder(OwnedBindableValues.Buffer parameters, ValueCodecRegistry codecs) {
            this.parameters = parameters;
            this.codecs = codecs;
        }

        @Override
        public void accept(Object source) {
            if (source != null && source.getClass() != previousType) {
                previousType = source.getClass();
                requiresSnapshot = BindableValueSnapshots.requiresImmutableSnapshot(source);
            }
            Object value = requiresSnapshot ? BindableValueSnapshots.logicalValue(source) : source;
            parameters.add(codecs.write(value));
        }
    }

    public static ConditionExecutionView compile(LogicalOperator operator,
                                                 List<ConditionNode> children,
                                                 Function<TermCondition, Object> ownedValueReader) {
        Objects.requireNonNull(operator, "logical operator must not be null");
        Objects.requireNonNull(children, "condition children must not be null");
        Objects.requireNonNull(ownedValueReader, "owned condition value reader must not be null");
        if (children.size() == 1 && children.getFirst() instanceof TermCondition term) {
            ConditionExecutionView single = singleTerm(operator, term, ownedValueReader);
            if (single != null) return single;
        }
        StableEncoder shape = StableDigest.sha256(SHAPE_DOMAIN);
        Mask mask = new Mask();
        List<Object> parameters = appendGroup(shape, operator, children, ownedValueReader, mask);
        return new ConditionExecutionView(
                shape.finishHex(), parameters, mask.required, mask.cacheable);
    }

    /** 单项标量/无参条件直接编码结构；固定前缀与枚举编号分隔，末尾完整保留字段，不含参数值。 */
    private static ConditionExecutionView singleTerm(LogicalOperator operator, TermCondition term,
                                                       Function<TermCondition, Object> ownedValueReader) {
        int index = STRUCTURAL_TERMS.indexOf(term.operator());
        if (index < 0) return null;
        switch (term.operator()) {
            case "in", "not-in", "between", "not-between" -> { return null; }
            default -> { }
        }
        List<Object> parameters = switch (term.operator()) {
            case "is-null", "is-not-null" -> List.of();
            default -> Collections.singletonList(ownedValueReader.apply(term));
        };
        // T: 不会与复杂条件的十六进制摘要重叠；字段中的分隔符不会改变前面的固定结构。
        String shape = "T:" + operator.name() + ":" + index + ":" + term.field();
        return new ConditionExecutionView(shape, parameters, 1L << index, true);
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

    private static List<Object> appendGroup(StableEncoder shape,
                                    LogicalOperator operator,
                                    List<ConditionNode> children,
                                    Function<TermCondition, Object> ownedValueReader,
                                    Mask mask) {
        List<Object> parameters = new ArrayList<>();
        List<List<Object>> segments = null;
        shape.marker(operator.name());
        for (ConditionNode child : children) {
            List<Object> shared = null;
            if (child instanceof ConditionGroup group) {
                ConditionExecutionView nested = of(group);
                shape.text("N", nested.shapeDigest());
                shared = nested.parameterSources();
                mask.required |= nested.requiredStandardTermMask();
                mask.cacheable = mask.cacheable && nested.structurallyCacheable();
            } else if (child instanceof TermCondition term) {
                shared = appendTerm(shape, term, parameters, ownedValueReader, mask);
            } else {
                mask.cacheable = false;
            }
            if (shared != null && !shared.isEmpty()) {
                if (segments == null) segments = new ArrayList<>();
                if (!parameters.isEmpty()) {
                    segments.add(Collections.unmodifiableList(parameters));
                    parameters = new ArrayList<>();
                }
                segments.add(shared);
            }
        }
        shape.marker("END");
        if (segments == null) return Collections.unmodifiableList(parameters);
        if (!parameters.isEmpty()) segments.add(Collections.unmodifiableList(parameters));
        return ConditionExecutionView.concatenate(segments);
    }

    @SuppressWarnings("unchecked")
    private static List<Object> appendTerm(StableEncoder shape,
                                   TermCondition term,
                                   List<Object> parameters,
                                   Function<TermCondition, Object> ownedValueReader,
                                   Mask mask) {
        Object value = ownedValueReader.apply(term);
        int termIndex = STRUCTURAL_TERMS.indexOf(term.operator());
        if (termIndex < 0) {
            mask.cacheable = false;
            return null;
        }
        mask.required |= 1L << termIndex;
        int before = parameters.size();
        List<Object> shared = null;
        switch (term.operator()) {
            case "is-null", "is-not-null" -> {
            }
            case "in", "not-in", "between", "not-between" -> {
                if (value instanceof List<?> values) shared = (List<Object>) values;
                else appendMultiValueParameters(parameters, value);
            }
            default -> parameters.add(value);
        }
        // F/N 与分组、标准运算符标签互斥；长度分帧保留字段边界，无需重复 TERM/OPERATOR 标签。
        shape.text("F", term.field())
             .integer(term.operator(), shared == null ? parameters.size() - before : shared.size());
        return shared;
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
