package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.ConditionNode;
import com.flying.orm.core.condition.ConditionValueShape;
import com.flying.orm.core.condition.LogicalOperator;
import com.flying.orm.core.condition.TermCondition;
import com.flying.orm.core.condition.TermHandler;
import com.flying.orm.core.condition.TermRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

/**
 * 把内建条件 term 的业务值转换为动态字段和当前方言真正需要的绑定值。
 *
 * <p>自定义 term 由自己的 handler 定义值语义，不能在这里按 Java 容器类型猜测并二次编码。
 * 普通标量值没有变化时保留原 AST 节点，避免给查询热路径增加无意义的对象分配。</p>
 *
 * @author wangr
 * @date 2026-08-17
 * @version v2.0
 */
final class FormConditionValueNormalizer {

    private final FormSqlRenderSupport support;

    FormConditionValueNormalizer(FormSqlRenderSupport support) {
        this.support = Objects.requireNonNull(support, "form SQL render support must not be null");
    }

    ConditionGroup normalize(DynamicForm form, ConditionGroup where) {
        return normalize(form, where, null);
    }

    /** HAVING keeps result aliases in the AST while encoding against their declared value source. */
    ConditionGroup normalize(DynamicForm form, ConditionGroup where,
                             Function<String, DynamicField> valueFieldResolver) {
        DynamicForm safeForm = Objects.requireNonNull(form, "dynamic form must not be null");
        ConditionGroup safeWhere = Objects.requireNonNull(where, "where condition must not be null");
        return normalizeGroup(safeForm, safeWhere, valueFieldResolver);
    }

    private ConditionGroup normalizeGroup(DynamicForm form, ConditionGroup group,
                                          Function<String, DynamicField> valueFieldResolver) {
        List<ConditionNode> children = group.children();
        List<ConditionNode> normalized = null;
        for (int index = 0; index < children.size(); index++) {
            ConditionNode child = children.get(index);
            ConditionNode next = child instanceof ConditionGroup nested
                    ? normalizeGroup(form, nested, valueFieldResolver)
                    : normalizeTerm(form, (TermCondition) child, valueFieldResolver);
            if (normalized == null && next != child) {
                normalized = new ArrayList<>(children.size());
                normalized.addAll(children.subList(0, index));
            }
            if (normalized != null) {
                normalized.add(next);
            }
        }
        if (normalized == null) {
            return group;
        }
        ConditionGroup.Builder builder = group.operator() == LogicalOperator.AND
                ? ConditionGroup.and() : ConditionGroup.or();
        normalized.forEach(builder::add);
        return builder.build();
    }

    private TermCondition normalizeTerm(DynamicForm form, TermCondition term,
                                         Function<String, DynamicField> valueFieldResolver) {
        DynamicField field = form.findField(term.field()).orElse(null);
        if (field == null) {
            Object termValue = term.value();
            FormDataScopes.TrustedScopeValue trusted = termValue instanceof FormDataScopes.TrustedScopeValue value
                    ? value : null;
            Object source = trusted == null ? termValue : trusted.value();
            if (trusted == null) {
                support.field(form, term.field());
            }
            return TermCondition.of(term.field(), term.operator(), source);
        }
        TermHandler handler = TermRegistry.standard().find(term.operator()).orElse(null);
        TermHandler configured = support.conditionTerms().find(term.operator()).orElse(null);
        if (handler == null || configured != handler) {
            return field.name().equals(term.field())
                    ? term
                    : TermCondition.of(field.name(), term.operator(), term.value());
        }
        DynamicField valueField = valueFieldResolver == null ? field
                : Objects.requireNonNull(valueFieldResolver.apply(field.name()),
                                         "condition value source field must not be null");
        support.requireStableOffsetTimeComparison(valueField, term.operator());
        if (field.name().equals(term.field())
                && (handler.shape() == ConditionValueShape.NONE
                || !support.requiresFieldAwareConditionEncoding(valueField))) {
            return term;
        }
        Object source = term.value();
        NormalizedValue normalized = switch (handler.shape()) {
            case NONE -> NormalizedValue.unchanged(source);
            case SCALAR -> normalizedScalar(valueField, source);
            case COLLECTION, RANGE -> normalizeValues(valueField, source);
            case SCALAR_OR_COLLECTION -> throw new IllegalStateException(
                    "standard condition term has an unsupported value shape");
        };
        if (field.name().equals(term.field()) && !normalized.changed()) {
            return term;
        }
        return TermCondition.of(field.name(), term.operator(), normalized.value());
    }

    private NormalizedValue normalizedScalar(DynamicField field, Object source) {
        Object normalized = support.writeConditionValue(field, source);
        return new NormalizedValue(normalized, normalized != source);
    }

    private NormalizedValue normalizeValues(DynamicField field, Object source) {
        List<?> values = values(source);
        List<Object> normalized = null;
        for (int index = 0; index < values.size(); index++) {
            Object value = values.get(index);
            Object encoded = support.writeConditionValue(field, value);
            if (normalized == null && encoded != value) {
                normalized = new ArrayList<>(values.size());
                normalized.addAll(values.subList(0, index));
            }
            if (normalized != null) {
                normalized.add(encoded);
            }
        }
        return normalized == null
                ? NormalizedValue.unchanged(source)
                : new NormalizedValue(Collections.unmodifiableList(normalized), true);
    }

    private static List<?> values(Object source) {
        if (source instanceof List<?> values) {
            return values;
        }
        if (source instanceof Iterable<?> iterable) {
            List<Object> values = new ArrayList<>();
            iterable.forEach(values::add);
            return values;
        }
        if (source != null && source.getClass().isArray()) {
            int length = Array.getLength(source);
            List<Object> values = new ArrayList<>(length);
            for (int index = 0; index < length; index++) {
                values.add(Array.get(source, index));
            }
            return values;
        }
        throw new IllegalArgumentException("multi-value condition requires an array or Iterable");
    }

    /** 标记值是否实际变化，使常见标准条件无需重建 AST。 */
    private record NormalizedValue(Object value, boolean changed) {

        private static NormalizedValue unchanged(Object value) {
            return new NormalizedValue(value, false);
        }
    }
}
