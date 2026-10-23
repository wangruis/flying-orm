package com.flying.orm.rdb.json;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.condition.StructuredConditionPolicy;
import com.flying.orm.core.condition.StructuredConditionException;
import com.flying.orm.core.condition.StructuredConditionErrorCode;
import com.flying.orm.core.condition.TermRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.type.LogicalType;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.rdb.form.StructuredConditionCustomizer;
import com.flying.orm.rdb.form.StructuredConditionResolver;
import com.flying.orm.rdb.internal.condition.ConditionNodes;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static com.flying.orm.rdb.json.JsonConditionValue.requireKind;

/**
 * 把前端 JSON 条件整理成经过校验的 JsonConditionValue，是结构化输入进入 JSON 方言渲染前的安全适配层。
 *
 * <p>这里只接受 operator、path、key 和 value，不接受 SQL 片段。路径被拆成属性名和数组下标，值先验证可编码性，
 * 值由方言 handler 参数化，路径由 handler 绑定或安全引用。DynamicForm 可用时还会确认目标字段确实是 JSON 类型。</p>
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public final class JsonStructuredConditions implements StructuredConditionResolver,
                                                      StructuredConditionCustomizer {

    public static final String JSON_PATH_EQUALS = "json-path-eq";
    public static final String JSON_PATH_NOT_EQUALS = "json-path-ne";
    public static final String JSON_PATH_GREATER_THAN = "json-path-gt";
    public static final String JSON_PATH_GREATER_OR_EQUALS = "json-path-ge";
    public static final String JSON_PATH_LESS_THAN = "json-path-lt";
    public static final String JSON_PATH_LESS_OR_EQUALS = "json-path-le";
    public static final String JSON_PATH_LIKE = "json-path-like";
    public static final String JSON_PATH_NOT_LIKE = "json-path-not-like";
    public static final String JSON_PATH_IS_NULL = "json-path-is-null";
    public static final String JSON_PATH_NOT_NULL = "json-path-not-null";
    public static final String JSON_PATH_IN = "json-path-in";
    public static final String JSON_PATH_NOT_IN = "json-path-not-in";
    public static final String JSON_CONTAINS = "json-contains";
    public static final String JSON_NOT_CONTAINS = "json-not-contains";
    public static final String JSON_CONTAINED_BY = "json-contained-by";
    public static final String JSON_NOT_CONTAINED_BY = "json-not-contained-by";
    public static final String JSON_EXISTS = "json-exists";
    public static final String JSON_NOT_EXISTS = "json-not-exists";
    public static final String JSON_ARRAY_CONTAINS = "json-array-contains";
    public static final String JSON_ARRAY_NOT_CONTAINS = "json-array-not-contains";

    private static final Set<String> BUILT_IN_OPERATORS = Set.of(JSON_PATH_EQUALS,
                                                                 JSON_PATH_NOT_EQUALS,
                                                                 JSON_PATH_GREATER_THAN,
                                                                 JSON_PATH_GREATER_OR_EQUALS,
                                                                 JSON_PATH_LESS_THAN,
                                                                 JSON_PATH_LESS_OR_EQUALS,
                                                                 JSON_PATH_LIKE,
                                                                 JSON_PATH_NOT_LIKE,
                                                                 JSON_PATH_IS_NULL,
                                                                 JSON_PATH_NOT_NULL,
                                                                 JSON_PATH_IN,
                                                                 JSON_PATH_NOT_IN,
                                                                 JSON_CONTAINS,
                                                                 JSON_NOT_CONTAINS,
                                                                 JSON_CONTAINED_BY,
                                                                 JSON_NOT_CONTAINED_BY,
                                                                 JSON_EXISTS,
                                                                 JSON_NOT_EXISTS,
                                                                 JSON_ARRAY_CONTAINS,
                                                                 JSON_ARRAY_NOT_CONTAINS);

    /** MySQL 与 PostgreSQL 的 JSON term 共享同一组治理描述器，冻结一次即可供输入编译复用。 */
    private static final TermRegistry GOVERNED_TERMS = JsonTermHandlers.mysql().terms();

    private static final JsonStructuredConditions STANDARD = new JsonStructuredConditions();

    private JsonStructuredConditions() {
    }

    /**
     * 返回无方言状态的 JSON 条件解析器。它只负责校验字段、路径和值；具体 SQL 由方言 term 包决定。
     */
    public static JsonStructuredConditions standard() {
        return STANDARD;
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
    public void validate(DynamicForm form, StructuredConditionInput input, StructuredConditionPolicy policy) {
        StructuredConditionCustomizer.super.validate(form, input, policy);
        if (policy.maxCollectionSize() == Integer.MAX_VALUE) {
            return;
        }
        for (StructuredConditionInput node : ConditionNodes.preorder(input)) {
            String operator = normalize(node.operator());
            if (!JSON_PATH_IN.equals(operator) && !JSON_PATH_NOT_IN.equals(operator)) {
                continue;
            }
            Object value = node.value();
            Object values = value instanceof JsonConditionValue condition ? condition.value()
                    : value instanceof Map<?, ?> map ? map.get("value") : null;
            int size = values instanceof Collection<?> collection ? collection.size()
                    : values != null && values.getClass().isArray() ? Array.getLength(values) : 0;
            if (size > policy.maxCollectionSize()) {
                throw StructuredConditionException.term(StructuredConditionErrorCode.VALUE_COLLECTION_TOO_LARGE,
                        null, node.field(), operator, "json path membership exceeds collection size");
            }
        }
    }

    @Override
    public StructuredConditionPolicy customize(StructuredConditionPolicy policy) {
        StructuredConditionPolicy safePolicy = StructuredConditionCustomizer.super.customize(policy);
        for (String operator : BUILT_IN_OPERATORS) {
            safePolicy = safePolicy.allowOperator(operator);
        }
        return safePolicy.withAdditionalTerms(GOVERNED_TERMS);
    }

    private StructuredConditionInput adaptTerm(Function<String, DynamicField> fields,
                                               StructuredConditionInput input) {
        String operator = normalize(input.operator());
        // 非 JSON operator 原样交给后续 resolver，多个 customizer 可以组合而不互相吞条件。
        if (!BUILT_IN_OPERATORS.contains(operator)) {
            return input;
        }
        String name = Objects.requireNonNull(input.field(), "json condition field must not be null");
        DynamicField field = fields.apply(name);
        if (field == null || field.databaseType().isArray() || field.databaseType().logicalType() != LogicalType.JSON) {
            throw new IllegalArgumentException("json operator requires a JSON field: " + name);
        }
        Object value = adaptValue(operator, input.value());
        return new StructuredConditionInput(input.field(), operator, value, input.logic(), input.terms());
    }

    private Object adaptValue(String operator, Object value) {
        return switch (operator) {
            case JSON_PATH_EQUALS -> pathValue(value, JsonConditionValue.Kind.PATH_EQUALS);
            case JSON_PATH_NOT_EQUALS, JSON_PATH_GREATER_THAN, JSON_PATH_GREATER_OR_EQUALS,
                 JSON_PATH_LESS_THAN, JSON_PATH_LESS_OR_EQUALS -> pathValue(value, JsonConditionValue.Kind.PATH_COMPARE);
            case JSON_PATH_LIKE, JSON_PATH_NOT_LIKE -> pathValue(value, JsonConditionValue.Kind.PATH_PATTERN);
            case JSON_PATH_IS_NULL, JSON_PATH_NOT_NULL -> pathOnlyValue(value, JsonConditionValue.Kind.PATH_NULL);
            case JSON_PATH_IN, JSON_PATH_NOT_IN -> pathValue(value, JsonConditionValue.Kind.PATH_MEMBERSHIP);
            case JSON_CONTAINS, JSON_NOT_CONTAINS, JSON_CONTAINED_BY, JSON_NOT_CONTAINED_BY -> containsValue(value);
            case JSON_EXISTS, JSON_NOT_EXISTS -> pathOnlyValue(value, JsonConditionValue.Kind.EXISTS);
            case JSON_ARRAY_CONTAINS, JSON_ARRAY_NOT_CONTAINS -> arrayContainsValue(value);
            default -> value;
        };
    }

    private JsonConditionValue pathValue(Object value, JsonConditionValue.Kind kind) {
        if (value instanceof JsonConditionValue conditionValue) {
            return requireKind(conditionValue, kind);
        }
        if (!(value instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException("json path comparison value must be a map with path and value");
        }
        Object operand = requiredValue(map);
        return new JsonConditionValue(kind, JsonConditionValue.parsePath(resolvePathText(map)),
                kind == JsonConditionValue.Kind.PATH_MEMBERSHIP ? operand : requireScalarValue(operand));
    }

    private JsonConditionValue containsValue(Object value) {
        if (value instanceof JsonConditionValue conditionValue) {
            return requireKind(conditionValue, JsonConditionValue.Kind.CONTAINS);
        }
        return JsonConditionValue.contains(value);
    }

    private JsonConditionValue pathOnlyValue(Object value, JsonConditionValue.Kind kind) {
        if (value instanceof JsonConditionValue conditionValue) {
            return requireKind(conditionValue, kind);
        }
        Object path = value instanceof Map<?, ?> map ? map.get("path") : value;
        return new JsonConditionValue(kind, JsonConditionValue.parsePath(requireText(path, "json path")), null);
    }

    private JsonConditionValue arrayContainsValue(Object value) {
        if (value instanceof JsonConditionValue conditionValue) {
            return requireKind(conditionValue, JsonConditionValue.Kind.ARRAY_CONTAINS);
        }
        if (!(value instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException("json-array-contains value must be a map with path and value");
        }
        return JsonConditionValue.arrayContains(JsonConditionValue.parsePath(resolvePathText(map)),
                                                requireScalarValue(requiredValue(map)));
    }

    private Object requiredValue(Map<?, ?> map) {
        if (!map.containsKey("value")) {
            throw new IllegalArgumentException("json condition value field is required");
        }
        return map.get("value");
    }

    private String resolvePathText(Map<?, ?> map) {
        if (map.containsKey("path")) {
            return requireText(map.get("path"), "json path");
        }
        if (map.containsKey("key")) {
            return requireText(map.get("key"), "json key");
        }
        throw new IllegalArgumentException("json condition key or path is required");
    }

    private Object requireScalarValue(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("json condition compare value is required");
        }
        if (value instanceof Map<?, ?> || value instanceof Iterable<?> || value.getClass().isArray()) {
            throw new IllegalArgumentException("json condition compare value must be scalar");
        }
        return value;
    }

    private String requireText(Object value, String name) {
        if (!(value instanceof CharSequence text)) {
            throw new IllegalArgumentException(name + " must be text");
        }
        String trimmed = text.toString().trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return trimmed;
    }

    private String normalize(String operator) {
        return operator == null ? "" : operator.trim().toLowerCase(Locale.ROOT);
    }

}
