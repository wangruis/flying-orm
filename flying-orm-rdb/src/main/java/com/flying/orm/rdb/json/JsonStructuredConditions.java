package com.flying.orm.rdb.json;

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
import java.util.regex.Pattern;

/**
 * 把前端 JSON 条件整理成经过校验的 JsonConditionValue，是结构化输入进入 JSON 方言渲染前的安全适配层。
 *
 * <p>这里只接受 operator、path、key 和 value，不接受 SQL 片段。路径被拆成受限 key 段，值先验证可编码性，
 * 最终路径和值都由方言 handler 参数化。DynamicForm 可用时还会确认目标字段确实是 JSON 类型。</p>
 *
 * @author wangr
 * @date 2026-07-29
 * @version v1.0
 */
public final class JsonStructuredConditions implements StructuredConditionResolver,
                                                      StructuredConditionCustomizer {

    public static final String JSON_PATH_EQUALS = "json-path-eq";
    public static final String JSON_CONTAINS = "json-contains";
    public static final String JSON_EXISTS = "json-exists";
    public static final String JSON_ARRAY_CONTAINS = "json-array-contains";

    private static final Pattern JSON_KEY = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Set<String> BUILT_IN_OPERATORS = Set.of(JSON_PATH_EQUALS,
                                                                 JSON_CONTAINS,
                                                                 JSON_EXISTS,
                                                                 JSON_ARRAY_CONTAINS);

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
    public ConditionGroup compile(DynamicForm form,
                                  StructuredConditionInput input,
                                  StructuredConditionPolicy policy) {
        return StructuredConditionResolver.composite(this).compile(form, input, policy);
    }

    /**
     * Resolver 会走这个带表单的入口，因此字段类型只能来自可信的 DynamicForm 元数据。
     */
    @Override
    public StructuredConditionInput adapt(DynamicForm form, StructuredConditionInput input) {
        return adaptNode(Objects.requireNonNull(form, "dynamic form must not be null"),
                         Objects.requireNonNull(input, "structured condition input must not be null"));
    }

    @Override
    public StructuredConditionPolicy customize(StructuredConditionPolicy policy) {
        StructuredConditionPolicy safePolicy = StructuredConditionCustomizer.super.customize(policy);
        for (String operator : BUILT_IN_OPERATORS) {
            safePolicy = safePolicy.allowOperator(operator);
        }
        return safePolicy.withAdditionalTerms(GOVERNED_TERMS);
    }

    private StructuredConditionInput adaptNode(DynamicForm form, StructuredConditionInput input) {
        if (input.field() != null || input.operator() != null) {
            String operator = normalize(input.operator());
            // 非 JSON operator 原样交给后续 resolver，多个 customizer 可以组合而不互相吞条件。
            if (!BUILT_IN_OPERATORS.contains(operator)) {
                return input;
            }
            validateJsonField(form, input.field());
            Object value = adaptValue(operator, input.value());
            return new StructuredConditionInput(input.field(), operator, value, input.logic(), input.terms());
        }

        List<StructuredConditionInput> terms = new ArrayList<>(input.terms().size());
        for (StructuredConditionInput term : input.terms()) {
            terms.add(adaptNode(form, Objects.requireNonNull(term, "structured condition child must not be null")));
        }
        return new StructuredConditionInput(input.field(), input.operator(), input.value(), input.logic(), terms);
    }

    private Object adaptValue(String operator, Object value) {
        return switch (operator) {
            case JSON_PATH_EQUALS -> pathEqualsValue(value);
            case JSON_CONTAINS -> containsValue(value);
            case JSON_EXISTS -> existsValue(value);
            case JSON_ARRAY_CONTAINS -> arrayContainsValue(value);
            default -> value;
        };
    }

    private JsonConditionValue pathEqualsValue(Object value) {
        if (value instanceof JsonConditionValue conditionValue) {
            return requireKind(conditionValue, JsonConditionValue.Kind.PATH_EQUALS);
        }
        if (!(value instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException("json-path-eq value must be a map with path and value");
        }
        return JsonConditionValue.pathEquals(pathSegments(resolvePathText(map)),
                                             requireScalarValue(requiredValue(map)));
    }

    private JsonConditionValue containsValue(Object value) {
        if (value instanceof JsonConditionValue conditionValue) {
            return requireKind(conditionValue, JsonConditionValue.Kind.CONTAINS);
        }
        return JsonConditionValue.contains(value);
    }

    private JsonConditionValue existsValue(Object value) {
        if (value instanceof JsonConditionValue conditionValue) {
            return requireKind(conditionValue, JsonConditionValue.Kind.EXISTS);
        }
        Object path = value instanceof Map<?, ?> map ? map.get("path") : value;
        return JsonConditionValue.exists(pathSegments(requireText(path, "json path")));
    }

    private JsonConditionValue arrayContainsValue(Object value) {
        if (value instanceof JsonConditionValue conditionValue) {
            return requireKind(conditionValue, JsonConditionValue.Kind.ARRAY_CONTAINS);
        }
        if (!(value instanceof Map<?, ?> map)) {
            throw new IllegalArgumentException("json-array-contains value must be a map with path and value");
        }
        return JsonConditionValue.arrayContains(pathSegments(resolvePathText(map)),
                                                requireScalarValue(requiredValue(map)));
    }

    private JsonConditionValue requireKind(JsonConditionValue value, JsonConditionValue.Kind expected) {
        if (value.kind() != expected) {
            throw new IllegalArgumentException("json condition kind must be " + expected);
        }
        return value;
    }

    private void validateJsonField(DynamicForm form, String fieldName) {
        DynamicField field = form.field(Objects.requireNonNull(fieldName, "json condition field must not be null"));
        if (field.databaseType().isArray() || field.databaseType().logicalType() != LogicalType.JSON) {
            throw new IllegalArgumentException("json operator requires a JSON field: " + field.name());
        }
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

    private List<String> pathSegments(String path) {
        // 只解析普通 key 段，不允许通配符、脚本表达式或方言原生 JSONPath 注入。
        String candidate = requireText(path, "json path");
        if (candidate.startsWith("$.")) {
            candidate = candidate.substring(2);
        }
        if (candidate.isBlank()) {
            throw new IllegalArgumentException("json path must not be empty");
        }
        String[] parts = candidate.split("\\.", -1);
        List<String> segments = new ArrayList<>(parts.length);
        for (String part : parts) {
            if (!JSON_KEY.matcher(part).matches()) {
                throw new IllegalArgumentException("json path only supports names joined by dots");
            }
            segments.add(part);
        }
        return List.copyOf(segments);
    }

    private Object requireScalarValue(Object value) {
        if (value == null) {
            throw new IllegalArgumentException("json condition compare value is required");
        }
        if (value instanceof Map<?, ?> || value instanceof Iterable<?> || value.getClass().isArray()) {
            throw new IllegalArgumentException("json condition compare value must be scalar");
        }
        if (value instanceof CharSequence text && text.isEmpty()) {
            throw new IllegalArgumentException("json condition compare value must not be empty");
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
