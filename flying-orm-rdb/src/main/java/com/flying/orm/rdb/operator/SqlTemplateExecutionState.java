package com.flying.orm.rdb.operator;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.template.SqlTemplate;
import com.flying.orm.rdb.template.SqlTemplateEngine;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 已注册 SQL 模板的一次调用状态。
 *
 * <p>这里统一处理普通参数、受控标识符和可信服务端参数的边界。响应式入口在订阅时取服务端参数，
 * 同步 JDBC 入口在执行前直接取值；两边最终都交给同一个 {@link SqlTemplateEngine}，因此不会出现
 * 参数顺序或标识符引用规则不一致的问题。</p>
 */
final class SqlTemplateExecutionState {

    private final SqlTemplateEngine engine;

    private final SqlTemplate template;

    private final Set<String> serverParameters;

    private final Map<String, Object> values = new LinkedHashMap<>();

    private final Map<String, String> identifiers = new LinkedHashMap<>();

    private SqlExecutionOptions options;

    SqlTemplateExecutionState(SqlTemplateEngine engine, SqlTemplate template, Set<String> serverParameters) {
        this.engine = Objects.requireNonNull(engine, "SQL template engine must not be null");
        this.template = Objects.requireNonNull(template, "SQL template must not be null");
        this.serverParameters = Set.copyOf(Objects.requireNonNull(
                serverParameters, "SQL template server parameters must not be null"));
    }

    void bind(String name, Object value) {
        String safeName = requireName(name, "SQL template parameter name");
        if (serverParameters.contains(safeName)) {
            throw new IllegalArgumentException("SQL template server parameter cannot be bound by caller");
        }
        values.put(safeName, value);
    }

    void bindAll(Map<String, ?> values) {
        Map<String, ?> safeValues = Objects.requireNonNull(values, "SQL template values must not be null");
        // 先整体校验，避免调用方传入半合法 Map 时只写进前半部分参数。
        for (String name : safeValues.keySet()) {
            String safeName = requireName(name, "SQL template parameter name");
            if (serverParameters.contains(safeName)) {
                throw new IllegalArgumentException("SQL template server parameter cannot be bound by caller");
            }
        }
        safeValues.forEach(this::bind);
    }

    void identifier(String name, String value) {
        identifiers.put(requireName(name, "SQL template identifier name"),
                        Objects.requireNonNull(value, "SQL template identifier value must not be null"));
    }

    void identifiers(Map<String, String> identifiers) {
        Objects.requireNonNull(identifiers, "SQL template identifiers must not be null").forEach(this::identifier);
    }

    void options(SqlExecutionOptions options) {
        this.options = Objects.requireNonNull(options, "sql execution options must not be null");
    }

    Snapshot snapshot() {
        // 构建器可继续被调用；已经开始执行的那次必须看到独立快照，不能受后续 bind 影响。
        return new Snapshot(Collections.unmodifiableMap(new LinkedHashMap<>(values)),
                            Map.copyOf(identifiers),
                            options);
    }

    Set<String> serverParameters() {
        return serverParameters;
    }

    String templateId() {
        return template.id();
    }

    SqlRequest render(Snapshot snapshot, Map<String, ?> serverValues) {
        Map<String, ?> safeServerValues = new LinkedHashMap<>(Objects.requireNonNull(
                serverValues, "SQL template server parameter values must not be null"));
        if (!safeServerValues.keySet().equals(serverParameters)) {
            throw new IllegalArgumentException("SQL template server parameters do not match declaration");
        }
        LinkedHashMap<String, Object> merged = new LinkedHashMap<>(safeServerValues.size() + snapshot.values().size());
        safeServerValues.forEach(merged::put);
        snapshot.values().forEach(merged::put);
        return engine.render(template.id(), merged, snapshot.identifiers());
    }

    private static String requireName(String value, String fieldName) {
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(fieldName + " must not be blank");
        }
        return value.trim();
    }

    record Snapshot(Map<String, Object> values,
                    Map<String, String> identifiers,
                    SqlExecutionOptions options) {
    }
}
