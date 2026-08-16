package com.flying.orm.rdb.template;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 构建完成后只读的模板注册表，同一个实例可以被响应式和同步入口并发共享。
 *
 * <p>服务端安全参数和模板一起在启动阶段登记。普通业务调用只能绑定剩余参数，租户、用户等可信值由
 * {@link SqlTemplateParameterProvider} 在每次订阅时提供。</p>
 *
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class SqlTemplateRegistry {

    private final Map<String, SqlTemplate> templates;

    private final Map<String, Set<String>> serverParameters;

    private SqlTemplateRegistry(Map<String, SqlTemplate> templates,
                                Map<String, Set<String>> serverParameters) {
        this.templates = Map.copyOf(templates);
        this.serverParameters = Map.copyOf(serverParameters);
    }

    public static Builder builder() {
        return new Builder();
    }

    public SqlTemplate template(String id) {
        String key = normalize(id);
        SqlTemplate template = templates.get(key);
        if (template == null) {
            throw new IllegalArgumentException("SQL template is not registered");
        }
        return template;
    }

    /**
     * 返回只能由可信服务端上下文提供的参数名。返回集合不可修改，可以直接交给并发查询使用。
     *
     * @param id 模板 ID
     * @return 服务端安全参数名；模板没有声明时返回空集合
     */
    public Set<String> serverParameters(String id) {
        String key = normalize(id);
        template(key);
        return serverParameters.getOrDefault(key, Set.of());
    }

    /** 仅供同包模板引擎在装配期按实际方言复验全部只读模板。 */
    Iterable<SqlTemplate> templates() {
        return templates.values();
    }

    private static String normalize(String id) {
        return Objects.requireNonNull(id, "SQL template id must not be null").trim().toLowerCase(Locale.ROOT);
    }

    public static final class Builder {

        private final Map<String, SqlTemplate> templates = new LinkedHashMap<>();

        private final Map<String, Set<String>> serverParameters = new LinkedHashMap<>();

        private Builder() {
        }

        public Builder register(SqlTemplate template) {
            return register(template, Set.of());
        }

        /**
         * 注册模板及其服务端安全参数。参数名必须是普通 Java 标识符，且不能和动态标识符槽位重名。
         * SQL 中是否真正使用这些参数会由统一模板扫描器在第一次执行前再次严格核对。
         *
         * @param template 启动阶段可信模板
         * @param serverParameters 不允许普通调用方绑定的参数名
         * @return 当前注册器
         */
        public Builder register(SqlTemplate template, Set<String> serverParameters) {
            SqlTemplate safeTemplate = Objects.requireNonNull(template, "SQL template must not be null");
            Set<String> normalizedParameters = new LinkedHashSet<>();
            for (String parameter : Objects.requireNonNull(
                    serverParameters, "SQL template server parameters must not be null")) {
                String normalized = SqlTemplate.requireName(parameter, "SQL template server parameter");
                if (!normalizedParameters.add(normalized)) {
                    throw new IllegalArgumentException("duplicate SQL template server parameter after normalization");
                }
                if (safeTemplate.identifierSlots().contains(normalized)) {
                    throw new IllegalArgumentException("SQL template server parameter conflicts with identifier slot");
                }
            }
            Set<String> safeServerParameters = Collections.unmodifiableSet(normalizedParameters);
            String key = normalize(safeTemplate.id());
            SqlTemplate previous = templates.putIfAbsent(key, safeTemplate);
            if (previous != null) {
                throw new IllegalArgumentException("duplicate SQL template id");
            }
            this.serverParameters.put(key, safeServerParameters);
            return this;
        }

        public SqlTemplateRegistry build() {
            return new SqlTemplateRegistry(templates, serverParameters);
        }
    }
}
