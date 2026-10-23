package com.flying.orm.rdb.template;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.InternalApi;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * 把已注册模板渲染为参数化 {@link SqlRequest}。
 *
 * <p>扫描器只识别引号外的占位符；业务值始终作为绑定参数，受控标识符始终经过方言引用。</p>
 *
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class SqlTemplateEngine {

    private final SqlTemplateRenderer.Backend backend;

    private final Map<String, PreparedSql> compiledTemplates;

    private SqlTemplateEngine(SqlTemplateRegistry registry,
                              RdbDialect dialect,
                              ValueCodecRegistry valueCodecs,
                              boolean jdbcBindMarkers) {
        SqlTemplateRegistry safeRegistry = Objects.requireNonNull(
                registry, "SQL template registry must not be null");
        this.backend = SqlTemplateRenderer.Backend.create(dialect, valueCodecs, jdbcBindMarkers);
        Map<String, PreparedSql> compiled = new LinkedHashMap<>();
        for (SqlTemplateRegistry.Entry entry : safeRegistry.entries()) {
            SqlTemplateRenderer.CompiledTemplate template = SqlTemplateRenderer.compile(entry.template(), backend);
            if (!template.valueSlots().containsAll(entry.serverParameters())) {
                throw new IllegalArgumentException("SQL template server parameters are not all used by SQL template");
            }
            compiled.put(SqlTemplateRegistry.normalize(entry.template().id()),
                    new PreparedSql(template, entry.serverParameters()));
        }
        this.compiledTemplates = Collections.unmodifiableMap(compiled);
    }

    public static SqlTemplateEngine create(SqlTemplateRegistry registry,
                                           RdbDialect dialect,
                                           ValueCodecRegistry valueCodecs) {
        return new SqlTemplateEngine(registry, dialect, valueCodecs, false);
    }

    /** 直接为 JDBC 编译注册模板，不先生成响应式参数计划。 */
    @InternalApi
    public static SqlTemplateEngine createJdbc(SqlTemplateRegistry registry,
                                               RdbDialect dialect,
                                               ValueCodecRegistry valueCodecs) {
        return new SqlTemplateEngine(registry, dialect, valueCodecs, true);
    }

    /** 返回使用 JDBC 问号参数标记的模板引擎。 */
    @InternalApi
    public SqlTemplateEngine forJdbc() {
        if (backend.jdbcBindMarkers()) {
            return this;
        }
        SqlTemplateRenderer.Backend jdbcBackend = SqlTemplateRenderer.Backend.create(
                backend.dialect(), backend.valueCodecs(), true);
        Map<String, PreparedSql> jdbcTemplates = new LinkedHashMap<>();
        compiledTemplates.forEach((id, compiled) -> jdbcTemplates.put(
                id, new PreparedSql(SqlTemplateRenderer.retarget(compiled.compiled, jdbcBackend),
                        compiled.serverParameters)));
        return new SqlTemplateEngine(jdbcBackend, jdbcTemplates);
    }

    public SqlRequest render(String templateId,
                             Map<String, ?> values,
                             Map<String, String> identifiers) {
        return prepare(templateId).render(values, identifiers);
    }

    /**
     * 把后端代码中的单条 SQL 编译为 R2DBC 原生请求。只处理命名值参数，不开放动态标识符。
     */
    public static SqlRequest compileNative(String sql,
                                           Map<String, ?> values,
                                           RdbDialect dialect,
                                           ValueCodecRegistry valueCodecs) {
        return prepareNative(sql, SqlTemplateRenderer.Backend.create(dialect, valueCodecs, false))
                .render(values, Map.of());
    }

    /**
     * 原生 SQL 调用状态在创建时完成一次方言校验和占位符编译，重复执行时只绑定本次参数。
     */
    @InternalApi
    public static PreparedSql prepareNative(String sql,
                                                  RdbDialect dialect,
                                                  ValueCodecRegistry valueCodecs,
                                                  boolean jdbcBindMarkers) {
        return prepareNative(
                sql, SqlTemplateRenderer.Backend.create(dialect, valueCodecs, jdbcBindMarkers));
    }

    private static PreparedSql prepareNative(String sql, SqlTemplateRenderer.Backend backend) {
        return new PreparedSql(
                SqlTemplateRenderer.compile(SqlTemplate.nativeStatement(sql), backend), Set.of());
    }

    /** 在调用创建时绑定注册事实；重复执行不再按 ID 查找或重复传递服务端参数声明。 */
    @InternalApi
    public PreparedSql prepare(String templateId) {
        PreparedSql compiled = compiledTemplates.get(
                SqlTemplateRegistry.normalize(templateId));
        if (compiled == null) {
            throw new IllegalArgumentException("SQL template is not registered");
        }
        return compiled;
    }

    private SqlTemplateEngine(SqlTemplateRenderer.Backend backend,
                              Map<String, PreparedSql> compiledTemplates) {
        this.backend = Objects.requireNonNull(backend, "SQL template backend must not be null");
        this.compiledTemplates = Collections.unmodifiableMap(Objects.requireNonNull(
                compiledTemplates, "compiled SQL templates must not be null"));
    }

    /** 原生 SQL 与注册模板共用的不可变编译结果；每次渲染只接收本次调用数据。 */
    @InternalApi
    public static final class PreparedSql {
        private final SqlTemplateRenderer.CompiledTemplate compiled;
        private final Set<String> serverParameters;

        private PreparedSql(SqlTemplateRenderer.CompiledTemplate compiled, Set<String> serverParameters) {
            this.compiled = compiled;
            this.serverParameters = Set.copyOf(serverParameters);
        }

        public Set<String> serverParameters() {
            return serverParameters;
        }

        /** 是否为 SQL 实际使用的值参数；共享参数 Map 中的其他属性不参与绑定。 */
        public boolean hasParameter(String name) {
            return compiled.valueSlots().contains(name);
        }

        /** 按 SQL 实际占位符取值生成不可变请求；缺失值拒绝，未引用的键忽略。 */
        public SqlRequest render(Map<String, ?> values, Map<String, String> identifiers) {
            return SqlTemplateRenderer.renderCompiled(compiled, values, identifiers, false);
        }

        /** 仅供已在调用边界取得参数所有权的模块内部调用。 */
        public SqlRequest renderOwned(Map<String, ?> values, Map<String, String> identifiers) {
            return SqlTemplateRenderer.renderCompiled(compiled, values, identifiers, true);
        }
    }
}
