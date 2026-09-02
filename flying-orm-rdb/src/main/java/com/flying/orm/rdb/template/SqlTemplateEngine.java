package com.flying.orm.rdb.template;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.InternalApi;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

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

    private final SqlTemplateRegistry registry;

    private final SqlTemplateRenderer.Backend backend;

    private final Map<String, SqlTemplateRenderer.CompiledTemplate> compiledTemplates;

    private SqlTemplateEngine(SqlTemplateRegistry registry,
                              RdbDialect dialect,
                              ValueCodecRegistry valueCodecs,
                              boolean jdbcBindMarkers) {
        SqlTemplateRegistry safeRegistry = Objects.requireNonNull(
                registry, "SQL template registry must not be null");
        this.registry = safeRegistry;
        this.backend = SqlTemplateRenderer.Backend.create(dialect, valueCodecs, jdbcBindMarkers);
        Map<String, SqlTemplateRenderer.CompiledTemplate> compiled = new LinkedHashMap<>();
        for (SqlTemplateRegistry.Entry entry : safeRegistry.entries()) {
            compiled.put(SqlTemplateRegistry.normalize(entry.template().id()),
                         SqlTemplateRenderer.compileRegistered(entry.template(), this.backend));
        }
        this.compiledTemplates = Collections.unmodifiableMap(compiled);
    }

    public static SqlTemplateEngine create(SqlTemplateRegistry registry,
                                           RdbDialect dialect,
                                           ValueCodecRegistry valueCodecs) {
        return new SqlTemplateEngine(registry, dialect, valueCodecs, false);
    }

    /** 返回使用 JDBC 问号参数标记的模板引擎。 */
    @InternalApi
    public SqlTemplateEngine forJdbc() {
        if (backend.jdbcBindMarkers()) {
            return this;
        }
        SqlTemplateRenderer.Backend jdbcBackend = SqlTemplateRenderer.Backend.create(
                backend.dialect(), backend.valueCodecs(), true);
        Map<String, SqlTemplateRenderer.CompiledTemplate> jdbcTemplates = new LinkedHashMap<>();
        compiledTemplates.forEach((id, compiled) -> jdbcTemplates.put(
                id, SqlTemplateRenderer.retarget(compiled, jdbcBackend)));
        return new SqlTemplateEngine(registry, jdbcBackend, jdbcTemplates);
    }

    public SqlRequest render(String templateId,
                             Map<String, ?> values,
                             Map<String, String> identifiers) {
        return SqlTemplateRenderer.renderCompiled(
                compiledTemplate(templateId), values, identifiers, false);
    }

    /**
     * 渲染已经在模板调用状态或参数提供器边界取得所有权的参数。
     * 仅供 flying-orm 内部跨包协作，业务代码应调用 {@link #render(String, Map, Map)}。
     */
    @InternalApi
    public SqlRequest renderOwned(String templateId,
                                  Map<String, ?> values,
                                  Map<String, String> identifiers) {
        return SqlTemplateRenderer.renderCompiled(
                compiledTemplate(templateId), values, identifiers, true);
    }

    /**
     * 把后端代码中的单条 SQL 编译为 R2DBC 原生请求。只处理命名值参数，不开放动态标识符。
     */
    public static SqlRequest compileNative(String sql,
                                           Map<String, ?> values,
                                           RdbDialect dialect,
                                           ValueCodecRegistry valueCodecs) {
        return compileNative(sql, values, SqlTemplateRenderer.Backend.create(dialect, valueCodecs, false));
    }

    /** 同步 JDBC 使用同一安全扫描过程，但参数标记固定生成 {@code ?}。 */
    @InternalApi
    public static SqlRequest compileNativeJdbc(String sql,
                                               Map<String, ?> values,
                                               RdbDialect dialect,
                                               ValueCodecRegistry valueCodecs) {
        return compileNative(sql, values, SqlTemplateRenderer.Backend.create(dialect, valueCodecs, true));
    }

    private static SqlRequest compileNative(String sql,
                                            Map<String, ?> values,
                                            SqlTemplateRenderer.Backend backend) {
        SqlTemplate template = SqlTemplate.nativeStatement(sql);
        return SqlTemplateRenderer.render(template, values, Map.of(), backend);
    }

    private SqlTemplateRenderer.CompiledTemplate compiledTemplate(String templateId) {
        SqlTemplateRenderer.CompiledTemplate compiled = compiledTemplates.get(
                SqlTemplateRegistry.normalize(templateId));
        if (compiled == null) {
            throw new IllegalArgumentException("SQL template is not registered");
        }
        return compiled;
    }

    private SqlTemplateEngine(SqlTemplateRegistry registry,
                              SqlTemplateRenderer.Backend backend,
                              Map<String, SqlTemplateRenderer.CompiledTemplate> compiledTemplates) {
        this.registry = Objects.requireNonNull(registry, "SQL template registry must not be null");
        this.backend = Objects.requireNonNull(backend, "SQL template backend must not be null");
        this.compiledTemplates = Collections.unmodifiableMap(Objects.requireNonNull(
                compiledTemplates, "compiled SQL templates must not be null"));
    }
}
