package com.flying.orm.rdb.operator;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.internal.template.SqlStatements;
import com.flying.orm.rdb.template.SqlTemplateEngine;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * 原生 SQL 单次调用时保存的参数和执行保护。
 *
 * <p>响应式和同步入口都使用这个小对象，保证 {@code :name} 的检查、codec 转换和参数出现顺序完全一致。
 * 它是可变构建状态，只能服务一次业务调用，不能放进单例或跨线程共享。</p>
 */
final class NativeSqlExecutionState {

    private final ValueCodecRegistry valueCodecs;

    private final RdbDialect dialect;

    private final String sql;

    private final boolean jdbcBindMarkers;

    private final Map<String, Object> values = new LinkedHashMap<>();

    private SqlExecutionOptions options;

    NativeSqlExecutionState(ValueCodecRegistry valueCodecs, RdbDialect dialect, String sql) {
        this(valueCodecs, dialect, sql, false);
    }

    NativeSqlExecutionState(ValueCodecRegistry valueCodecs,
                            RdbDialect dialect,
                            String sql,
                            boolean jdbcBindMarkers) {
        this.valueCodecs = Objects.requireNonNull(valueCodecs, "value codec registry must not be null");
        this.dialect = Objects.requireNonNull(dialect, "RDB dialect must not be null");
        // 原生入口也必须只有一条语句，避免参数化入口被误当成多语句执行通道。
        this.sql = SqlStatements.requireSingle(sql, dialect);
        this.jdbcBindMarkers = jdbcBindMarkers;
    }

    void bind(String name, Object value) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("native SQL parameter name must not be blank");
        }
        values.put(name.trim(), value);
    }

    void bindAll(Map<String, ?> values) {
        Objects.requireNonNull(values, "native SQL parameter values must not be null").forEach(this::bind);
    }

    void options(SqlExecutionOptions options) {
        this.options = Objects.requireNonNull(options, "sql execution options must not be null");
    }

    SqlExecutionOptions options() {
        return options;
    }

    SqlRequest request() {
        // 编译阶段会校验少传、多传参数，并按 SQL 里的位置生成最终参数列表。
        return jdbcBindMarkers
                ? SqlTemplateEngine.compileNativeJdbc(sql, values, dialect, valueCodecs)
                : SqlTemplateEngine.compileNative(sql, values, dialect, valueCodecs);
    }
}
