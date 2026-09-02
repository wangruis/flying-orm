package com.flying.orm.core.sql.render;

import com.flying.orm.core.internal.Names;

import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

/**
 * 可跨请求复用的最小 SQL 结构计划。
 *
 * <p>它只保存规范 SQL、参数槽数量和可选的方言传输 SQL，不保存业务值、结果映射、资源预算或事务状态。
 * 方言传输 SQL 只是可复用的结构编译产物，不构成安全校验凭据；JDBC/R2DBC 执行边界仍会按实际数据库产品
 * 独立校验规范 SQL，并从规范 SQL 得到可信的执行 SQL。</p>
 *
 * @author wangr
 * @date 2026-08-24
 * @version v3.1
 */
public class SqlStatementPlan {

    private final String sql;
    private final SqlBindMarkerStyle bindMarkerStyle;
    private final int parameterCount;
    private final String transportDialect;
    private final String transportSql;

    protected SqlStatementPlan(Validated values) {
        this.sql = values.sql;
        this.bindMarkerStyle = values.bindMarkerStyle;
        this.parameterCount = values.parameterCount;
        this.transportDialect = values.transportDialect;
        this.transportSql = values.transportSql;
    }

    protected static Validated validated(String sql,
                                         SqlBindMarkerStyle bindMarkerStyle,
                                         int parameterCount,
                                         String transportDialect,
                                         String transportSql) {
        String safeSql = Names.requireText(sql, "SQL statement plan");
        SqlBindMarkerStyle safeBindMarkerStyle = Objects.requireNonNull(
                bindMarkerStyle, "SQL statement bind marker style must not be null");
        if (parameterCount < 0) {
            throw new IllegalArgumentException("SQL statement parameter count must not be negative");
        }
        String safeTransportDialect = normalizeDialect(transportDialect);
        String safeTransportSql = transportSql == null ? null : Names.requireText(
                transportSql, "SQL statement transport plan");
        if (safeTransportDialect.isEmpty() != (safeTransportSql == null)) {
            throw new IllegalArgumentException(
                    "SQL statement transport dialect and SQL must be configured together");
        }
        return new Validated(safeSql, safeBindMarkerStyle, parameterCount,
                             safeTransportDialect, safeTransportSql);
    }

    /** 创建尚未针对驱动方言编译传输 SQL 的结构计划。 */
    public static SqlStatementPlan canonical(String sql,
                                             SqlBindMarkerStyle bindMarkerStyle,
                                             int parameterCount) {
        return new SqlStatementPlan(validated(sql, bindMarkerStyle, parameterCount, "", null));
    }

    /**
     * 创建携带方言传输 SQL 的结构计划。
     *
     * <p>该公开工厂只组装不可变计划，不证明调用方提供的 SQL 已经完成安全校验。执行器仍会按实际
     * 数据库产品独立校验规范 SQL，并从规范 SQL 编译可信的传输 SQL。</p>
     */
    public static SqlStatementPlan prepared(String sql,
                                            SqlBindMarkerStyle bindMarkerStyle,
                                            int parameterCount,
                                            String transportDialect,
                                            String transportSql) {
        String safeDialect = normalizeDialect(transportDialect);
        if (safeDialect.isEmpty()) {
            throw new IllegalArgumentException("SQL statement transport dialect must not be blank");
        }
        return new SqlStatementPlan(validated(
                sql, bindMarkerStyle, parameterCount, safeDialect, transportSql));
    }

    public String sql() {
        return sql;
    }

    public SqlBindMarkerStyle bindMarkerStyle() {
        return bindMarkerStyle;
    }

    public int parameterCount() {
        return parameterCount;
    }

    /** 方言相同时返回计划携带的传输 SQL；返回值不代表执行边界已经信任或校验该 SQL。 */
    public Optional<String> transportSql(String dialect) {
        return transportSql != null && transportDialect.equals(normalizeDialect(dialect))
                ? Optional.of(transportSql)
                : Optional.empty();
    }

    /** 是否携带该方言的传输 SQL；该状态不代表执行边界已经完成安全校验。 */
    public boolean preparedFor(String dialect) {
        return !transportDialect.isEmpty() && transportDialect.equals(normalizeDialect(dialect));
    }

    /** 是否携带方言传输 SQL；该状态不证明创建者或 SQL 已经受信任。 */
    public boolean prepared() {
        return !transportDialect.isEmpty();
    }

    private static String normalizeDialect(String dialect) {
        return dialect == null ? "" : dialect.trim().toUpperCase(Locale.ROOT);
    }

    protected static final class Validated {

        private final String sql;
        private final SqlBindMarkerStyle bindMarkerStyle;
        private final int parameterCount;
        private final String transportDialect;
        private final String transportSql;

        private Validated(String sql,
                          SqlBindMarkerStyle bindMarkerStyle,
                          int parameterCount,
                          String transportDialect,
                          String transportSql) {
            this.sql = sql;
            this.bindMarkerStyle = bindMarkerStyle;
            this.parameterCount = parameterCount;
            this.transportDialect = transportDialect;
            this.transportSql = transportSql;
        }
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof SqlStatementPlan plan)) {
            return false;
        }
        return parameterCount == plan.parameterCount
                && sql.equals(plan.sql)
                && bindMarkerStyle == plan.bindMarkerStyle
                && transportDialect.equals(plan.transportDialect)
                && Objects.equals(transportSql, plan.transportSql);
    }

    @Override
    public int hashCode() {
        return Objects.hash(sql, bindMarkerStyle, parameterCount, transportDialect, transportSql);
    }

    @Override
    public String toString() {
        return "SqlStatementPlan[sql=" + sql + ", bindMarkerStyle=" + bindMarkerStyle
                + ", parameterCount=" + parameterCount + ", transportDialect="
                + transportDialect + ']';
    }
}
