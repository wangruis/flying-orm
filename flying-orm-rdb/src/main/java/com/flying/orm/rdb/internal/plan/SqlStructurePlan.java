package com.flying.orm.rdb.internal.plan;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlStatementPlan;

import java.util.Objects;

/**
 * 可跨请求复用的完整参数化 SQL 结构计划。
 *
 * <p>计划只保存已经校验和方言编译的 SQL、操作与目标表，不保存参数值、条件树、租户或权限上下文。</p>
 *
 * @author wangr
 * @date 2026-08-04
 * @version v1.0
 */
public final class SqlStructurePlan {

    private final SqlStatementPlan statement;
    private final String operation;
    private final String table;

    /**
     * 创建不可变 SQL 结构计划。
     *
     * @param statement 尚未绑定具体驱动传输格式的规范 SQL 结构
     * @param operation 操作类型
     * @param table 物理表身份
     */
    public SqlStructurePlan(SqlStatementPlan statement,
                            String operation,
                            String table) {
        this.statement = Objects.requireNonNull(
                statement, "sql statement plan must not be null");
        this.operation = requireText(operation, "sql structure plan operation");
        this.table = requireText(table, "sql structure plan table");
    }

    /**
     * 创建参数按声明顺序绑定的结构计划。
     *
     * @param sql 参数化 SQL
     * @param dialect 数据库方言或产品名
     * @param bindMarkerStyle 参数标记来源
     * @param operation 操作类型
     * @param table 物理表身份
     * @param parameterCount 参数数量
     * @return 不含任何请求参数值的计划
     */
    public static SqlStructurePlan sequential(String sql,
                                              String dialect,
                                              SqlBindMarkerStyle bindMarkerStyle,
                                              String operation,
                                              String table,
                                              int parameterCount) {
        if (parameterCount < 0) {
            throw new IllegalArgumentException("sql structure parameter count must not be negative");
        }
        SqlStatementPlan statement = SqlStatementCompiler.compile(
                sql,
                parameterCount,
                bindMarkerStyle,
                requireText(dialect, "sql structure dialect"));
        return new SqlStructurePlan(statement, operation, table);
    }

    /** @return 参数化 SQL 文本。 */
    public String sql() {
        return statement.sql();
    }

    /** @return 留待最终执行边界校验和编译的规范 SQL 结构计划。 */
    public SqlStatementPlan statement() {
        return statement;
    }

    /** @return 操作类型。 */
    public String operation() {
        return operation;
    }

    /** @return 物理表身份。 */
    public String table() {
        return table;
    }

    /** @return 参数槽数量。 */
    public int parameterCount() {
        return statement.parameterCount();
    }

    private static String requireText(String text, String name) {
        String safeText = Objects.requireNonNull(text, name + " must not be null");
        if (safeText.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return safeText;
    }
}
