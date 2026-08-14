package com.flying.orm.rdb.internal.plan;

import java.util.List;
import java.util.Objects;

/**
 * 可跨请求复用的完整参数化 SQL 结构计划。
 *
 * <p>计划只保存 SQL、操作、目标表、投影列和 primitive 参数槽，不保存参数值、条件树、租户或权限上下文。
 * 参数槽数组只在缓存 miss 编译一次，避免 {@code Integer} 装箱和 {@code IntStream} 临时对象。</p>
 *
 * @author wangr
 * @date 2026-08-04
 * @version v1.0
 */
public final class SqlStructurePlan {

    private final String sql;
    private final String operation;
    private final String table;
    private final List<String> projectionColumns;
    private final int[] parameterSlots;

    /**
     * 创建不可变 SQL 结构计划。
     *
     * @param sql 参数化 SQL
     * @param operation 操作类型
     * @param table 物理表身份
     * @param projectionColumns 投影列稳定顺序；写操作传空列表
     * @param parameterSlots SQL 占位符对应的当前请求参数索引
     */
    public SqlStructurePlan(String sql,
                            String operation,
                            String table,
                            List<String> projectionColumns,
                            int[] parameterSlots) {
        this.sql = requireText(sql, "sql structure plan text");
        this.operation = requireText(operation, "sql structure plan operation");
        this.table = requireText(table, "sql structure plan table");
        this.projectionColumns = List.copyOf(Objects.requireNonNull(
                projectionColumns, "sql structure projection columns must not be null"));
        this.parameterSlots = Objects.requireNonNull(
                parameterSlots, "sql structure parameter slots must not be null").clone();
        for (int slot : this.parameterSlots) {
            if (slot < 0) {
                throw new IllegalArgumentException("sql structure parameter slots must be non-negative");
            }
        }
    }

    /**
     * 创建参数按声明顺序绑定的结构计划。
     *
     * @param sql 参数化 SQL
     * @param operation 操作类型
     * @param table 物理表身份
     * @param projectionColumns 投影列
     * @param parameterCount 参数数量
     * @return 不含任何请求参数值的计划
     */
    public static SqlStructurePlan sequential(String sql,
                                              String operation,
                                              String table,
                                              List<String> projectionColumns,
                                              int parameterCount) {
        if (parameterCount < 0) {
            throw new IllegalArgumentException("sql structure parameter count must not be negative");
        }
        int[] slots = new int[parameterCount];
        for (int index = 0; index < parameterCount; index++) {
            slots[index] = index;
        }
        return new SqlStructurePlan(sql, operation, table, projectionColumns, slots);
    }

    /** @return 参数化 SQL 文本。 */
    public String sql() {
        return sql;
    }

    /** @return 操作类型。 */
    public String operation() {
        return operation;
    }

    /** @return 物理表身份。 */
    public String table() {
        return table;
    }

    /** @return 不可变投影列列表。 */
    public List<String> projectionColumns() {
        return projectionColumns;
    }

    /** @return 参数槽位防御性副本；内部计数优先使用 {@link #parameterCount()} 避免复制。 */
    public int[] parameterSlots() {
        return parameterSlots.clone();
    }

    /** @return 参数槽数量。 */
    public int parameterCount() {
        return parameterSlots.length;
    }

    private static String requireText(String text, String name) {
        String safeText = Objects.requireNonNull(text, name + " must not be null");
        if (safeText.isBlank()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
        return safeText;
    }
}
