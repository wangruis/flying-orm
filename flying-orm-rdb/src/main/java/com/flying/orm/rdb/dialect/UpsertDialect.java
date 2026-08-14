package com.flying.orm.rdb.dialect;

import java.util.List;
import java.util.Objects;
import java.util.StringJoiner;

/**
 * UpsertDialect 只管一件事：同一组列在不同数据库里怎么写 upsert。
 *
 * @author wangr
 * @date 2026-07-24
 * @version v1.0
 */
public interface UpsertDialect {

    /**
     * 渲染 upsert SQL。参数占位符始终跟 columns 顺序一致，执行层继续复用批量绑定。
     *
     * @param table           表名
     * @param columns         本次写入的列
     * @param conflictColumns 用来判断冲突的列，通常是主键
     * @param updateColumns   冲突后需要更新的列
     * @return upsert SQL
     */
    default String render(String table,
                          List<String> columns,
                          List<String> conflictColumns,
                          List<String> updateColumns) {
        return render(table, columns, conflictColumns, updateColumns, markerList(columns.size()));
    }

    /**
     * 渲染带字段级参数表达式的 upsert。JSONB 这类字段可以把普通问号改成带类型的表达式。
     */
    String render(String table,
                  List<String> columns,
                  List<String> conflictColumns,
                  List<String> updateColumns,
                  List<String> valueExpressions);

    /**
     * H2 的 merge 写法，适合内嵌开发和测试。
     *
     * @return H2 upsert 方言
     */
    static UpsertDialect h2() {
        return (table, columns, conflictColumns, updateColumns, valueExpressions) -> "merge into " + table + " ("
                + join(columns) + ") key (" + join(conflictColumns) + ") values ("
                + joinExpressions(columns, valueExpressions) + ")";
    }

    /**
     * MySQL 的 on duplicate key 写法。
     *
     * @return MySQL upsert 方言
     */
    static UpsertDialect mysql() {
        return (table, columns, conflictColumns, updateColumns, valueExpressions) -> {
            requireUpdateColumns(updateColumns);
            StringJoiner sets = new StringJoiner(", ");
            for (String column : updateColumns) {
                sets.add(column + " = values(" + column + ")");
            }
            return "insert into " + table + " (" + join(columns) + ") values ("
                    + joinExpressions(columns, valueExpressions)
                    + ") on duplicate key update " + sets;
        };
    }

    /**
     * PostgreSQL 的 on conflict 写法。
     *
     * @return PostgreSQL upsert 方言
     */
    static UpsertDialect postgresql() {
        return (table, columns, conflictColumns, updateColumns, valueExpressions) -> {
            requireUpdateColumns(updateColumns);
            StringJoiner sets = new StringJoiner(", ");
            for (String column : updateColumns) {
                sets.add(column + " = excluded." + column);
            }
            return "insert into " + table + " (" + join(columns) + ") values ("
                    + joinExpressions(columns, valueExpressions)
                    + ") on conflict (" + join(conflictColumns) + ") do update set " + sets;
        };
    }

    /**
     * Oracle 的 merge 写法。
     *
     * @return Oracle upsert 方言
     */
    static UpsertDialect oracle() {
        return (table, columns, conflictColumns, updateColumns, valueExpressions) -> {
            requireUpdateColumns(updateColumns);
            StringJoiner sourceColumns = new StringJoiner(", ");
            List<String> expressions = expressions(columns, valueExpressions);
            for (int index = 0; index < columns.size(); index++) {
                sourceColumns.add(expressions.get(index) + " as " + columns.get(index));
            }
            return "merge into " + table + " target using (select " + sourceColumns + " from dual) source "
                    + mergeBody(columns, conflictColumns, updateColumns, true);
        };
    }

    /**
     * SQL Server 的 merge 写法。
     *
     * @return SQL Server upsert 方言
     */
    static UpsertDialect sqlServer() {
        return (table, columns, conflictColumns, updateColumns, valueExpressions) -> {
            requireUpdateColumns(updateColumns);
            // HOLDLOCK 把匹配和插入放在同一键范围锁内，减少并发 upsert 同时判断“不存在”后撞唯一键的窗口。
            return "merge into " + table + " with (holdlock) as target using (values ("
                    + joinExpressions(columns, valueExpressions)
                    + ")) as source (" + join(columns) + ") "
                    + mergeBody(columns, conflictColumns, updateColumns, false) + ";";
        };
    }

    private static String join(List<String> values) {
        List<String> safeValues = List.copyOf(Objects.requireNonNull(values, "upsert columns must not be null"));
        if (safeValues.isEmpty()) {
            throw new IllegalArgumentException("upsert columns must not be empty");
        }
        return String.join(", ", safeValues);
    }

    private static List<String> markerList(int count) {
        java.util.ArrayList<String> markers = new java.util.ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            markers.add("?");
        }
        return List.copyOf(markers);
    }

    private static String joinExpressions(List<String> columns, List<String> valueExpressions) {
        return String.join(", ", expressions(columns, valueExpressions));
    }

    private static List<String> expressions(List<String> columns, List<String> valueExpressions) {
        List<String> safeColumns = List.copyOf(Objects.requireNonNull(columns, "upsert columns must not be null"));
        List<String> safeExpressions = List.copyOf(Objects.requireNonNull(valueExpressions,
                                                                          "upsert value expressions must not be null"));
        if (safeColumns.size() != safeExpressions.size()) {
            throw new IllegalArgumentException("upsert value expression count must match column count");
        }
        return safeExpressions;
    }

    private static String mergeBody(List<String> columns,
                                    List<String> conflictColumns,
                                    List<String> updateColumns,
                                    boolean wrapPredicate) {
        String target = "target.";
        String source = "source.";
        StringJoiner predicates = new StringJoiner(" and ");
        for (String column : conflictColumns) {
            predicates.add(target + column + " = " + source + column);
        }
        StringJoiner sets = new StringJoiner(", ");
        for (String column : updateColumns) {
            sets.add(target + column + " = " + source + column);
        }
        StringJoiner sourceValues = new StringJoiner(", ");
        for (String column : columns) {
            sourceValues.add(source + column);
        }
        String predicate = wrapPredicate ? "(" + predicates + ")" : predicates.toString();
        return "on " + predicate + " when matched then update set " + sets
                + " when not matched then insert (" + join(columns) + ") values (" + sourceValues + ")";
    }

    private static void requireUpdateColumns(List<String> updateColumns) {
        if (Objects.requireNonNull(updateColumns, "upsert update columns must not be null").isEmpty()) {
            throw new IllegalArgumentException("upsert update columns must not be empty");
        }
    }
}
