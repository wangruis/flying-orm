package com.flying.orm.rdb.dialect;

import java.util.List;

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
     * @param table 表名
     * @param columns 本次写入的列
     * @param conflictColumns 用来判断冲突的列，通常是主键
     * @param updateColumns 冲突后需要更新的列
     * @return upsert SQL
     */
    default String render(String table,
                          List<String> columns,
                          List<String> conflictColumns,
                          List<String> updateColumns) {
        return render(table, columns, conflictColumns, updateColumns,
                      BuiltInUpsertDialects.markers(columns.size()));
    }

    /** 渲染带字段级参数表达式的 upsert。 */
    String render(String table,
                  List<String> columns,
                  List<String> conflictColumns,
                  List<String> updateColumns,
                  List<String> valueExpressions);

    /** @return H2 upsert 方言。 */
    static UpsertDialect h2() {
        return BuiltInUpsertDialects.h2();
    }

    /** @return MySQL upsert 方言。 */
    static UpsertDialect mysql() {
        return BuiltInUpsertDialects.mysql();
    }

    /** @return PostgreSQL upsert 方言。 */
    static UpsertDialect postgresql() {
        return BuiltInUpsertDialects.postgresql();
    }

    /** @return Oracle upsert 方言。 */
    static UpsertDialect oracle() {
        return BuiltInUpsertDialects.oracle();
    }

    /** @return SQL Server upsert 方言。 */
    static UpsertDialect sqlServer() {
        return BuiltInUpsertDialects.sqlServer();
    }
}
