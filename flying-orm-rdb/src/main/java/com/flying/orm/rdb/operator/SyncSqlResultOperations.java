package com.flying.orm.rdb.operator;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

import java.util.List;

/** 同步原生 SQL 与同步模板共用的查询、映射和单行语义。 */
final class SyncSqlResultOperations {

    private static final RowMapper<DynamicRow> IDENTITY = row -> row;

    private SyncSqlResultOperations() {
    }

    static List<DynamicRow> query(SyncSqlExecutor executor, SqlRequest request, SqlExecutionOptions options) {
        return options == null ? executor.query(request) : executor.query(request, options);
    }

    static DynamicRow one(SyncSqlExecutor executor,
                          SqlRequest request,
                          SqlExecutionOptions options,
                          String subject) {
        return one(executor.queryMapped(request, options, IDENTITY, 2), subject);
    }

    static <T> T one(SyncSqlExecutor executor,
                     SqlRequest request,
                     SqlExecutionOptions options,
                     RowMapper<T> mapper,
                     String subject) {
        return one(executor.queryMapped(request, options, mapper, 2), subject);
    }

    static <T> T one(List<T> rows, String subject) {
        if (rows.isEmpty()) {
            return null;
        }
        if (rows.size() != 1) {
            throw new IllegalStateException(subject + " expected zero or one row but returned " + rows.size());
        }
        return rows.getFirst();
    }

    static long execute(SyncSqlExecutor executor, SqlRequest request, SqlExecutionOptions options) {
        return options == null ? executor.rowsUpdated(request) : executor.rowsUpdated(request, options);
    }
}
