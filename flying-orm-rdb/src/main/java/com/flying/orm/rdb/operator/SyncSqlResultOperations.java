package com.flying.orm.rdb.operator;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** 同步原生 SQL 与同步模板共用的查询、映射和单行语义。 */
final class SyncSqlResultOperations {

    private SyncSqlResultOperations() {
    }

    static List<DynamicRow> query(SyncSqlExecutor executor, SqlRequest request, SqlExecutionOptions options) {
        SyncSqlExecutor safeExecutor = Objects.requireNonNull(executor, "sync SQL executor must not be null");
        return options == null ? safeExecutor.query(request) : safeExecutor.query(request, options);
    }

    static <T> List<T> map(List<DynamicRow> rows, RowMapper<T> mapper) {
        RowMapper<T> safeMapper = Objects.requireNonNull(mapper, "row mapper must not be null");
        List<T> mapped = new ArrayList<>(rows.size());
        for (DynamicRow row : rows) {
            mapped.add(safeMapper.map(row));
        }
        return List.copyOf(mapped);
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
        SyncSqlExecutor safeExecutor = Objects.requireNonNull(executor, "sync SQL executor must not be null");
        return options == null ? safeExecutor.rowsUpdated(request) : safeExecutor.rowsUpdated(request, options);
    }
}
