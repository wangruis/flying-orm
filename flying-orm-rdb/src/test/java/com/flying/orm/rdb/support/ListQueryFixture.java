package com.flying.orm.rdb.support;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;

import java.util.List;
import java.util.function.Predicate;

/** Test data source for planner tests whose rows are already supplied in memory; never used by JDBC. */
public interface ListQueryFixture extends SyncSqlExecutor {
    @Override
    List<DynamicRow> query(SqlRequest request);

    @Override
    default List<DynamicRow> query(SqlRequest request, SqlExecutionOptions options) {
        return query(request);
    }

    @Override
    default long queryWhile(SqlRequest request, SqlExecutionOptions options,
                            Predicate<? super DynamicRow> consumer) {
        List<DynamicRow> rows = options == null ? query(request) : query(request, options);
        long consumed = 0;
        for (DynamicRow row : rows) {
            consumed++;
            if (!consumer.test(row)) break;
        }
        return consumed;
    }
}
