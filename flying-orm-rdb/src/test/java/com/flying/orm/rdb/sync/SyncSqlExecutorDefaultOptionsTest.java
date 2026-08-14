package com.flying.orm.rdb.sync;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlResultMemoryLimitExceededException;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertThrows;

/** 验证同步接口默认保护不会遗漏结果集内存预算。 */
class SyncSqlExecutorDefaultOptionsTest {

    @Test
    void defaultOptionsRejectsOversizedCustomExecutorRows() {
        SyncSqlExecutor executor = new FixedRowsExecutor();

        assertThrows(SqlResultMemoryLimitExceededException.class,
                     () -> executor.query(request(), SqlExecutionOptions.safeDefaults().withMaxResultBytes(32)));
    }

    /** 估算器无法在安全深度内完成时，即使显式上限取 long 最大值也必须失败闭合。 */
    @Test
    void maximumConfiguredLimitRejectsRowsWhoseMemoryEstimateIsUnknown() {
        Object value = "leaf";
        for (int depth = 0; depth < 66; depth++) {
            value = List.of(value);
        }
        SyncSqlExecutor executor = new FixedRowsExecutor(DynamicRow.copyOf(Map.of("payload", value)));

        assertThrows(SqlResultMemoryLimitExceededException.class,
                     () -> executor.query(
                             request(),
                             SqlExecutionOptions.safeDefaults().withMaxResultBytes(Long.MAX_VALUE)));
    }

    private static SqlRequest request() {
        return new SqlRequest("select payload from event_log", List.of());
    }

    private static final class FixedRowsExecutor implements SyncSqlExecutor {

        private final DynamicRow row;

        private FixedRowsExecutor() {
            this(DynamicRow.copyOf(Map.of("payload", "01234567890123456789")));
        }

        private FixedRowsExecutor(DynamicRow row) {
            this.row = row;
        }

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            return List.of(row);
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            return 0L;
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            return new SqlWriteResult(0L, List.of());
        }
    }
}
