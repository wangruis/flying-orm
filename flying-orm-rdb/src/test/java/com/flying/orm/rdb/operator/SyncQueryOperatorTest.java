package com.flying.orm.rdb.operator;

import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 验证同步链式查询直接走 JDBC 契约，并继续复用响应式入口的 SQL 与安全规则。 */
class SyncQueryOperatorTest {

    /** 默认租户范围、业务条件和逻辑删除必须保持参数化，并按稳定顺序交给同步执行器。 */
    @Test
    void nativeQueryUsesSharedCommandAndSyncExecutor() {
        RecordingExecutor executor = new RecordingExecutor();
        SqlRenderer renderer = SqlRenderer.builder().addDefaultTerms().build();
        SyncQueryOperator query = new SyncQueryOperator(
                executor, renderer, DataScope.tenant("tenant_id", "tenant-a"));

        List<DynamicRow> rows = query.select("id", "name")
                                     .from("users")
                                     .where(where -> where.is("name", "Alice"))
                                     .logicDelete("deleted")
                                     .fetchMap();

        assertEquals(1, rows.size());
        assertTrue(executor.request.sql().startsWith("select id, name from users where "));
        assertTrue(executor.request.sql().contains("tenant_id"));
        assertTrue(executor.request.sql().contains("deleted"));
        assertFalse(executor.request.sql().contains("Alice"));
        assertEquals(List.of("Alice", "tenant-a", 0), executor.request.parameters());
    }

    private static final class RecordingExecutor implements SyncSqlExecutor {

        private SqlRequest request;

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            this.request = request;
            return List.of(DynamicRow.copyOf(java.util.Map.of("id", 1L, "name", "Alice")));
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new UnsupportedOperationException("query test executor does not write");
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            throw new UnsupportedOperationException("query test executor does not write");
        }
    }
}
