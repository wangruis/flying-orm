package com.flying.orm.rdb.operator;

import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 验证同步原生 SQL 直接使用同步执行器，不依赖响应式阻塞桥。 */
class SyncNativeSqlOperatorTest {

    @Test
    void compilesParametersInSqlOrderAndPassesExecutionOptionsToSyncExecutor() {
        RecordingExecutor executor = new RecordingExecutor(List.of(DynamicRow.copyOf(Map.of("id", 7L))));
        SqlExecutionOptions options = SqlExecutionOptions.maxRows(10);

        List<DynamicRow> rows = nativeSql(executor,
                                          "select id from users where tenant_id = :tenant and owner_id = :owner "
                                                  + "or backup_owner_id = :owner")
                .bind("owner", 7L)
                .bind("tenant", "t-1")
                .options(options)
                .query();

        assertEquals(List.of("t-1", 7L, 7L), executor.request.parameters());
        assertEquals(options, executor.options);
        assertEquals(7L, rows.getFirst().get("id"));
    }

    /** PostgreSQL JDBC 仍然必须得到问号，不能把 R2DBC 的 $1 直接交给 PreparedStatement。 */
    @Test
    void compilesPostgresqlNamedParametersToJdbcQuestionMarkers() {
        RecordingExecutor executor = new RecordingExecutor(List.of());

        new SyncNativeSqlOperator(executor,
                                  ValueCodecRegistry.standard(),
                                  EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults()),
                                  RdbDialect.postgresql(),
                                  "select id from users where tenant_id = :tenant")
                .bind("tenant", "t-1")
                .query();

        assertEquals("select id from users where tenant_id = ?", executor.request.sql());
        assertEquals(List.of("t-1"), executor.request.parameters());
    }

    @Test
    void returnsNullForNoRowAndRejectsMoreThanOneRow() {
        RecordingExecutor empty = new RecordingExecutor(List.of());
        assertNull(nativeSql(empty, "select id from users where id = :id").bind("id", 1L).one());

        RecordingExecutor multiple = new RecordingExecutor(List.of(
                DynamicRow.copyOf(Map.of("id", 1L)), DynamicRow.copyOf(Map.of("id", 2L))));
        assertThrows(IllegalStateException.class,
                     () -> nativeSql(multiple, "select id from users where tenant_id = :tenant")
                             .bind("tenant", "t-1")
                             .one());
    }

    private static SyncNativeSqlOperator nativeSql(RecordingExecutor executor, String sql) {
        return new SyncNativeSqlOperator(executor,
                                         ValueCodecRegistry.standard(),
                                         EntityModelRegistry.create(CacheRegionPolicy.entityMappingDefaults()),
                                         RdbDialect.mysql(),
                                         sql);
    }

    private static final class RecordingExecutor implements SyncSqlExecutor {

        private final List<DynamicRow> rows;
        private SqlRequest request;
        private SqlExecutionOptions options;

        private RecordingExecutor(List<DynamicRow> rows) {
            this.rows = new ArrayList<>(rows);
        }

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            this.request = request;
            return List.copyOf(rows);
        }

        @Override
        public List<DynamicRow> query(SqlRequest request, SqlExecutionOptions options) {
            this.request = request;
            this.options = options;
            return List.copyOf(rows);
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            this.request = request;
            return 1L;
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            this.request = request;
            this.options = options;
            return new SqlWriteResult(1L, List.of());
        }
    }
}
