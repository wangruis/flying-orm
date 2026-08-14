package com.flying.orm.rdb.operator;

import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.Version;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.mapping.FlyingLogicDelete;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 验证实体 Lambda 的同步入口直接执行 JDBC 契约，同时复用响应式入口的字段和安全 SQL 规则。 */
class SyncEntityLambdaDmlOperatorTest {

    @Test
    void queryUpdateAndDeleteUseNativeSyncRuntime() {
        RecordingExecutor executor = new RecordingExecutor();
        SqlRenderer conditionRenderer = SqlRenderer.builder().addDefaultTerms().build();
        FormDataSqlRenderer formRenderer = FormDataSqlRenderer.create(conditionRenderer, RdbDialect.mysql());
        SyncFormClient client = SyncFormClient.create(executor, new UnusedBatchExecutor(), formRenderer);
        SyncEntityDmlOperator<DimensionUser> dml = client.entity(DimensionUser.class);

        List<DynamicRow> rows = dml.query()
                                   .select(DimensionUser::getName)
                                   .where(DimensionUser::getUserId, "u-1")
                                   .in(DimensionUser::getDimensionId, List.of("d-1", "d-2"))
                                   .between(DimensionUser::getVersion, 1L, 9L)
                                   .isNotNull(DimensionUser::getName)
                                   .orderByAsc(DimensionUser::getName)
                                   .executeRows(SqlExecutionOptions.maxRows(10));

        long updated = dml.update()
                          .set(DimensionUser::getName, "new-name")
                          .increment(DimensionUser::getScore, 2)
                          .decrement(DimensionUser::getBalance, 1)
                          .where(DimensionUser::getUserId, "u-1")
                          .isNotNull(DimensionUser::getName)
                          .optimisticLock(3L)
                          .execute(SqlExecutionOptions.safeDefaults());

        long logicallyDeleted = dml.delete()
                                   .where(DimensionUser::getUserId, "u-1")
                                   .notIn(DimensionUser::getDimensionId, List.of("blocked"))
                                   .optimisticLock(3L)
                                   .execute();
        long physicallyDeleted = dml.delete()
                                    .where(DimensionUser::getUserId, "u-2")
                                    .isNull(DimensionUser::getName)
                                    .physical()
                                    .execute(SqlExecutionOptions.safeDefaults());

        assertEquals("Alice", rows.getFirst().get("name"));
        assertEquals(1L, updated);
        assertEquals(1L, logicallyDeleted);
        assertEquals(1L, physicallyDeleted);
        assertEquals(4, executor.requests.size());
        assertTrue(executor.requests.get(0).sql().startsWith("select `name` from `dimension_user`"));
        assertTrue(executor.requests.get(1).sql().startsWith("update `dimension_user` set"));
        assertTrue(executor.requests.get(2).sql().startsWith("update `dimension_user` set `deleted`"));
        assertTrue(executor.requests.get(3).sql().startsWith("delete from `dimension_user`"));
    }

    @TableName("dimension_user")
    @FlyingLogicDelete(field = "deleted")
    private static final class DimensionUser {
        @TableField("user_id")
        private String userId;
        @TableField("dimension_id")
        private String dimensionId;
        private String name;
        private int deleted;
        @Version
        private long version;
        private int score;
        private int balance;

        String getUserId() { return userId; }
        String getDimensionId() { return dimensionId; }
        String getName() { return name; }
        long getVersion() { return version; }
        int getScore() { return score; }
        int getBalance() { return balance; }
    }

    private static final class RecordingExecutor implements SyncSqlExecutor {
        private final List<SqlRequest> requests = new ArrayList<>();

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            requests.add(request);
            return List.of(DynamicRow.copyOf(Map.of("name", "Alice")));
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            requests.add(request);
            return 1L;
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            return new SqlWriteResult(rowsUpdated(request, options), List.of());
        }
    }

    private static final class UnusedBatchExecutor implements SyncBatchExecutor {
        @Override
        public BatchWriteResult writeBatch(BatchWriteRequest request) {
            throw new UnsupportedOperationException("entity DML test does not execute batches");
        }

        @Override
        public List<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
            throw new UnsupportedOperationException("entity DML test does not execute batches");
        }
    }
}
