package com.flying.orm.rdb.operator;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.JdbcSchemaClient;
import com.flying.orm.rdb.schema.SchemaMigrationReviewPolicy;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 验证同步 DDL 的原生 JDBC 入口不会依赖响应式 DDL builder。 */
class SyncDdlOperatorTest {

    @Test
    void nativePathKeepsStructureDslWhenPlanningAndReviewingForeignKeys() {
        RecordingExecutor executor = new RecordingExecutor();
        RdbDialect dialect = RdbDialect.h2();
        SyncDdlOperator ddl = SyncDdlOperator.create(
                JdbcSchemaClient.create(executor, dialect),
                JdbcFormMetadataReaders.create(executor, dialect));

        SyncCreateOrAlterTableBuilder table = ddl.createOrAlter("orders")
                                                  .addColumn().name("id").number(19).primaryKey().commit()
                                                  .addColumn().name("user_id").number(19).commit()
                                                  .addIndex("idx_orders_user").column("user_id").commit()
                                                  .addForeignKey("fk_orders_user")
                                                  .column("user_id")
                                                  .referenceTable("users")
                                                  .referenceColumn("id")
                                                  .commit();

        // 空元数据代表表尚未创建。plan 和 review 只能读库，不能提前下发 DDL。
        var plan = table.plan();
        var reviewed = table.review(SchemaMigrationReviewPolicy.allowBlocking());

        assertEquals(1, plan.targetForeignKeys().size());
        assertEquals("fk_orders_user", plan.targetForeignKeys().getFirst().name());
        assertEquals(List.of(), executor.writes());

        assertEquals("orders", reviewed.migration().target().table());
    }

    @Test
    void nativeCommitUsesSameDslWithoutReactiveAwait() {
        RecordingExecutor executor = new RecordingExecutor();
        RdbDialect dialect = RdbDialect.h2();
        SyncDdlOperator ddl = SyncDdlOperator.create(
                JdbcSchemaClient.create(executor, dialect),
                JdbcFormMetadataReaders.create(executor, dialect));

        long rows = ddl.createOrAlter("audit_log")
                       .addColumn().name("id").number(19).primaryKey().comment("primary key").commit()
                       .addColumn().name("message").varchar(128).commit()
                       .commit();

        assertEquals(executor.writes().size(), rows);
        assertTrue(rows > 0L);
        assertTrue(executor.writes().getFirst().sql().startsWith("create table audit_log"));
    }

    @Test
    void nativePathExecutesReviewedPlanDirectly() {
        RecordingExecutor executor = new RecordingExecutor();
        RdbDialect dialect = RdbDialect.h2();
        SyncDdlOperator ddl = SyncDdlOperator.create(
                JdbcSchemaClient.create(executor, dialect),
                JdbcFormMetadataReaders.create(executor, dialect));

        SyncCreateOrAlterTableBuilder table = ddl.createOrAlter("report_cache")
                                                  .addColumn().name("id").number(19).primaryKey().commit();
        long rows = table.executeReviewed(table.review(SchemaMigrationReviewPolicy.allowBlocking())).rowsUpdated();

        assertEquals(executor.writes().size(), rows);
        assertTrue(rows > 0L);
        assertTrue(executor.writes().getFirst().sql().startsWith("create table report_cache"));
    }

    /**
     * 这个替身只模拟两件事：元数据查询没有行，表示表不存在；每条 DDL 返回一行成功。
     * 它让测试能够验证同步编排边界，而不是把真实数据库留在单元测试里。
     */
    private static final class RecordingExecutor implements SyncSqlExecutor {

        private final List<SqlRequest> writes = new ArrayList<>();

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            return List.of();
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            writes.add(request);
            return 1L;
        }

        @Override
        public long rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
            return rowsUpdated(request);
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            return new SqlWriteResult(rowsUpdated(request), List.of());
        }

        private List<SqlRequest> writes() {
            return List.copyOf(writes);
        }
    }
}
