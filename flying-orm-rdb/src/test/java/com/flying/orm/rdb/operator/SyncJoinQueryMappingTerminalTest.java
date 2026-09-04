package com.flying.orm.rdb.operator;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.mapping.RowMapper;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SyncJoinQueryMappingTerminalTest {

    @Test
    void mapsDynamicJoinRowsInsideTheSyncExecutorTerminal() {
        DynamicForm form = DynamicForm.builder("people", "people")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("name", "VARCHAR"))
                                      .build();
        SqlRenderer sqlRenderer = SqlRenderer.builder().addDefaultTerms().build();
        CapturingSqlExecutor executor = new CapturingSqlExecutor();
        SyncFormClient client = SyncFormClient.create(
                executor, new UnusedBatchExecutor(),
                FormDataSqlRenderer.create(sqlRenderer, RdbDialect.h2()));

        List<DynamicRow> rows = new SyncJoinQueryOperator(client, sqlRenderer, form)
                .selectAs(form, "name", "person_name")
                .executeRows();

        assertEquals("Ada", rows.getFirst().get("person_name"));
        assertEquals(1, executor.mappedQueries);
        assertEquals(0, executor.materializedQueries,
                     "dynamic JOIN mapping must not first materialize an intermediate row list");
    }

    @Test
    void mapsJoinRowsInsideTheSyncExecutorTerminal() {
        DynamicForm form = DynamicForm.builder("people", "people")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("name", "VARCHAR"))
                                      .build();
        SqlRenderer sqlRenderer = SqlRenderer.builder().addDefaultTerms().build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(sqlRenderer, RdbDialect.h2());
        CapturingSqlExecutor executor = new CapturingSqlExecutor();
        SyncFormClient client = SyncFormClient.create(executor, new UnusedBatchExecutor(), renderer);
        SyncJoinQueryOperator query = new SyncJoinQueryOperator(client, sqlRenderer, form)
                .selectAs(form, "name", "person_name");

        List<String> names = query.execute(row -> row.get("person_name", String.class));

        assertEquals(List.of("Ada"), names);
        assertEquals(1, executor.mappedQueries);
        assertEquals(0, executor.materializedQueries,
                     "typed JOIN mapping must not first materialize a DynamicRow list");
    }

    private static final class CapturingSqlExecutor implements SyncSqlExecutor {

        private final DynamicRow row = DynamicRow.copyOf(Map.of("person_name", "Ada"));
        private int materializedQueries;
        private int mappedQueries;

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            materializedQueries++;
            return List.of(row);
        }

        @Override
        public List<DynamicRow> query(SqlRequest request, SqlExecutionOptions options) {
            return query(request);
        }

        @Override
        public <T> List<T> queryMapped(SqlRequest request,
                                       SqlExecutionOptions options,
                                       RowMapper<T> mapper,
                                       int rowLimit) {
            mappedQueries++;
            return List.of(mapper.map(row));
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            throw new UnsupportedOperationException();
        }
    }

    private static final class UnusedBatchExecutor implements SyncBatchExecutor {

        @Override
        public BatchWriteResult writeBatch(BatchWriteRequest request) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
            throw new UnsupportedOperationException();
        }
    }
}
