package com.flying.orm.rdb.form;

import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.internal.mapping.RepositoryUpsertValues;
import com.flying.orm.rdb.jdbc.JdbcBatchWriter;
import org.junit.jupiter.api.Test;
import org.h2.jdbcx.JdbcDataSource;
import reactor.core.publisher.Flux;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class UpsertPurePrimaryKeyTest {

    @Test
    void rendersInsertOrNoOpForEveryBuiltInDialectWhenThereIsNothingToUpdate() {
        DynamicForm form = DynamicForm.builder("pure_keys", "pure_keys")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .build();
        List<RdbDialect> dialects = List.of(RdbDialect.h2(),
                                            RdbDialect.mysql(),
                                            RdbDialect.postgresql(),
                                            RdbDialect.oracle(),
                                            RdbDialect.sqlServer());

        for (RdbDialect dialect : dialects) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), dialect);

            BatchWriteRequest request = assertDoesNotThrow(
                    () -> renderer.upsertBatch(
                            form,
                            List.of(Map.of("id", 1L)),
                            BatchWriteOptions.defaults()),
                    dialect.name());

            assertEquals(1, request.parameterCount(), dialect.name());
            assertNoBusinessUpdate(dialect.name(), request.sql().toLowerCase(Locale.ROOT));
        }
    }

    @Test
    void executesPurePrimaryKeyUpsertTwiceOnH2() throws Exception {
        JdbcDataSource dataSource = dataSource("pure_key_upsert");
        execute(dataSource, "create table pure_keys (id bigint primary key)");
        DynamicForm form = DynamicForm.builder("pure_keys", "pure_keys")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .build();
        FormDataSqlRenderer renderer = renderer();
        BatchWriteRequest request = renderer.upsertBatch(
                form, List.of(Map.of("id", 1L)), BatchWriteOptions.atomic(1));
        JdbcBatchWriter writer = JdbcBatchWriter.create(dataSource);

        assertEquals(BatchWriteResult.Status.COMMITTED, writer.writeBatch(request).status());
        assertEquals(BatchWriteResult.Status.COMMITTED, writer.writeBatch(request).status());

        assertEquals(1L, queryLong(dataSource, "select count(*) from pure_keys"));
    }

    @Test
    void executesIndependentInsertAndUpdateStagesOnH2() throws Exception {
        JdbcDataSource dataSource = dataSource("staged_upsert");
        execute(dataSource, "create table stage_rows ("
                + "id bigint primary key, insert_only varchar(64), update_only varchar(64))");
        DynamicForm form = DynamicForm.builder("stage_rows", "stage_rows")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("insert_only", "VARCHAR"))
                                      .addField(DynamicField.of("update_only", "VARCHAR"))
                                      .build();
        EntityValues<StageRow> entityValues = EntityValues.createUncached(StageRow.class);
        RepositoryUpsertValues first = entityValues.repositoryUpsertValues(
                new StageRow(1L, "first insert", "first update"));
        RepositoryUpsertValues second = entityValues.repositoryUpsertValues(
                new StageRow(1L, "second insert", "second update"));
        BatchInsertPlan plan = renderer().batchRenderer.upsertPlan(form, first, form, first, first);
        BatchWriteRequest request = plan.request(
                Flux.just(plan.parameters(first, 0L), plan.parameters(second, 1L)),
                BatchWriteOptions.atomic(2));

        BatchWriteResult result = JdbcBatchWriter.create(dataSource).writeBatch(request);

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery(
                     "select insert_only, update_only from stage_rows where id = 1")) {
            assertTrue(rows.next());
            assertEquals("first insert", rows.getString(1));
            assertEquals("second update", rows.getString(2));
            assertFalse(rows.next());
        }
    }

    @Test
    void executesTenantCompositeConflictIdentityOnH2() throws Exception {
        JdbcDataSource dataSource = dataSource("tenant_composite_upsert");
        execute(dataSource, "create table tenant_rows (tenant_id varchar(32), id bigint, value_col varchar(64), "
                + "primary key (tenant_id, id))");
        DynamicForm form = DynamicForm.builder("tenant_rows", "tenant_rows")
                                      .addField(DynamicField.primaryKey("tenant_id", "VARCHAR"))
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("value_col", "VARCHAR"))
                                      .tenant("tenant_id", TenantStrategy.AUTO)
                                      .build();
        BatchWriteRequest request = renderer().upsertBatch(
                form,
                List.of(Map.of("tenant_id", "tenant-a", "id", 1L, "value_col", "a-first"),
                        Map.of("tenant_id", "tenant-b", "id", 1L, "value_col", "b-first"),
                        Map.of("tenant_id", "tenant-a", "id", 1L, "value_col", "a-second")),
                BatchWriteOptions.atomic(3));

        BatchWriteResult result = JdbcBatchWriter.create(dataSource).writeBatch(request);

        assertEquals(BatchWriteResult.Status.COMMITTED, result.status());
        assertEquals(2L, queryLong(dataSource, "select count(*) from tenant_rows"));
        assertEquals("a-second", queryString(
                dataSource, "select value_col from tenant_rows where tenant_id = 'tenant-a' and id = 1"));
        assertEquals("b-first", queryString(
                dataSource, "select value_col from tenant_rows where tenant_id = 'tenant-b' and id = 1"));
    }

    private static void assertNoBusinessUpdate(String dialect, String sql) {
        if (sql.contains("on conflict")) {
            assertTrue(sql.contains("do nothing"), dialect + ": " + sql);
            assertFalse(sql.contains("do update"), dialect + ": " + sql);
            return;
        }
        if (sql.contains("on duplicate key")) {
            assertTrue(sql.contains("if("), dialect + ": " + sql);
            assertFalse(sql.contains(" = values("), dialect + ": " + sql);
            return;
        }
        assertFalse(sql.contains("when matched then update"), dialect + ": " + sql);
        assertFalse(sql.contains("update set"), dialect + ": " + sql);
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.h2());
    }

    private static JdbcDataSource dataSource(String name) {
        JdbcDataSource dataSource = new JdbcDataSource();
        dataSource.setURL("jdbc:h2:mem:" + name + ";DB_CLOSE_DELAY=-1");
        dataSource.setUser("sa");
        dataSource.setPassword("");
        return dataSource;
    }

    private static void execute(JdbcDataSource dataSource, String sql) throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static long queryLong(JdbcDataSource dataSource, String sql) throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery(sql)) {
            assertTrue(rows.next());
            return rows.getLong(1);
        }
    }

    private static String queryString(JdbcDataSource dataSource, String sql) throws Exception {
        try (Connection connection = dataSource.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery(sql)) {
            assertTrue(rows.next());
            return rows.getString(1);
        }
    }

    @TableName("stage_rows")
    private static final class StageRow {

        @TableId(type = IdType.INPUT)
        private final Long id;

        @TableField(value = "insert_only",
                    insertStrategy = FieldStrategy.ALWAYS,
                    updateStrategy = FieldStrategy.NEVER)
        private final String insertOnly;

        @TableField(value = "update_only",
                    insertStrategy = FieldStrategy.NEVER,
                    updateStrategy = FieldStrategy.ALWAYS)
        private final String updateOnly;

        private StageRow(Long id, String insertOnly, String updateOnly) {
            this.id = id;
            this.insertOnly = insertOnly;
            this.updateOnly = updateOnly;
        }

        public Long getId() {
            return id;
        }

        public String getInsertOnly() {
            return insertOnly;
        }

        public String getUpdateOnly() {
            return updateOnly;
        }
    }
}
