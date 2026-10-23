package com.flying.orm.rdb.internal.plan;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SqlInsertBatchPlanTest {
    @Test
    void onlyNativeUpsertRenderingProvidesBatchRowIndexProvenance() {
        com.flying.orm.rdb.dialect.RdbDialect pg = com.flying.orm.rdb.dialect.RdbDialect.postgresql();
        com.flying.orm.core.form.DynamicForm form = com.flying.orm.core.form.DynamicForm.builder("sample", "sample")
                .addField(com.flying.orm.core.form.DynamicField.primaryKey("id", "BIGINT")).build();
        com.flying.orm.rdb.batch.BatchWriteRequest nativeRequest = com.flying.orm.rdb.form.FormDataSqlRenderer.create(
                com.flying.orm.core.sql.render.SqlRenderer.builder().addDefaultTerms().build(), pg)
                .upsertBatch(form, java.util.List.of(java.util.Map.of("id", 1L)));
        assertEquals("insert into \"sample\" (\"id\") values ($1) on conflict (\"id\") do nothing"
                        + " returning cast($2 as integer)",
                SqlExecutionStatements.r2dbcUpsertBatch(nativeRequest.statement(), "postgresql"));
        assertFalse(nativeRequest.sql().contains("returning"));
        assertEquals(nativeRequest.sql() + " returning cast(? as integer)",
                SqlExecutionStatements.jdbcUpsertBatch(nativeRequest.statement(), "postgresql"));
        assertNull(SqlExecutionStatements.r2dbcUpsertBatch(SqlStatementCompiler.compile(nativeRequest.sql(), 1,
                SqlBindMarkerStyle.CANONICAL, "postgresql"), "postgresql"));
        for (String product : new String[]{"mysql", "oracle", "sqlserver", "h2", "unknown"}) {
            assertNull(SqlExecutionStatements.r2dbcUpsertBatch(nativeRequest.statement(), product));
            assertNull(SqlExecutionStatements.jdbcUpsertBatch(nativeRequest.statement(), product));
        }
        com.flying.orm.rdb.dialect.RdbDialect custom = com.flying.orm.rdb.dialect.RdbDialect.of(
                pg.name(), pg.schema(), pg.pagination(), pg.upsert()::render);
        com.flying.orm.rdb.batch.BatchWriteRequest customRequest = com.flying.orm.rdb.form.FormDataSqlRenderer.create(
                com.flying.orm.core.sql.render.SqlRenderer.builder().addDefaultTerms().build(), custom)
                .upsertBatch(form, java.util.List.of(java.util.Map.of("id", 1L)));
        assertEquals(nativeRequest.sql(), customRequest.sql());
        assertNull(SqlExecutionStatements.r2dbcUpsertBatch(customRequest.statement(), "postgresql"));
        assertNull(SqlExecutionStatements.jdbcUpsertBatch(customRequest.statement(), "postgresql"));
    }

    @Test
    void preservesQuotedQuestionMarksAndTypedExpressionsAcrossRows() {
        SqlStatementPlan plan = SqlStatementCompiler.insert("insert into \"a?b\" (id, payload, label) values ",
                "(?, cast(? as jsonb), '?')", 2, "postgresql");
        assertEquals("insert into \"a?b\" (id, payload, label) values "
                        + "($1, cast($2 as jsonb), '?'), ($3, cast($4 as jsonb), '?')",
                SqlExecutionStatements.r2dbcInsertBatch(plan, "PostgreSQL", 2));
        assertEquals(2, plan.parameterCount());
        assertEquals("insert into \"a?b\" (id, payload, label) values (?, cast(? as jsonb), '?')", plan.sql());
    }

    @Test
    void callerPreparedAndGeneralCompiledSqlNeverAcquireInsertProvenance() {
        String sql = "insert into sample(id) values (?)";
        SqlStatementPlan external = SqlStatementPlan.prepared(sql, SqlBindMarkerStyle.CANONICAL,
                1, "postgresql", "untrusted transport");
        assertNull(SqlExecutionStatements.r2dbcInsertBatch(external, "postgresql", 2));
        assertNull(SqlExecutionStatements.r2dbcInsertBatch(SqlStatementCompiler.compile(sql, 1,
                SqlBindMarkerStyle.CANONICAL, "postgresql"), "postgresql", 2));
    }

    @Test
    void singleRowsAndOtherProductsKeepTheirExistingExecution() {
        SqlStatementPlan plan = SqlStatementCompiler.insert("insert into sample(id) values ", "(?)", 1, "postgresql");
        assertNull(SqlExecutionStatements.r2dbcInsertBatch(plan, "postgresql", 1));
        for (String product : new String[]{"mysql", "oracle", "sqlserver", "h2", "unknown"}) {
            assertNull(SqlExecutionStatements.r2dbcInsertBatch(plan, product, 2));
            SqlStatementPlan other = SqlStatementCompiler.insert("insert into sample(id) values ", "(?)", 1, product);
            assertNull(SqlExecutionStatements.r2dbcInsertBatch(other, "postgresql", 2));
        }
    }

    @Test
    void protocolLimitFallsBackWithoutRejectingUserBufferSize() {
        SqlStatementPlan plan = SqlStatementCompiler.insert("insert into sample(id) values ", "(?)", 1, "postgresql");
        String lastAllowed = SqlExecutionStatements.r2dbcInsertBatch(plan, "postgresql", 65_535);
        assertTrue(lastAllowed.endsWith("($65535)"));
        assertNull(SqlExecutionStatements.r2dbcInsertBatch(plan, "postgresql", 65_536));
        assertNull(SqlExecutionStatements.r2dbcInsertBatch(plan, "postgresql", Integer.MAX_VALUE));
    }

    @Test
    void insertFactoryStillRequiresSingleSqlAndMatchingParameterCount() {
        assertThrows(IllegalArgumentException.class, () -> SqlStatementCompiler.insert(
                "insert into sample(id) values ", "(?); delete from sample", 1, "postgresql"));
        assertThrows(IllegalArgumentException.class, () -> SqlStatementCompiler.insert(
                "insert into sample(id) values ", "(?)", 2, "postgresql"));
    }
}
