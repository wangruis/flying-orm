package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class MySqlUpsertConflictTargetTest {

    @Test
    void guardsThePrimaryKeyBeforeUpdatingAConflictSelectedByMySql() {
        DynamicForm form = DynamicForm.builder("accounts", "accounts")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("email", "VARCHAR").withUnique(true))
                                      .addField(DynamicField.of("display_name", "VARCHAR"))
                                      .build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.mysql());

        BatchWriteRequest request = renderer.upsertBatch(
                form,
                List.of(Map.of("id", 1L, "email", "ada@example.test", "display_name", "Ada")),
                BatchWriteOptions.defaults());

        String sql = request.sql().toLowerCase();
        assertTrue(sql.contains("`id` = if(`id` <=> values(`id`)"));
        assertTrue(sql.contains("select null union all select null"));
    }

    @Test
    void comparesEveryColumnOfACompositePrimaryKey() {
        DynamicForm form = DynamicForm.builder("memberships", "memberships")
                                      .addField(DynamicField.primaryKey("tenant_id", "BIGINT"))
                                      .addField(DynamicField.primaryKey("user_id", "BIGINT"))
                                      .addField(DynamicField.of("name_col", "VARCHAR"))
                                      .build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.mysql());

        String sql = renderer.upsertBatch(
                form,
                List.of(Map.of("tenant_id", 1L, "user_id", 2L, "name_col", "Ada")),
                BatchWriteOptions.defaults()).sql().toLowerCase();

        assertTrue(sql.contains("`tenant_id` <=> values(`tenant_id`)"));
        assertTrue(sql.contains("`user_id` <=> values(`user_id`)"));
    }

    @Test
    void rejectsAnUpsertThatOmitsAnyCompositePrimaryKeyFieldForEveryDialect() {
        DynamicForm form = DynamicForm.builder("memberships", "memberships")
                                      .addField(DynamicField.of("name_col", "VARCHAR"))
                                      .addField(DynamicField.primaryKey("tenant_id", "BIGINT"))
                                      .addField(DynamicField.primaryKey("id", "BIGINT")
                                                            .generatedByIdentity(1, 1, 1))
                                      .build();
        List<RdbDialect> dialects = List.of(RdbDialect.h2(),
                                            RdbDialect.mysql(),
                                            RdbDialect.postgresql(),
                                            RdbDialect.oracle(),
                                            RdbDialect.sqlServer());

        for (RdbDialect dialect : dialects) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), dialect);

            IllegalArgumentException error = assertThrows(
                    IllegalArgumentException.class,
                    () -> renderer.upsertBatch(
                            form,
                            List.of(Map.of("tenant_id", 1L, "name_col", "Ada")),
                            BatchWriteOptions.defaults()),
                    dialect.name());

            assertEquals("batch upsert requires all primary key fields in submitted values", error.getMessage());
        }
    }
}
