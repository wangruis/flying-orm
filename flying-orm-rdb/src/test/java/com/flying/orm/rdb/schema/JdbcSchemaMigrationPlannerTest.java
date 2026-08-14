package com.flying.orm.rdb.schema;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;
import com.flying.orm.rdb.protection.ProtectedFormLayout;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 验证 JDBC 规划入口和内置元数据 reader 的缺表信号保持兼容。 */
class JdbcSchemaMigrationPlannerTest {

    /** 已有明文列不能被 Schema FULL_UPDATE 静默改成密文存储，历史数据必须走显式迁移。 */
    @Test
    void rejectsImplicitPlaintextToProtectedColumnMigration() {
        DynamicForm logical = DynamicForm.builder("customer", "customer")
                                         .addField(DynamicField.primaryKey("id", "BIGINT"))
                                         .addField(DynamicField.of("contact", "VARCHAR"))
                                         .encrypted("contact", EncryptedFieldDefinition.builder().build())
                                         .build();
        TableMetadata plaintext = DynamicForm.builder("current", "customer")
                                             .addField(DynamicField.primaryKey("id", "BIGINT"))
                                             .addField(DynamicField.of("contact", "VARCHAR"))
                                             .build()
                                             .toTableMetadata();
        JdbcSchemaMigrationPlanner planner = new JdbcSchemaMigrationPlanner(
                FormSchemaSqlRenderer.create(RdbDialect.postgresql()));

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> planner.plan(logical, List.of(), List.of(), ignored -> plaintext,
                                   SchemaMigrationOptions.safe().allowColumnChange()));

        assertEquals("encrypted field storage requires an explicit plaintext migration", error.getMessage());
    }

    /** 数据库 reader 返回的通用 BLOB 类型必须被识别为已就绪密文列，不能产生伪变更或伪回滚。 */
    @Test
    void recognizesExistingNativeProtectedStorageWithoutRepeatedMigration() {
        DynamicForm logical = DynamicForm.builder("customer", "customer")
                                         .addField(DynamicField.primaryKey("id", "BIGINT"))
                                         .addField(DynamicField.of("contact", "VARCHAR"))
                                         .encrypted("contact", EncryptedFieldDefinition.builder().build())
                                         .build();
        DynamicForm physical = ProtectedFormLayout.physical(logical);
        DynamicForm.Builder current = DynamicForm.builder("current", "customer");
        physical.fields().forEach(field -> current.addField(
                field.dataType().startsWith("PROTECTED_")
                        ? new DynamicField(field.name(), "BLOB", field.primaryKey(), field.nullable(),
                                           field.unique(), field.length(), field.precision(), field.scale(),
                                           field.comment(), field.generation())
                        : field));
        TableMetadata protectedTable = current.build().toTableMetadata();
        JdbcSchemaMigrationPlanner planner = new JdbcSchemaMigrationPlanner(
                FormSchemaSqlRenderer.create(RdbDialect.postgresql()));

        SchemaMigrationPlan plan = planner.plan(
                logical, List.of(), List.of(), ignored -> protectedTable, SchemaMigrationOptions.safe());

        assertTrue(plan.requests().isEmpty());
        assertTrue(plan.skippedChanges().isEmpty());
    }

    /** 已存在但不足 32 字节的盲索引列不能被当成可用结构，否则精确搜索令牌会被截断。 */
    @Test
    void rejectsExistingProtectedHashStorageThatIsTooShort() {
        DynamicForm logical = DynamicForm.builder("customer", "customer")
                                         .addField(DynamicField.primaryKey("id", "BIGINT"))
                                         .addField(DynamicField.of("contact", "VARCHAR"))
                                         .encrypted("contact", EncryptedFieldDefinition.builder().build())
                                         .build();
        DynamicForm physical = ProtectedFormLayout.physical(logical);
        DynamicForm.Builder current = DynamicForm.builder("current", "customer");
        physical.fields().forEach(field -> current.addField(
                "PROTECTED_HASH".equals(field.dataType())
                        ? DynamicField.of(field.name(), "BLOB").withLength(16)
                        : "PROTECTED_BINARY".equals(field.dataType())
                                ? DynamicField.of(field.name(), "BLOB").withNullable(field.nullable())
                                : field));
        JdbcSchemaMigrationPlanner planner = new JdbcSchemaMigrationPlanner(
                FormSchemaSqlRenderer.create(RdbDialect.postgresql()));

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> planner.plan(logical, List.of(), List.of(),
                                   ignored -> current.build().toTableMetadata(), SchemaMigrationOptions.safe()));

        assertEquals("protected search hash storage must hold at least 32 bytes", error.getMessage());
    }

    @Test
    void plannerRecognizesFixedMissingMetadataSignalFromJdbcReader() {
        DynamicForm form = DynamicForm.builder("users", "users")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .build();
        JdbcFormMetadataReader reader = JdbcFormMetadataReaders.create(new EmptyMetadataExecutor(), RdbDialect.h2());
        JdbcSchemaMigrationPlanner planner = new JdbcSchemaMigrationPlanner(
                FormSchemaSqlRenderer.create(RdbDialect.h2()));

        SchemaMigrationPlan plan = planner.plan(form, List.of(), List.of(), reader, SchemaMigrationOptions.safe());

        assertFalse(plan.tableExists());
    }

    /** JDBC 与 R2DBC 共用辅助表幂等规划：缺表时创建，已存在时不重复创建。 */
    @Test
    void plansContainsSideTableIdempotentlyFromJdbcMetadata() {
        DynamicForm form = DynamicForm.builder("customer", "customer")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("contact", "VARCHAR"))
                                      .encrypted("contact", EncryptedFieldDefinition.builder()
                                                                                    .searchModes(
                                                                                            EncryptedSearchMode.CONTAINS)
                                                                                    .build())
                                      .build();
        TableMetadata primary = ProtectedFormLayout.physical(form).toTableMetadata();
        ProtectedContainsLayout layout = ProtectedContainsLayout.resolve(form).orElseThrow();
        JdbcSchemaMigrationPlanner planner = new JdbcSchemaMigrationPlanner(
                FormSchemaSqlRenderer.create(RdbDialect.postgresql()));

        SchemaMigrationPlan missing = planner.plan(form, List.of(), List.of(), table -> {
            if (table.equals(primary.name())) {
                return primary;
            }
            throw new IllegalArgumentException("table metadata not found");
        }, SchemaMigrationOptions.safe());
        SchemaMigrationPlan existing = planner.plan(form, List.of(), List.of(), table ->
                table.equals(primary.name()) ? primary : layout.table().toTableMetadata(),
                SchemaMigrationOptions.safe());

        assertTrue(missing.sqlTexts().stream().anyMatch(sql -> sql.startsWith("create table ")
                && sql.contains(layout.table().table())));
        assertFalse(existing.sqlTexts().stream().anyMatch(sql -> sql.startsWith("create table ")
                && sql.contains(layout.table().table())));
    }

    private static final class EmptyMetadataExecutor implements SyncSqlExecutor {

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            return List.of();
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            throw new UnsupportedOperationException("metadata planner test does not write");
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            throw new UnsupportedOperationException("metadata planner test does not write");
        }
    }
}
