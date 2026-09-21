package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;

import com.flying.orm.core.annotation.EncryptedField;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReader;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaAddedColumnOrderTest {

    private static final RdbDialect DIALECT = RdbDialect.h2();
    private static final DatabaseDescriptor DATABASE = DatabaseDescriptor.of("H2", "2", DIALECT);
    private static final RelationIdentity TABLE = RelationIdentity.table("records");

    @Test
    void diffPreservesNewColumnDeclarationOrderForEveryDialect() {
        RelationalTableDefinition desired = table("id", "zeta", "alpha");
        for (RdbDialect dialect : List.of(RdbDialect.h2(), RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            SchemaCompatibilityReport diff = SchemaDiffer.diff(desired, SchemaSnapshot.present(table("id")),
                    dialect.capabilities(), SchemaCompatibilityMode.EXACT);

            assertEquals(List.of("zeta", "alpha"), diff.operations().stream()
                    .filter(operation -> operation.kind() == SchemaOperation.Kind.ADD_COLUMN)
                    .map(SchemaOperation::objectName).toList(), dialect.name());
        }
    }

    @Test
    void publicReviewedExecutionAppendsOrdinaryColumnsInTargetOrder() throws Exception {
        assertH2RoundTrip(table("id", "zeta", "alpha"));
    }

    @Test
    void publicReviewedExecutionAppendsEncryptedEntityColumnsInPhysicalTargetOrder() throws Exception {
        RelationalTableDefinition desired = EntitySchemaDescriptor.builder(EncryptedRecord.class).build().table();
        List<String> columns = desired.columns().stream().map(ColumnDefinition::name).toList();
        assertEquals("secret", columns.get(1));
        assertTrue(columns.get(2).startsWith("__fop_e_"));

        assertH2RoundTrip(desired);
    }

    @Test
    void middleInsertionIsManualBeforeAnyReviewedDdlIsSent() throws Exception {
        try (Database database = new Database("id", "existing")) {
            ReviewedSchemaPlan plan = database.review(table("id", "added", "existing"));

            assertTrue(plan.requiresManualAction());
            assertTrue(plan.steps().stream().anyMatch(step ->
                    step.operation().kind() == SchemaOperation.Kind.VERIFY_MANUALLY
                            && step.operation().objectName().equals("column-order")));
            SchemaExecutionReport execution = database.client.executeReviewed(plan, database.reader,
                    SchemaMigrationApproval.approve(plan, "isolated H2 column-order regression"));

            assertEquals(SchemaExecutionStatus.FAILED, execution.status());
            assertTrue(execution.steps().stream().noneMatch(SchemaExecutionReport.StepResult::sqlSent));
            assertEquals(List.of("id", "existing"), database.columnNames());
        }
    }

    private static void assertH2RoundTrip(RelationalTableDefinition desired) throws Exception {
        try (Database database = new Database("id")) {
            ReviewedSchemaPlan plan = database.review(desired);
            assertFalse(plan.requiresManualAction());

            SchemaExecutionReport execution = database.client.executeReviewed(plan, database.reader,
                    SchemaMigrationApproval.approve(plan, "isolated H2 column-order regression"));

            assertEquals(SchemaExecutionStatus.SUCCESS, execution.status(),
                    execution.verification().toString());
            List<String> targetNames = desired.columns().stream().map(ColumnDefinition::name).toList();
            assertEquals(targetNames, database.columnNames());
            assertEquals(targetNames.subList(1, targetNames.size()), plan.steps().stream()
                    .filter(step -> step.operation().kind() == SchemaOperation.Kind.ADD_COLUMN)
                    .map(step -> step.operation().objectName()).toList());
            assertTrue(database.review(desired).steps().isEmpty(), "readback must match the complete target");
        }
    }

    private static RelationalTableDefinition table(String... names) {
        RelationalTableDefinition.Builder builder = RelationalTableDefinition.builder(TABLE);
        for (String name : names) {
            builder.addColumn(ColumnDefinition.builder(name, "BIGINT").build());
        }
        return builder.build();
    }

    @TableName("records")
    private record EncryptedRecord(Long id, @EncryptedField String secret) {
    }

    private static final class Database implements AutoCloseable {
        private final Connection keeper;
        private final JdbcFormMetadataReader reader;
        private final JdbcSchemaClient client;

        private Database(String... columns) throws Exception {
            JdbcDataSource dataSource = new JdbcDataSource();
            dataSource.setURL("jdbc:h2:mem:column_order_" + UUID.randomUUID());
            keeper = dataSource.getConnection();
            try (Statement statement = keeper.createStatement()) {
                statement.execute("create table records (" + String.join(" bigint, ", columns) + " bigint)");
            }
            SyncSqlExecutor executor = SyncSqlExecutor.jdbc(ConnectionAccessTestSupport.jdbc(dataSource), DIALECT);
            reader = JdbcFormMetadataReaders.create(executor, DIALECT);
            client = JdbcSchemaClient.create(executor, DIALECT);
        }

        private ReviewedSchemaPlan review(RelationalTableDefinition desired) {
            return client.reviewRelational(DATABASE, desired, reader, SchemaCompatibilityMode.EXACT);
        }

        private List<String> columnNames() {
            return reader.readSnapshot(TABLE).columns().value().stream().map(ColumnDefinition::name).toList();
        }

        @Override
        public void close() throws Exception {
            keeper.close();
        }
    }
}
