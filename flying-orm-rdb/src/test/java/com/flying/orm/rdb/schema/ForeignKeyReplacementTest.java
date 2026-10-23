package com.flying.orm.rdb.schema;

import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.ReferentialAction;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.metadata.JdbcFormMetadataReaders;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.h2.jdbcx.JdbcDataSource;
import org.junit.jupiter.api.Test;

import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ForeignKeyReplacementTest {

    private static final RelationIdentity TABLE = RelationIdentity.of(null, "app", "accounts");
    private static final RelationIdentity PARENT = RelationIdentity.of(null, "app", "parents");
    private static final List<RdbDialect> DIALECTS = List.of(RdbDialect.h2(), RdbDialect.sqlServer());
    private static final List<ReferentialAction> ACTIONS = List.of(ReferentialAction.NO_ACTION, ReferentialAction.CASCADE);

    @Test
    void columnEvolutionExecutesBetweenForeignKeyRemovalAndRecreation() throws Exception {
        ForeignKeyDefinition before = key(ReferentialAction.NO_ACTION, ReferentialAction.NO_ACTION);
        ForeignKeyDefinition after = key("parent_id", RelationIdentity.of(null, "app", "owners"), "id",
                ReferentialAction.NO_ACTION, ReferentialAction.NO_ACTION);
        RelationalTableDefinition actual = RelationalTableDefinition.builder(TABLE)
                .addColumn(ColumnDefinition.builder("id", "INTEGER").nullable(false).build())
                .addColumn(ColumnDefinition.builder("parent_id", "VARCHAR").length(32).build())
                .primaryKey(PrimaryKeyDefinition.of("pk_accounts", "id")).addForeignKey(before).build();
        RelationalTableDefinition desired = RelationalTableDefinition.builder(TABLE)
                .addColumn(actual.column("id"))
                .addColumn(ColumnDefinition.builder("parent_id", "VARCHAR").length(64).build())
                .primaryKey(actual.primaryKey().orElseThrow()).addForeignKey(after).build();
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer(), RdbDialect.h2())) {
            assertEquals(List.of(SchemaOperation.Kind.DROP_FOREIGN_KEY, SchemaOperation.Kind.CHANGE_COLUMN,
                    SchemaOperation.Kind.ADD_FOREIGN_KEY), review(dialect, actual, desired).operations().stream()
                    .map(SchemaOperation::kind).toList(), dialect.name());
        }
        SchemaMigrationPlan plan = review(RdbDialect.h2(), actual, desired);
        try (java.sql.Connection connection = DriverManager.getConnection("jdbc:h2:mem:fk_columns_" + UUID.randomUUID());
             Statement statement = connection.createStatement()) {
            statement.execute("create schema app");
            statement.execute("create table app.parents(id varchar(32) primary key)");
            statement.execute("create table app.owners(id varchar(64) primary key)");
            statement.execute("create table app.accounts(id int primary key, parent_id varchar(32), "
                    + "constraint fk_accounts foreign key(parent_id) references app.parents(id))");
            statement.execute("insert into app.parents values('one')");
            statement.execute("insert into app.owners values('one')");
            statement.execute("insert into app.accounts values(10, 'one')");
            for (String sql : plan.sqlTexts()) statement.execute(sql);
            try (java.sql.ResultSet row = statement.executeQuery("select parent_id from app.accounts where id = 10")) {
                assertTrue(row.next());
                assertEquals("one", row.getString(1));
            }
            assertEquals("23506", assertThrows(SQLException.class,
                    () -> statement.executeUpdate("update app.accounts set parent_id = 'missing' where id = 10")).getSQLState());
        }
    }

    @Test
    void deleteAndUpdateActionChangesReplaceOnlyTheDeclaredForeignKey() {
        for (RdbDialect dialect : DIALECTS) {
            for (ReferentialAction sourceDelete : ACTIONS) {
                for (ReferentialAction sourceUpdate : ACTIONS) {
                    for (ReferentialAction targetDelete : ACTIONS) {
                        for (ReferentialAction targetUpdate : ACTIONS) {
                            if (sourceDelete == targetDelete && sourceUpdate == targetUpdate) continue;
                            var actual = key("parent_id", PARENT, "id", sourceDelete, sourceUpdate);
                            var desired = key("parent_id", PARENT, "id", targetDelete, targetUpdate);
                            List<String> sql = render(dialect, actual, desired);
                            assertReplacementSql(dialect, desired, sql);
                            assertEquals(sql, render(dialect, actual, desired));
                        }
                    }
                }
            }
        }
    }

    @Test
    void changedLocalAndReferencedColumnsUseTheTargetRelationship() {
        var actual = key("parent_id", PARENT, "id", ReferentialAction.CASCADE, ReferentialAction.NO_ACTION);
        var desired = key("next_parent", RelationIdentity.of(null, "app", "parents_v2"), "code",
                ReferentialAction.NO_ACTION, ReferentialAction.CASCADE);
        for (RdbDialect dialect : DIALECTS) {
            assertReplacementSql(dialect, desired, render(dialect, actual, desired));
        }
    }

    @Test
    void publicReviewRetainsDialectSpecificForeignKeyReplacement() {
        var actual = table(key(ReferentialAction.NO_ACTION, ReferentialAction.NO_ACTION));
        var desired = table(key(ReferentialAction.CASCADE, ReferentialAction.NO_ACTION));
        for (RdbDialect dialect : List.of(RdbDialect.h2(), RdbDialect.sqlServer(), RdbDialect.oracle())) {
            SchemaMigrationPlan plan = review(dialect, actual, desired);
            assertEquals(2, plan.requests().size(), dialect.name());
            assertTrue(plan.operations().stream().allMatch(operation ->
                    operation.kind() == SchemaOperation.Kind.CHANGE_FOREIGN_KEY));
            assertEquals(plan.requests(), review(dialect, actual, desired).requests());
        }
    }

    @Test
    void h2ReplacementRejectsOrphansAndAppliesTargetCascadeActions() throws Exception {
        var actual = key(ReferentialAction.NO_ACTION, ReferentialAction.NO_ACTION);
        var desired = key(ReferentialAction.CASCADE, ReferentialAction.CASCADE);
        List<String> sql = render(RdbDialect.h2(), actual, desired);
        try (var connection = DriverManager.getConnection("jdbc:h2:mem:fk_steps_" + UUID.randomUUID());
             var statement = connection.createStatement()) {
            initialize(statement);
            for (String ddl : sql) {
                statement.execute(ddl);
            }
            assertEquals("23506", assertThrows(SQLException.class,
                    () -> statement.execute("insert into app.accounts values(11, 999)")).getSQLState());
            assertEquals("23506", assertThrows(SQLException.class,
                    () -> statement.execute("update app.accounts set parent_id = 999 where id = 10")).getSQLState());
            assertEquals(1, statement.executeUpdate("update app.parents set id = 2 where id = 1"));
            try (var row = statement.executeQuery("select parent_id from app.accounts where id = 10")) {
                assertTrue(row.next());
                assertEquals(2, row.getInt(1));
            }
            assertEquals(1, statement.executeUpdate("delete from app.parents where id = 2"));
            try (var rows = statement.executeQuery("select count(*) from app.accounts")) {
                assertTrue(rows.next());
                assertEquals(0, rows.getInt(1));
            }
            try (var keys = connection.getMetaData().getImportedKeys(null, "APP", "ACCOUNTS")) {
                assertTrue(keys.next());
                assertEquals("FK_ACCOUNTS", keys.getString("FK_NAME"));
                assertEquals(DatabaseMetaData.importedKeyCascade, keys.getShort("DELETE_RULE"));
                assertEquals(DatabaseMetaData.importedKeyCascade, keys.getShort("UPDATE_RULE"));
                assertFalse(keys.next(), "only the declared foreign key should exist");
            }
        }
    }

    @Test
    void publicH2ReviewedExecutionChangesActionsAndReadsBackTheNamedTarget() throws Exception {
        JdbcDataSource source = new JdbcDataSource();
        source.setURL("jdbc:h2:mem:fk_reviewed_" + UUID.randomUUID());
        RdbDialect dialect = RdbDialect.h2();
        try (var keeper = source.getConnection(); var statement = keeper.createStatement()) {
            initialize(statement);
            var executor = SyncSqlExecutor.jdbc(ConnectionAccessTestSupport.jdbc(source), dialect);
            var reader = JdbcFormMetadataReaders.create(executor, dialect);
            var client = JdbcSchemaClient.create(executor, dialect);
            var desired = table(key(ReferentialAction.CASCADE, ReferentialAction.CASCADE));
            var database = DatabaseDescriptor.of("H2", "2.4.240", dialect);
            var plan = client.reviewRelational(database, desired, reader, SchemaCompatibilityMode.EXACT);
            var report = client.execute(plan, reader);
            assertNotNull(report);
            assertTrue(client.reviewRelational(database, desired, reader, SchemaCompatibilityMode.EXACT).steps().isEmpty());
        }
    }

    private static void assertReplacementSql(RdbDialect dialect, ForeignKeyDefinition desired, List<String> sql) {
        SchemaDialect schema = dialect.schema();
        String alter = "alter table " + schema.relationIdentifier(TABLE);
        String relationship = " foreign key (" + schema.identifier(desired.columns().getFirst()) + ") references "
                + schema.relationIdentifier(desired.reference())
                + " (" + schema.identifier(desired.referenceColumns().getFirst()) + ')';
        String actions = (desired.onDelete() == ReferentialAction.CASCADE ? " on delete cascade" : "")
                + (desired.onUpdate() == ReferentialAction.CASCADE ? " on update cascade" : "");
        assertEquals(List.of(alter + " drop constraint " + schema.identifier("fk_accounts"),
                alter + " add constraint " + schema.identifier("fk_accounts") + relationship + actions), sql, dialect.name());
    }

    private static List<String> render(RdbDialect dialect, ForeignKeyDefinition actual, ForeignKeyDefinition desired) {
        var operation = SchemaOperation.of(SchemaOperation.Kind.CHANGE_FOREIGN_KEY, TABLE, desired.name(), actual, desired,
                SchemaOperation.Compatibility.REQUIRES_REVIEW);
        return RelationalSchemaSqlRenderer.create(dialect.schema()).render(operation)
                .stream().map(request -> request.sql()).toList();
    }

    private static ForeignKeyDefinition key(ReferentialAction delete, ReferentialAction update) {
        return key("parent_id", PARENT, "id", delete, update);
    }

    private static ForeignKeyDefinition key(String column, RelationIdentity reference, String referenceColumn,
                                            ReferentialAction delete, ReferentialAction update) {
        return ForeignKeyDefinition.builder("fk_accounts").addColumn(column).reference(reference)
                .addReferenceColumn(referenceColumn).onDelete(delete).onUpdate(update).build();
    }

    private static RelationalTableDefinition table(ForeignKeyDefinition foreignKey) {
        return RelationalTableDefinition.builder(TABLE)
                .addColumn(ColumnDefinition.builder("id", "INTEGER").nullable(false).build())
                .addColumn(ColumnDefinition.builder("parent_id", "INTEGER").build())
                .primaryKey(PrimaryKeyDefinition.of("pk_accounts", "id")).addForeignKey(foreignKey).build();
    }

    private static SchemaMigrationPlan review(RdbDialect dialect, RelationalTableDefinition actual,
                                              RelationalTableDefinition desired) {
        return RelationalSchemaPlanReviewer.create(dialect).review(DatabaseDescriptor.of(dialect.name(), "test", dialect),
                desired, SchemaSnapshot.present(actual), SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT);
    }

    private static void initialize(Statement statement) throws SQLException {
        statement.execute("create schema app");
        statement.execute("create table app.parents(id int primary key)");
        statement.execute("create table app.accounts(id int not null, parent_id int, "
                + "constraint pk_accounts primary key(id), "
                + "constraint fk_accounts foreign key(parent_id) references app.parents(id))");
        statement.execute("insert into app.parents values(1)");
        statement.execute("insert into app.accounts values(10, 1)");
    }
}
