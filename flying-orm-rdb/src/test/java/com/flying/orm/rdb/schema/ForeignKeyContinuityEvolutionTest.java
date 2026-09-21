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
import java.util.regex.Pattern;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ForeignKeyContinuityEvolutionTest {

    private static final RelationIdentity TABLE = RelationIdentity.of(null, "app", "accounts");
    private static final RelationIdentity PARENT = RelationIdentity.of(null, "app", "parents");
    private static final List<RdbDialect> DIALECTS = List.of(RdbDialect.h2(), RdbDialect.sqlServer());
    private static final List<ReferentialAction> ACTIONS = List.of(ReferentialAction.NO_ACTION, ReferentialAction.CASCADE);
    private static final Pattern GUARD = Pattern.compile("fo_guard_[a-f0-9]{20}");

    @Test
    void deleteAndUpdateActionChangesUseANonCascadingGuardOnH2AndSqlServer() {
        for (RdbDialect dialect : DIALECTS) {
            for (ReferentialAction sourceDelete : ACTIONS) {
                for (ReferentialAction sourceUpdate : ACTIONS) {
                    for (ReferentialAction targetDelete : ACTIONS) {
                        for (ReferentialAction targetUpdate : ACTIONS) {
                            if (sourceDelete == targetDelete && sourceUpdate == targetUpdate) continue;
                            var actual = key("parent_id", PARENT, "id", sourceDelete, sourceUpdate);
                            var desired = key("parent_id", PARENT, "id", targetDelete, targetUpdate);
                            List<String> sql = render(dialect, actual, desired);
                            assertGuardedSql(dialect, desired, sql);
                            assertEquals(sql, render(dialect, actual, desired), "guard naming must be deterministic");
                        }
                    }
                }
            }
        }
    }

    @Test
    void changedLocalAndReferencedColumnsAreProtectedByTheTargetRelationship() {
        var actual = key("parent_id", PARENT, "id", ReferentialAction.CASCADE, ReferentialAction.NO_ACTION);
        var desired = key("next_parent", RelationIdentity.of(null, "app", "parents_v2"), "code",
                ReferentialAction.NO_ACTION, ReferentialAction.CASCADE);
        for (RdbDialect dialect : DIALECTS) {
            assertGuardedSql(dialect, desired, render(dialect, actual, desired));
        }
    }

    @Test
    void publicReviewDistinguishesContinuousForeignKeysFromOracleQuiescedReplacement() {
        var actual = table(key(ReferentialAction.NO_ACTION, ReferentialAction.NO_ACTION));
        var desired = table(key(ReferentialAction.CASCADE, ReferentialAction.NO_ACTION));
        for (RdbDialect dialect : List.of(RdbDialect.h2(), RdbDialect.sqlServer(), RdbDialect.oracle())) {
            ReviewedSchemaPlan plan = review(dialect, actual, desired);
            if (dialect.name().equals("oracle")) {
                assertFalse(plan.requiresManualAction());
                assertEquals(2, plan.requests().size());
                assertTrue(plan.requiresWritesQuiesced());
                assertFalse(plan.acceptsApproval(SchemaMigrationApproval.approve(plan, "ordinary approval")));
            } else {
                assertFalse(plan.requiresManualAction(), dialect.name());
                assertFalse(plan.requiresWritesQuiesced());
                assertEquals(4, plan.requests().size(), dialect.name());
                assertTrue(plan.operations().stream().allMatch(operation ->
                        operation.kind() == SchemaOperation.Kind.CHANGE_FOREIGN_KEY));
                assertEquals(plan.fingerprint(), review(dialect, actual, desired).fingerprint());
            }
        }
    }

    @Test
    void h2RejectsOrphanInsertsAndUpdatesAfterEveryDdlStep() throws Exception {
        var actual = key(ReferentialAction.NO_ACTION, ReferentialAction.NO_ACTION);
        var desired = key(ReferentialAction.CASCADE, ReferentialAction.CASCADE);
        List<String> sql = render(RdbDialect.h2(), actual, desired);
        try (var connection = DriverManager.getConnection("jdbc:h2:mem:fk_steps_" + UUID.randomUUID());
             var statement = connection.createStatement()) {
            initialize(statement);
            for (String ddl : sql) {
                statement.execute(ddl);
                assertEquals("23506", assertThrows(SQLException.class,
                        () -> statement.execute("insert into app.accounts values(11, 999)"), ddl).getSQLState());
                assertEquals("23506", assertThrows(SQLException.class,
                        () -> statement.execute("update app.accounts set parent_id = 999 where id = 10"), ddl).getSQLState());
            }
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
                assertFalse(keys.next(), "the temporary guard must not survive successful replacement");
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
            assertFalse(plan.requiresManualAction());
            var report = client.executeReviewed(plan, reader,
                    SchemaMigrationApproval.approve(plan, "isolated H2 foreign-key continuity regression"));
            assertEquals(SchemaExecutionStatus.SUCCESS, report.status(), report.verification().toString());
            assertTrue(client.reviewRelational(database, desired, reader, SchemaCompatibilityMode.EXACT).steps().isEmpty());
        }
    }

    private static void assertGuardedSql(RdbDialect dialect, ForeignKeyDefinition desired, List<String> sql) {
        var match = GUARD.matcher(sql.getFirst());
        assertTrue(match.find(), sql.toString());
        String guard = match.group();
        SchemaDialect schema = dialect.schema();
        String alter = "alter table " + schema.relationIdentifier(TABLE);
        String relationship = " foreign key (" + schema.identifier(desired.columns().getFirst()) + ") references "
                + schema.relationIdentifier(desired.reference())
                + " (" + schema.identifier(desired.referenceColumns().getFirst()) + ')';
        String actions = (desired.onDelete() == ReferentialAction.CASCADE ? " on delete cascade" : "")
                + (desired.onUpdate() == ReferentialAction.CASCADE ? " on update cascade" : "");
        assertEquals(List.of(alter + " add constraint " + schema.identifier(guard) + relationship,
                alter + " drop constraint " + schema.identifier("fk_accounts"),
                alter + " add constraint " + schema.identifier("fk_accounts") + relationship + actions,
                alter + " drop constraint " + schema.identifier(guard)), sql, dialect.name());
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

    private static ReviewedSchemaPlan review(RdbDialect dialect, RelationalTableDefinition actual,
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
