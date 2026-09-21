package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class H2RelationalSchemaReplacementTest {

    private static final RelationIdentity TABLE = RelationIdentity.of(null, "app", "accounts");

    @Test
    void h2CanTransferUniqueProtectionToANewPrimaryKeyWithoutDroppingTheSharedIndex() throws Exception {
        verifyPrimaryReplacement(List.of(
                "alter table app.accounts add constraint guard_accounts unique (code)",
                "alter table app.accounts drop constraint pk_accounts",
                "alter table app.accounts add constraint pk_accounts primary key (code)",
                "alter table app.accounts drop constraint guard_accounts"));
    }

    @Test
    void reviewedPrimaryKeyReplacementRetainsUniquenessAfterEveryStatement() throws Exception {
        RelationalTableDefinition actual = columns().primaryKey(PrimaryKeyDefinition.of("pk_accounts", "id")).build();
        RelationalTableDefinition desired = columns().primaryKey(PrimaryKeyDefinition.of("pk_accounts", "code")).build();
        ReviewedSchemaPlan plan = review(actual, desired);

        assertFalse(plan.requiresManualAction());
        assertEquals(4, plan.requests().size());
        verifyPrimaryReplacement(plan.requests().stream().map(request -> request.sql()).toList());
        assertEquals(sql(plan), sql(review(actual, desired)), "temporary names must be deterministic");
    }

    @Test
    void establishesNotNullBeforeTransferringThePrimaryKeyToANewColumn() throws Exception {
        RelationalTableDefinition actual = RelationalTableDefinition.builder(TABLE)
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("code", "VARCHAR").length(32).build())
                .primaryKey(PrimaryKeyDefinition.of("pk_accounts", "id")).build();
        RelationalTableDefinition desired = columns().primaryKey(PrimaryKeyDefinition.of("pk_accounts", "code")).build();
        ReviewedSchemaPlan plan = review(actual, desired);

        assertFalse(plan.requiresManualAction());
        assertEquals(5, plan.requests().size());
        assertEquals("alter table app.accounts alter column code set not null", sql(plan).getFirst());
        verifyPrimaryReplacement(sql(plan), true);
    }

    @Test
    void reviewedUniqueReplacementRenamesItsEstablishedTargetProtection() throws Exception {
        RelationalTableDefinition actual = columns()
                .addUnique(UniqueConstraintDefinition.of("uq_accounts", "code")).build();
        RelationalTableDefinition desired = columns()
                .addUnique(UniqueConstraintDefinition.of("uq_accounts", "code", "id")).build();
        ReviewedSchemaPlan plan = review(actual, desired);
        assertFalse(plan.requiresManualAction());
        assertEquals(3, plan.requests().size());

        String url = "jdbc:h2:mem:" + UUID.randomUUID();
        try (Connection ddl = DriverManager.getConnection(url); Connection writer = DriverManager.getConnection(url)) {
            execute(ddl, "create schema app");
            execute(ddl, "create table app.accounts (id bigint not null, code varchar(32) not null,"
                    + " constraint uq_accounts unique (code))");
            execute(ddl, "insert into app.accounts values (1, 'a')");
            for (String request : sql(plan)) {
                execute(ddl, request);
                SQLException duplicate = assertThrows(SQLException.class,
                        () -> execute(writer, "insert into app.accounts values (1, 'a')"));
                assertEquals("23505", duplicate.getSQLState(), request);
            }
            execute(writer, "insert into app.accounts values (2, 'a')");
            try (var statement = ddl.createStatement(); var rows = statement.executeQuery(
                    "select constraint_name from information_schema.table_constraints"
                            + " where table_schema='APP' and table_name='ACCOUNTS' and constraint_type='UNIQUE'")) {
                assertTrue(rows.next());
                assertEquals("UQ_ACCOUNTS", rows.getString(1));
                assertFalse(rows.next(), "the guard must not remain as a second constraint");
            }
        }
    }

    private static void verifyPrimaryReplacement(List<String> requests) throws Exception {
        verifyPrimaryReplacement(requests, false);
    }

    private static void verifyPrimaryReplacement(List<String> requests, boolean initiallyNullable) throws Exception {
        String url = "jdbc:h2:mem:" + UUID.randomUUID();
        try (Connection ddl = DriverManager.getConnection(url); Connection writer = DriverManager.getConnection(url)) {
            execute(ddl, "create schema app");
            execute(ddl, "create table app.accounts (id bigint not null, code varchar(32)"
                    + (initiallyNullable ? "" : " not null") + ','
                    + " constraint pk_accounts primary key (id))");
            execute(ddl, "insert into app.accounts values (1, 'a')");
            for (String request : requests) {
                execute(ddl, request);
                if (initiallyNullable && request.equals(requests.getFirst())) {
                    SQLException nullKey = assertThrows(SQLException.class,
                            () -> execute(writer, "insert into app.accounts values (2, null)"));
                    assertEquals("23502", nullKey.getSQLState());
                    SQLException oldKey = assertThrows(SQLException.class,
                            () -> execute(writer, "insert into app.accounts values (1, 'b')"));
                    assertEquals("23505", oldKey.getSQLState());
                    continue;
                }
                SQLException duplicate = assertThrows(SQLException.class,
                        () -> execute(writer, "insert into app.accounts values (2, 'a')"));
                assertEquals("23505", duplicate.getSQLState(), request);
            }
            try (var keys = ddl.getMetaData().getPrimaryKeys(null, "APP", "ACCOUNTS")) {
                assertTrue(keys.next());
                assertEquals("CODE", keys.getString("COLUMN_NAME"));
                assertEquals("PK_ACCOUNTS", keys.getString("PK_NAME"));
                assertFalse(keys.next());
            }
            execute(writer, "insert into app.accounts values (1, 'b')");
            try (var statement = ddl.createStatement(); var rows = statement.executeQuery(
                    "select count(*) from information_schema.table_constraints"
                            + " where table_schema='APP' and table_name='ACCOUNTS' and constraint_type='UNIQUE'")) {
                assertTrue(rows.next());
                assertEquals(0, rows.getInt(1), "the temporary unique constraint must be removed");
            }
        }
    }

    private static void execute(Connection connection, String sql) throws SQLException {
        try (var statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    private static RelationalTableDefinition.Builder columns() {
        return RelationalTableDefinition.builder(TABLE)
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("code", "VARCHAR").length(32).nullable(false).build());
    }

    private static ReviewedSchemaPlan review(RelationalTableDefinition actual, RelationalTableDefinition desired) {
        RdbDialect dialect = RdbDialect.h2();
        return RelationalSchemaPlanReviewer.create(dialect).review(DatabaseDescriptor.of("H2", "2", dialect),
                desired, SchemaSnapshot.present(actual), SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT);
    }

    private static List<String> sql(ReviewedSchemaPlan plan) {
        return plan.requests().stream().map(request -> request.sql()).toList();
    }
}
