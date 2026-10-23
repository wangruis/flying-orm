package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.metadata.UniqueNullPolicy;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ConstraintReplacementTest {

    private static final RelationIdentity TABLE = RelationIdentity.of(null, "app", "accounts");

    @Test
    void replacementDoesNotInstallOrmOwnedTemporaryConstraints() {
        for (RdbDialect dialect : List.of(RdbDialect.h2(), RdbDialect.sqlServer(), RdbDialect.oracle())) {
            List<String> sql = render(dialect, SchemaOperation.Kind.CHANGE_PRIMARY_KEY, "pk_accounts",
                    PrimaryKeyDefinition.of("pk_accounts", "id"),
                    PrimaryKeyDefinition.of("pk_accounts", "id", "tenant"));
            assertEquals(2, sql.size(), dialect.name());
            assertFalse(sql.stream().anyMatch(statement -> statement.contains("fo_guard_")), dialect.name());
        }
    }

    @Test
    void oracleExpandedPrimaryKeyReplacesTheNamedConstraintDirectly() {
        var actual = PrimaryKeyDefinition.of("pk_accounts", "id");
        var desired = PrimaryKeyDefinition.of("pk_accounts", "id", "tenant");
        List<String> sql = render(RdbDialect.oracle(), SchemaOperation.Kind.CHANGE_PRIMARY_KEY,
                desired.name(), actual, desired);
        assertEquals(List.of(
                "alter table \"app\".\"accounts\" drop constraint \"pk_accounts\" drop index",
                "alter table \"app\".\"accounts\" add constraint \"pk_accounts\" primary key (\"id\", \"tenant\")"
                        + " enable validate"), sql);
    }

    @Test
    void oraclePrimaryKeyDoesNotSilentlyDropNotNullFromAnOldKeyColumn() {
        assertThrows(UnsupportedOperationException.class, () -> render(RdbDialect.oracle(),
                SchemaOperation.Kind.CHANGE_PRIMARY_KEY, "pk_accounts",
                PrimaryKeyDefinition.of("pk_accounts", "id"), PrimaryKeyDefinition.of("pk_accounts", "tenant")));
    }

    @Test
    void sqlServerPrimaryKeyReplacesTheNamedConstraintDirectly() {
        var actual = PrimaryKeyDefinition.of("pk_accounts", "id");
        var desired = PrimaryKeyDefinition.of("pk_accounts", "id", "tenant");
        List<String> sql = render(RdbDialect.sqlServer(), SchemaOperation.Kind.CHANGE_PRIMARY_KEY,
                desired.name(), actual, desired);
        assertEquals(List.of(
                "alter table [app].[accounts] drop constraint [pk_accounts]",
                "alter table [app].[accounts] add constraint [pk_accounts] primary key ([id], [tenant])"), sql);
    }

    @Test
    void sqlServerDefaultUniqueReplacesTheNamedConstraintDirectly() {
        var actual = unique("uq_accounts", UniqueNullPolicy.DEFAULT, "code");
        var desired = unique("uq_accounts", UniqueNullPolicy.DEFAULT, "code", "tenant");
        List<String> sql = render(RdbDialect.sqlServer(), SchemaOperation.Kind.CHANGE_UNIQUE,
                desired.name(), actual, desired);
        assertEquals(List.of(
                "alter table [app].[accounts] drop constraint [uq_accounts]",
                "alter table [app].[accounts] add constraint [uq_accounts] unique ([code], [tenant])"), sql);
    }

    @Test
    void standaloneUniqueIndexesRebuildTheNamedIndexDirectly() {
        var actual = index("ux_accounts", IndexKeyPart.asc("code"));
        var desired = index("ux_accounts", IndexKeyPart.asc("code"), IndexKeyPart.desc("tenant"));
        for (RdbDialect dialect : List.of(RdbDialect.h2(), RdbDialect.postgresql(), RdbDialect.oracle())) {
            List<String> sql = render(dialect, SchemaOperation.Kind.CHANGE_INDEX, desired.name(), actual, desired);
            SchemaDialect schema = dialect.schema();
            String indexName = dialect.name().equals("oracle")
                    ? schema.schemaObjectIdentifier(TABLE, desired.name()) : schema.identifier(desired.name());
            assertEquals(List.of(
                    "drop index " + schema.schemaObjectIdentifier(TABLE, actual.name()),
                    "create unique index " + indexName + " on " + schema.relationIdentifier(TABLE)
                            + " (" + schema.identifier("code") + " asc, " + schema.identifier("tenant") + " desc)"),
                    sql, dialect.name());
            assertEquals(sql, render(dialect, SchemaOperation.Kind.CHANGE_INDEX, desired.name(), actual, desired));
        }
    }

    @Test
    void sqlServerStandaloneUniqueIndexChangesInOneDropExistingStatement() {
        var actual = index("ux_accounts", IndexKeyPart.asc("code"));
        var desired = index("ux_accounts", IndexKeyPart.asc("code"), IndexKeyPart.desc("tenant"));
        assertEquals(List.of("create unique index [ux_accounts] on [app].[accounts] ([code] asc, [tenant] desc)"
                        + " with (drop_existing = on)"),
                render(RdbDialect.sqlServer(), SchemaOperation.Kind.CHANGE_INDEX, desired.name(), actual, desired));
    }

    @Test
    void oracleUniquePolicyChangesReplaceThePhysicalConstraintOrIndex() {
        for (UniqueNullPolicy source : UniqueNullPolicy.values()) {
            for (UniqueNullPolicy target : UniqueNullPolicy.values()) {
                var actual = unique("uq_accounts", source, "code", "tenant");
                var desired = unique("uq_accounts", target, "code", "region");
                List<String> sql = render(RdbDialect.oracle(), SchemaOperation.Kind.CHANGE_UNIQUE,
                        desired.name(), actual, desired);
                String create = target == UniqueNullPolicy.DEFAULT
                        ? "alter table \"app\".\"accounts\" add constraint \"uq_accounts\" unique (\"code\", \"region\")"
                        : "create unique index \"app\".\"uq_accounts\" on \"app\".\"accounts\""
                                + " (case when \"code\" is not null and \"region\" is not null then \"code\" end, "
                                + "case when \"code\" is not null and \"region\" is not null then \"region\" end)";
                String drop = source == UniqueNullPolicy.DEFAULT
                        ? "alter table \"app\".\"accounts\" drop constraint \"uq_accounts\" drop index"
                        : "drop index \"app\".\"uq_accounts\"";
                assertEquals(List.of(drop, create), sql, source + " -> " + target);
            }
        }
    }

    @Test
    void oracleNameOnlyChangesRenameExistingObjectsWithoutDuplicateIndexCreation() {
        for (UniqueNullPolicy policy : UniqueNullPolicy.values()) {
            var actual = unique("old_uq", policy, "code", "tenant");
            var desired = unique("new_uq", policy, "code", "tenant");
            List<String> expected = policy == UniqueNullPolicy.DEFAULT
                    ? List.of("alter table \"app\".\"accounts\" rename constraint \"old_uq\" to \"new_uq\"",
                            "alter index \"app\".\"old_uq\" rename to \"new_uq\"")
                    : List.of("alter index \"app\".\"old_uq\" rename to \"new_uq\"");
            assertEquals(expected, render(RdbDialect.oracle(), SchemaOperation.Kind.CHANGE_UNIQUE,
                    desired.name(), actual, desired));
        }
        var actual = index("old_ix", IndexKeyPart.asc("code"));
        var desired = index("new_ix", IndexKeyPart.asc("code"));
        assertEquals(List.of("alter index \"app\".\"old_ix\" rename to \"new_ix\""),
                render(RdbDialect.oracle(), SchemaOperation.Kind.CHANGE_INDEX, desired.name(), actual, desired));
    }

    @Test
    void oracleSameColumnPolicyChangesReplaceThePhysicalNullSemantics() {
        for (UniqueNullPolicy source : UniqueNullPolicy.values()) {
            UniqueNullPolicy target = source == UniqueNullPolicy.DEFAULT
                    ? UniqueNullPolicy.DISTINCT : UniqueNullPolicy.DEFAULT;
            var actual = unique("uq_accounts", source, "code", "tenant");
            var desired = unique("uq_accounts", target, "code", "tenant");
            List<String> sql = render(RdbDialect.oracle(), SchemaOperation.Kind.CHANGE_UNIQUE,
                    desired.name(), actual, desired);
            String create = "create unique index \"app\".\"uq_accounts\" on \"app\".\"accounts\" ";
            if (target == UniqueNullPolicy.DEFAULT) {
                assertEquals(List.of("drop index \"app\".\"uq_accounts\"",
                        "alter table \"app\".\"accounts\" add constraint \"uq_accounts\" unique (\"code\", \"tenant\")"), sql);
            } else {
                assertEquals(List.of("alter table \"app\".\"accounts\" drop constraint \"uq_accounts\" drop index",
                        create
                                + "(case when \"code\" is not null and \"tenant\" is not null then \"code\" end, "
                                + "case when \"code\" is not null and \"tenant\" is not null then \"tenant\" end)"), sql);
            }
        }
    }

    @Test
    void oracleSameColumnConstraintAndIndexReorderingUseDirectReplacement() {
        for (UniqueNullPolicy policy : UniqueNullPolicy.values()) {
            var actual = unique("uq_accounts", policy, "code", "tenant");
            var desired = unique("uq_accounts", policy, "tenant", "code");
            List<String> sql = render(RdbDialect.oracle(),
                    SchemaOperation.Kind.CHANGE_UNIQUE, desired.name(), actual, desired);
            assertEquals(2, sql.size());
            assertTrue(sql.getLast().indexOf("\"tenant\"") < sql.getLast().indexOf("\"code\""), sql.toString());
        }
        var actual = index("ux_accounts", IndexKeyPart.asc("code"));
        var desired = index("ux_accounts", IndexKeyPart.desc("code"));
        assertEquals(List.of("drop index \"app\".\"ux_accounts\"",
                        "create unique index \"app\".\"ux_accounts\" on \"app\".\"accounts\" (\"code\" desc)"),
                render(RdbDialect.oracle(), SchemaOperation.Kind.CHANGE_INDEX, desired.name(), actual, desired));
    }

    @Test
    void h2ReplacementEnforcesTargetKeysAndLeavesOnlyTheDeclaredIndex() throws Exception {
        var actual = index("ux_accounts", IndexKeyPart.asc("code"));
        var desired = index("ux_accounts", IndexKeyPart.asc("code"), IndexKeyPart.asc("tenant"));
        List<String> sql = render(RdbDialect.h2(), SchemaOperation.Kind.CHANGE_INDEX,
                desired.name(), actual, desired);
        try (var connection = DriverManager.getConnection("jdbc:h2:mem:" + UUID.randomUUID());
             var statement = connection.createStatement()) {
            statement.execute("create schema app");
            statement.execute("create table app.accounts(code int not null, tenant int not null)");
            statement.execute("create unique index app.ux_accounts on app.accounts(code)");
            statement.execute("insert into app.accounts values(1, 1)");
            for (String ddl : sql) {
                statement.execute(ddl);
            }
            SQLException violation = assertThrows(SQLException.class,
                    () -> statement.execute("insert into app.accounts values(1, 1)"));
            assertEquals("23505", violation.getSQLState());
            assertEquals(1, statement.executeUpdate("insert into app.accounts values(1, 2)"));
            try (var indexes = statement.executeQuery("select index_name from information_schema.indexes "
                    + "where table_schema = 'APP' and table_name = 'ACCOUNTS'")) {
                assertTrue(indexes.next());
                assertEquals("UX_ACCOUNTS", indexes.getString(1));
                assertFalse(indexes.next());
            }
        }
    }

    private static List<String> render(RdbDialect dialect, SchemaOperation.Kind kind,
                                      String name, Object actual, Object desired) {
        SchemaOperation operation = SchemaOperation.of(kind, TABLE, name, actual, desired,
                SchemaOperation.Compatibility.REQUIRES_REVIEW);
        return RelationalSchemaSqlRenderer.create(dialect.schema()).render(operation)
                .stream().map(request -> request.sql()).toList();
    }

    private static UniqueConstraintDefinition unique(String name, UniqueNullPolicy policy, String... columns) {
        return new UniqueConstraintDefinition(name, List.of(columns), policy);
    }

    private static IndexDefinition index(String name, IndexKeyPart... keys) {
        var builder = IndexDefinition.builder(name).unique();
        for (IndexKeyPart key : keys) builder.addKey(key);
        return builder.build();
    }
}
