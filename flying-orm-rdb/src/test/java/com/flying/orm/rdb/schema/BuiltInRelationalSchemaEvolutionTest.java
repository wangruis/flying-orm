package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BuiltInRelationalSchemaEvolutionTest {

    private static final RelationIdentity TABLE = RelationIdentity.of(null, "app", "accounts");
    private static final ColumnDefinition ID = ColumnDefinition.builder("id", "BIGINT").nullable(false).build();

    @Test
    void mysqlIdentityUsesTheDeclaredIndexWithoutRequiringAPrimaryKey() {
        ColumnDefinition generated = ColumnDefinition.builder("id", "BIGINT")
                .nullable(false).generation(ValueGeneration.identity()).build();
        for (boolean unique : List.of(false, true)) {
            RelationalTableDefinition table = RelationalTableDefinition.builder(TABLE)
                    .addColumn(generated).addColumn(code(32))
                    .addIndex(IndexDefinition.builder("ix_identity").unique(unique)
                            .addKey(IndexKeyPart.asc("id")).addKey(IndexKeyPart.desc("code")).build())
                    .build();
            List<String> statements = RelationalSchemaSqlRenderer.create(RdbDialect.mysql().schema())
                    .render(SchemaOperation.of(SchemaOperation.Kind.CREATE_TABLE, TABLE, TABLE.table(),
                            null, table, SchemaOperation.Compatibility.SAFE_INCREMENTAL))
                    .stream().map(com.flying.orm.core.sql.render.SqlRequest::sql).toList();
            assertEquals(1, statements.size());
            assertTrue(statements.getFirst().contains((unique ? "unique " : "")
                    + "index `ix_identity` (`id` asc, `code` desc)"), statements.toString());
        }
        RelationalTableDefinition table = RelationalTableDefinition.builder(TABLE)
                .addColumn(generated).addColumn(code(32))
                .addUnique(UniqueConstraintDefinition.of("uq_identity", "id")).build();
        assertEquals(1, RelationalSchemaSqlRenderer.create(RdbDialect.mysql().schema())
                .render(SchemaOperation.of(SchemaOperation.Kind.CREATE_TABLE, TABLE, TABLE.table(),
                        null, table, SchemaOperation.Compatibility.SAFE_INCREMENTAL)).size());
        RelationalTableDefinition indexed = RelationalTableDefinition.builder(TABLE)
                .addColumn(generated).addColumn(code(32).toBuilder().nullable(false).build())
                .primaryKey(PrimaryKeyDefinition.of("pk_code", "code", "id"))
                .addIndex(IndexDefinition.builder("ix_identity").addColumn("id").build()).build();
        List<String> statements = RelationalSchemaSqlRenderer.create(RdbDialect.mysql().schema())
                .render(SchemaOperation.of(SchemaOperation.Kind.CREATE_TABLE, TABLE, TABLE.table(),
                        null, indexed, SchemaOperation.Compatibility.SAFE_INCREMENTAL))
                .stream().map(com.flying.orm.core.sql.render.SqlRequest::sql).toList();
        assertEquals(1, statements.size());
        assertTrue(statements.getFirst().contains("index `ix_identity` (`id` asc)"));
    }

    @Test
    void reviewsExplicitTableRemovalForEveryBuiltInDialect() {
        for (RdbDialect dialect : dialects()) {
            SchemaMigrationPlan plan = RelationalSchemaPlanReviewer.create(dialect).reviewAbsent(
                    database(dialect), TABLE, SchemaSnapshot.present(table(code(32))),
                    SchemaSnapshotCoverage.complete());

            assertEquals(List.of("drop table " + dialect.schema().relationIdentifier(TABLE)), sql(plan));
            assertEquals(SchemaMigrationPlan.TargetState.ABSENT, plan.targetState().orElseThrow());
        }
    }

    @Test
    void reviewsDependencyDropsAndColumnRemovalWithDialectSpecificConstraintSyntax() {
        ColumnDefinition legacy = ColumnDefinition.builder("legacy", "BIGINT").build();
        RelationalTableDefinition actual = RelationalTableDefinition.builder(TABLE)
                .addColumn(ID).addColumn(code(32)).addColumn(legacy)
                .primaryKey(PrimaryKeyDefinition.of("pk_accounts", "id"))
                .addUnique(UniqueConstraintDefinition.of("uq_accounts_code", "code"))
                .addIndex(index("legacy", false))
                .addCheck(CheckConstraintDefinition.of("ck_accounts", CheckPredicate.isNotNull("legacy")))
                .addForeignKey(ForeignKeyDefinition.builder("fk_accounts")
                        .addColumn("legacy").reference(RelationIdentity.of(null, "app", "groups"))
                        .addReferenceColumn("id").build()).build();
        for (RdbDialect dialect : dialects()) {
            SchemaMigrationPlan plan = review(dialect, actual, table(code(32)));
            SchemaDialect schema = dialect.schema();
            String alter = "alter table " + schema.relationIdentifier(TABLE);
            boolean mysql = dialect.name().equals("mysql");

            assertEquals(List.of(
                    alter + (mysql ? " drop foreign key " : " drop constraint ") + schema.identifier("fk_accounts"),
                    alter + (mysql ? " drop check " : " drop constraint ") + schema.identifier("ck_accounts"),
                    schema.dropIndexSql(TABLE, "ix_accounts"),
                    alter + " drop column " + schema.identifier("legacy"),
                    alter + (mysql ? " drop index " : " drop constraint ") + schema.identifier("uq_accounts_code")
                            + (dialect.name().equals("oracle") ? " drop index" : ""),
                    alter + (mysql ? " drop primary key" : " drop constraint " + schema.identifier("pk_accounts"))),
                    sql(plan), dialect.name());
        }
    }

    @Test
    void reviewsIndexReplacementUsingNativeReplacementOrDirectRebuild() {
        for (RdbDialect dialect : dialects()) {
            SchemaMigrationPlan plan = review(dialect,
                    indexedTable(index("code", false)), indexedTable(index("id", false)));
            assertEquals(2, plan.requests().size());
            assertEquals(List.of(SchemaOperation.Kind.CHANGE_INDEX),
                    plan.operations().stream().map(SchemaOperation::kind).toList());
            assertEquals(plan.requests(), plan.steps().getFirst().requests());
            SchemaMigrationPlan unique = review(dialect, indexedTable(index("code", true)),
                    indexedTable(index("id", true)));
            assertEquals(dialect.name().equals("mysql") || dialect.name().equals("sqlserver") ? 1 : 2,
                    unique.requests().size());
        }
    }

    @Test
    void widensVariableLengthTextWithoutDroppingOtherColumnFacts() {
        for (RdbDialect dialect : dialects()) {
            SchemaMigrationPlan plan = review(dialect, table(code(32)), table(code(64)));

            String expected = switch (dialect.name()) {
                case "mysql" -> "alter table `app`.`accounts` modify column `code` VARCHAR(64)"
                        + " not null default 'active' comment 'business code'";
                case "oracle" -> "alter table \"app\".\"accounts\" modify (\"code\" VARCHAR2(64))";
                case "sqlserver" -> "alter table [app].[accounts] alter column [code] NVARCHAR(64) not null";
                case "h2" -> "alter table app.accounts alter column code set data type VARCHAR(64)";
                default -> throw new AssertionError(dialect.name());
            };
            assertEquals(List.of(expected), sql(plan), dialect.name());
        }
    }

    @Test
    void changesNullabilityWithoutResettingTypeDefaultOrComment() {
        ColumnDefinition desired = ColumnDefinition.builder("code", "VARCHAR").length(32)
                .defaultValue(ColumnDefault.literal("active")).comment("business code").build();
        for (RdbDialect dialect : dialects()) {
            SchemaMigrationPlan plan = review(dialect, table(code(32)), table(desired));
            String expected = switch (dialect.name()) {
                case "mysql" -> "alter table `app`.`accounts` modify column `code` VARCHAR(32)"
                        + " default 'active' comment 'business code'";
                case "oracle" -> "alter table \"app\".\"accounts\" modify (\"code\" null)";
                case "sqlserver" -> "alter table [app].[accounts] alter column [code] NVARCHAR(32) null";
                case "h2" -> "alter table app.accounts alter column code drop not null";
                default -> throw new AssertionError(dialect.name());
            };
            assertEquals(List.of(expected), sql(plan), dialect.name());
        }
    }

    @Test
    void widensAndChangesNullabilityWithoutDroppingOtherColumnFacts() {
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            for (boolean nullable : List.of(true, false)) {
                ColumnDefinition actual = ColumnDefinition.builder("code", "VARCHAR").length(32)
                        .nullable(!nullable).defaultValue(ColumnDefault.literal("active"))
                        .comment("business code").build();
                ColumnDefinition desired = ColumnDefinition.builder("code", "VARCHAR").length(64)
                        .nullable(nullable).defaultValue(ColumnDefault.literal("active"))
                        .comment("business code").build();
                SchemaMigrationPlan plan = review(dialect, table(actual), table(desired));
                List<String> expected = switch (dialect.name()) {
                    case "mysql" -> List.of("alter table `app`.`accounts` modify column `code` VARCHAR(64)"
                            + (nullable ? "" : " not null") + " default 'active' comment 'business code'");
                    case "sqlserver" -> List.of("alter table [app].[accounts] alter column [code] NVARCHAR(64)"
                            + (nullable ? " null" : " not null"));
                    case "oracle" -> List.of("alter table \"app\".\"accounts\" modify (\"code\" VARCHAR2(64))",
                            "alter table \"app\".\"accounts\" modify (\"code\" " + (nullable ? "null)" : "not null)"));
                    case "postgresql" -> List.of("alter table \"app\".\"accounts\" alter column \"code\" type VARCHAR(64)",
                            "alter table \"app\".\"accounts\" alter column \"code\" " + (nullable ? "drop" : "set") + " not null");
                    default -> throw new AssertionError(dialect.name());
                };
                assertEquals(expected, sql(plan), dialect.name());
            }
        }
    }

    @Test
    void preservesObservedStorageOptionsWhenTheTargetDoesNotOverrideThem() {
        ColumnDefinition mysqlActual = ColumnDefinition.builder("code", "VARCHAR").length(32).nullable(false)
                .defaultValue(ColumnDefault.literal("active")).comment("business code")
                .charset("utf8mb4").collation("utf8mb4_bin").build();
        SchemaMigrationPlan mysql = review(RdbDialect.mysql(), table(mysqlActual), table(code(64)));
        assertEquals(List.of("alter table `app`.`accounts` modify column `code` VARCHAR(64)"
                + " character set `utf8mb4` collate `utf8mb4_bin` not null default 'active'"
                + " comment 'business code'"), sql(mysql));

        ColumnDefinition sqlServerActual = ColumnDefinition.builder("code", "VARCHAR").length(32).nullable(false)
                .defaultValue(ColumnDefault.literal("active")).comment("business code")
                .collation("Latin1_General_100_BIN2").build();
        SchemaMigrationPlan sqlServer = review(RdbDialect.sqlServer(), table(sqlServerActual), table(code(64)));
        // COLLATE 使用已校验的排序规则名称，不按表名或列名添加方括号。
        assertEquals(List.of("alter table [app].[accounts] alter column [code] NVARCHAR(64)"
                + " collate Latin1_General_100_BIN2 not null"), sql(sqlServer));

        ColumnDefinition nullable = ColumnDefinition.builder("code", "VARCHAR").length(64)
                .defaultValue(ColumnDefault.literal("active")).comment("business code").build();
        assertEquals(List.of("alter table `app`.`accounts` modify column `code` VARCHAR(64)"
                + " character set `utf8mb4` collate `utf8mb4_bin` default 'active' comment 'business code'"),
                sql(review(RdbDialect.mysql(), table(mysqlActual), table(nullable))));
        assertEquals(List.of("alter table [app].[accounts] alter column [code] NVARCHAR(64)"
                + " collate Latin1_General_100_BIN2 null"),
                sql(review(RdbDialect.sqlServer(), table(sqlServerActual), table(nullable))));
    }

    @Test
    void removesDefaultsOnlyWhenTheirCompleteDdlIdentityIsKnown() {
        ColumnDefinition desired = ColumnDefinition.builder("code", "VARCHAR").length(32)
                .nullable(false).comment("business code").build();
        for (RdbDialect dialect : dialects()) {
            if (dialect.name().equals("sqlserver")) {
                assertThrows(UnsupportedOperationException.class, () -> review(dialect, table(code(32)), table(desired)));
                continue;
            }
            SchemaMigrationPlan plan = review(dialect, table(code(32)), table(desired));
            String expected = dialect.name().equals("mysql")
                    ? "alter table `app`.`accounts` alter column `code` drop default"
                    : dialect.name().equals("oracle")
                    ? "alter table \"app\".\"accounts\" modify (\"code\" default null)"
                    : "alter table app.accounts alter column code drop default";
            assertEquals(List.of(expected), sql(plan), dialect.name());
        }
    }

    @Test
    void changesCommentsUsingEachDialectsExistingCommentContract() {
        ColumnDefinition desired = ColumnDefinition.builder("code", "VARCHAR").length(32).nullable(false)
                .defaultValue(ColumnDefault.literal("active")).comment("changed").build();
        for (RdbDialect dialect : dialects()) {
            SchemaMigrationPlan plan = review(dialect, table(code(32)), table(desired));
            String expected = dialect.name().equals("mysql")
                    ? "alter table `app`.`accounts` modify column `code` VARCHAR(32)"
                            + " not null default 'active' comment 'changed'"
                    : dialect.schema().columnCommentChangeSql(TABLE, "code", "business code", "changed")
                            .orElseThrow();
            assertEquals(List.of(expected), sql(plan), dialect.name());
        }
    }

    @Test
    void rejectsUnsupportedColumnsAndKeepsSupportedCandidateChanges() {
        for (RdbDialect dialect : dialects()) {
            assertThrows(UnsupportedOperationException.class, () -> review(dialect, table(code(64)), table(code(32))));
            ColumnDefinition generated = ColumnDefinition.builder("id", "BIGINT").nullable(false)
                    .generation(ValueGeneration.identity()).build();
            assertThrows(UnsupportedOperationException.class, () -> review(dialect, table(code(32)),
                    RelationalTableDefinition.builder(TABLE).addColumn(generated).addColumn(code(32)).build()));
            RelationalTableDefinition actual = RelationalTableDefinition.builder(TABLE)
                    .addColumn(ID).addColumn(code(32))
                    .primaryKey(PrimaryKeyDefinition.of("pk_accounts", "id"))
                    .addUnique(UniqueConstraintDefinition.of("uq_accounts", "code")).build();
            RelationalTableDefinition desired = RelationalTableDefinition.builder(TABLE)
                    .addColumn(ID).addColumn(code(32))
                    .primaryKey(PrimaryKeyDefinition.of("pk_accounts", "id", "code"))
                    .addUnique(UniqueConstraintDefinition.of("uq_accounts", "code", "id")).build();
            SchemaMigrationPlan plan = review(dialect, actual, desired);
            assertEquals(dialect.name().equals("mysql") ? 2 : 4,
                    plan.requests().size(), dialect.name());
        }
    }

    @Test
    void combinesTypeNullabilityDefaultAndCommentChanges() {
        ColumnDefinition actual = code(32).toBuilder().defaultConstraintName("df_code").build();
        ColumnDefinition desired = ColumnDefinition.builder("code", "VARCHAR").length(64)
                .defaultValue(ColumnDefault.literal("inactive")).comment("changed").build();
        for (RdbDialect dialect : List.of(RdbDialect.postgresql(), RdbDialect.mysql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            SchemaMigrationPlan plan = review(dialect, table(actual), table(desired));
            if (dialect.name().equals("mysql")) {
                assertEquals(List.of("alter table `app`.`accounts` modify column `code` VARCHAR(64)"
                        + " default 'inactive' comment 'changed'"), sql(plan));
            } else {
                assertTrue(sql(plan).stream().anyMatch(sql -> sql.contains("inactive")), dialect.name());
                assertTrue(sql(plan).stream().anyMatch(sql -> sql.contains("changed")), dialect.name());
                assertEquals(4, plan.requests().size(), dialect.name());
            }
        }
    }

    @Test
    void rejectsSqlServerDefaultedColumnRemovalWithoutGuessingItsConstraintName() {
        assertThrows(UnsupportedOperationException.class, () -> review(RdbDialect.sqlServer(), table(code(32)),
                RelationalTableDefinition.builder(TABLE).addColumn(ID).build()));
    }

    @Test
    void removesAnObservedDefaultAndItsColumnInOneStatement() {
        ColumnDefinition column = ColumnDefinition.builder("code", "VARCHAR").length(32)
                .defaultValue(ColumnDefault.literal("active")).defaultConstraintName("DF_accounts_code").build();
        SchemaMigrationPlan plan = review(RdbDialect.sqlServer(), table(column),
                RelationalTableDefinition.builder(TABLE).addColumn(ID).build());

        assertEquals(List.of("alter table [app].[accounts] drop constraint [DF_accounts_code], column [code]"),
                sql(plan));
    }

    @Test
    void preservesExplicitDefaultIdentityWhenCreatingAndAddingADefault() {
        RdbDialect dialect = RdbDialect.sqlServer();
        ColumnDefinition plain = ColumnDefinition.builder("code", "VARCHAR").length(32).build();
        ColumnDefinition named = ColumnDefinition.builder("code", "VARCHAR").length(32)
                .defaultValue(ColumnDefault.literal("active")).defaultConstraintName("DF_accounts_code").build();
        SchemaMigrationPlan plan = review(dialect, table(plain), table(named));

        assertEquals(List.of("alter table [app].[accounts] add constraint [DF_accounts_code] default N'active' for [code]"),
                sql(plan));
        RelationalSchemaSqlRenderer renderer = RelationalSchemaSqlRenderer.create(dialect.schema());
        assertEquals("[code] NVARCHAR(32) null constraint [DF_accounts_code] default N'active'",
                renderer.columnDefinition(named));
        assertEquals("[code] NVARCHAR(32) null constraint [DF_accounts_code] default null",
                renderer.columnDefinition(ColumnDefinition.builder("code", "VARCHAR").length(32)
                        .defaultConstraintName("DF_accounts_code").build()));
        assertEquals("[id] BIGINT constraint [DF_accounts_id] default next value for [app].[account_seq] not null",
                renderer.columnDefinition(ColumnDefinition.builder("id", "BIGINT").nullable(false)
                        .generation(ValueGeneration.sequence("app.account_seq"))
                        .defaultConstraintName("DF_accounts_id").build()));
    }

    @Test
    void replacesObservedDefaultsWithoutLosingTheirConstraintIdentity() {
        ColumnDefinition actual = ColumnDefinition.builder("code", "VARCHAR").length(32)
                .defaultValue(ColumnDefault.literal("active")).defaultConstraintName("DF_old").build();
        for (String name : List.of("", "DF_old", "DF_new")) {
            ColumnDefinition desired = ColumnDefinition.builder("code", "VARCHAR").length(32)
                    .defaultValue(ColumnDefault.literal("停用'值"))
                    .defaultConstraintName(name.isEmpty() ? null : name).build();
            SchemaMigrationPlan plan = review(RdbDialect.sqlServer(), table(actual), table(desired));
            assertEquals(List.of("alter table [app].[accounts] drop constraint [DF_old]",
                    "alter table [app].[accounts] add constraint [" + (name.isEmpty() ? "DF_old" : name)
                            + "] default N'停用''值' for [code]"), sql(plan));
        }
    }

    @Test
    void replacesWithAnExplicitNullDefaultAndRenamesAnUnchangedDefault() {
        ColumnDefinition actual = ColumnDefinition.builder("code", "VARCHAR").length(32)
                .defaultValue(ColumnDefault.literal("active")).defaultConstraintName("DF_old").build();
        for (ColumnDefault value : List.of(ColumnDefault.none(), ColumnDefault.literal("active"))) {
            ColumnDefinition desired = ColumnDefinition.builder("code", "VARCHAR").length(32)
                    .defaultValue(value).defaultConstraintName("DF_new").build();
            SchemaMigrationPlan plan = review(RdbDialect.sqlServer(), table(actual), table(desired));
            assertEquals(List.of("alter table [app].[accounts] drop constraint [DF_old]",
                    "alter table [app].[accounts] add constraint [DF_new] default "
                            + (value.kind() == ColumnDefault.Kind.NONE ? "null" : "N'active'") + " for [code]"), sql(plan));
        }
    }

    @Test
    void rejectsReplacementWhenTheObservedDefaultNameIsMissing() {
        ColumnDefinition actual = ColumnDefinition.builder("code", "VARCHAR").length(32)
                .defaultValue(ColumnDefault.literal("active")).build();
        ColumnDefinition desired = ColumnDefinition.builder("code", "VARCHAR").length(32)
                .defaultValue(ColumnDefault.literal("inactive")).defaultConstraintName("DF_new").build();
        assertThrows(UnsupportedOperationException.class, () -> review(RdbDialect.sqlServer(), table(actual), table(desired)));
    }

    @Test
    void neverTreatsRenamingASequenceDefaultAsRemovingItsGeneration() {
        ColumnDefinition actual = ColumnDefinition.builder("code", "BIGINT")
                .generation(ValueGeneration.sequence("account_seq")).defaultConstraintName("DF_old").build();
        ColumnDefinition desired = ColumnDefinition.builder("code", "BIGINT")
                .generation(ValueGeneration.sequence("account_seq")).defaultConstraintName("DF_new").build();
        assertThrows(UnsupportedOperationException.class, () -> review(RdbDialect.sqlServer(), table(actual), table(desired)),
                "a name change cannot remove NEXT VALUE FOR generation");
    }

    private static List<RdbDialect> dialects() {
        return List.of(RdbDialect.mysql(), RdbDialect.h2(), RdbDialect.oracle(), RdbDialect.sqlServer());
    }

    private static DatabaseDescriptor database(RdbDialect dialect) {
        return DatabaseDescriptor.of(dialect.name(), "test", dialect);
    }

    private static SchemaMigrationPlan review(RdbDialect dialect,
                                             RelationalTableDefinition actual,
                                             RelationalTableDefinition desired) {
        return RelationalSchemaPlanReviewer.create(dialect).review(database(dialect), desired,
                SchemaSnapshot.present(actual), SchemaSnapshotCoverage.complete(), SchemaCompatibilityMode.EXACT);
    }

    private static List<String> sql(SchemaMigrationPlan plan) {
        return plan.requests().stream().map(request -> request.sql()).toList();
    }

    private static ColumnDefinition code(int length) {
        return ColumnDefinition.builder("code", "VARCHAR").length(length).nullable(false)
                .defaultValue(ColumnDefault.literal("active")).comment("business code").build();
    }

    private static RelationalTableDefinition table(ColumnDefinition code) {
        return RelationalTableDefinition.builder(TABLE).addColumn(ID).addColumn(code).build();
    }

    private static IndexDefinition index(String column, boolean unique) {
        IndexDefinition.Builder index = IndexDefinition.builder("ix_accounts").addKey(IndexKeyPart.asc(column));
        return (unique ? index.unique() : index).build();
    }

    private static RelationalTableDefinition indexedTable(IndexDefinition index) {
        return RelationalTableDefinition.builder(TABLE).addColumn(ID).addColumn(code(32)).addIndex(index).build();
    }
}
