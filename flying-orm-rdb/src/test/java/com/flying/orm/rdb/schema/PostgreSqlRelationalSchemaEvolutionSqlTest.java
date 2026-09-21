package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.ReferentialAction;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PostgreSqlRelationalSchemaEvolutionSqlTest {

    private static final RelationIdentity ACCOUNTS = RelationIdentity.of(null, "app", "accounts");
    private static final RelationIdentity GROUPS = RelationIdentity.of(null, "app", "groups");
    private static final ColumnDefinition ID = column("id", "BIGINT", null, false);
    private static final ColumnDefinition CODE_32 = column("code", "VARCHAR", 32, false);
    private static final ColumnDefinition CODE_64 = column("code", "VARCHAR", 64, false);

    @Test
    void rendersEveryPostgreSqlDropWithoutConditionalOrCascadeClauses() {
        ForeignKeyDefinition foreignKey = foreignKey(ReferentialAction.NO_ACTION);
        CheckConstraintDefinition check = CheckConstraintDefinition.of(
                "ck_accounts_code", CheckPredicate.isNotNull("code"));
        IndexDefinition index = index(IndexKeyPart.asc("code"));
        UniqueConstraintDefinition unique = UniqueConstraintDefinition.of("uq_accounts_code", "code");
        PrimaryKeyDefinition primaryKey = PrimaryKeyDefinition.of("pk_accounts", "id");
        RelationalTableDefinition table = table(CODE_32, primaryKey, unique, index, check, foreignKey);
        RelationalSchemaSqlRenderer renderer = renderer();

        List<String> sql = List.of(
                renderer.render(drop(SchemaOperation.Kind.DROP_FOREIGN_KEY, foreignKey.name(), foreignKey))
                        .getFirst().sql(),
                renderer.render(drop(SchemaOperation.Kind.DROP_CHECK, check.name(), check)).getFirst().sql(),
                renderer.render(drop(SchemaOperation.Kind.DROP_INDEX, index.name(), index)).getFirst().sql(),
                renderer.render(drop(SchemaOperation.Kind.DROP_UNIQUE, unique.name(), unique)).getFirst().sql(),
                renderer.render(drop(SchemaOperation.Kind.DROP_PRIMARY_KEY, primaryKey.name(), primaryKey))
                        .getFirst().sql(),
                renderer.render(drop(SchemaOperation.Kind.DROP_COLUMN, CODE_32.name(), CODE_32)).getFirst().sql(),
                renderer.render(drop(SchemaOperation.Kind.DROP_TABLE, ACCOUNTS.table(), table)).getFirst().sql());

        assertEquals(List.of(
                "alter table \"app\".\"accounts\" drop constraint \"fk_accounts_group\"",
                "alter table \"app\".\"accounts\" drop constraint \"ck_accounts_code\"",
                "drop index \"app\".\"ix_accounts_code\"",
                "alter table \"app\".\"accounts\" drop constraint \"uq_accounts_code\"",
                "alter table \"app\".\"accounts\" drop constraint \"pk_accounts\"",
                "alter table \"app\".\"accounts\" drop column \"code\"",
                "drop table \"app\".\"accounts\""), sql);
        assertTrue(sql.stream().noneMatch(value -> value.contains("if exists") || value.contains("cascade")));
    }

    @Test
    void rendersChangedObjectsAsExplicitDropThenAddRequests() {
        RelationalSchemaSqlRenderer renderer = renderer();

        assertEquals(List.of(
                "alter table \"app\".\"accounts\" drop constraint \"pk_accounts\","
                        + " add constraint \"pk_accounts\" primary key (\"code\", \"id\")"),
                sql(renderer.render(change(
                        SchemaOperation.Kind.CHANGE_PRIMARY_KEY,
                        "pk_accounts",
                        PrimaryKeyDefinition.of("pk_accounts", "id"),
                        PrimaryKeyDefinition.of("pk_accounts", "code", "id")))));
        assertEquals(List.of(
                "alter table \"app\".\"accounts\" drop constraint \"uq_accounts_code\","
                        + " add constraint \"uq_accounts_code\" unique (\"code\", \"id\")"),
                sql(renderer.render(change(
                        SchemaOperation.Kind.CHANGE_UNIQUE,
                        "uq_accounts_code",
                        UniqueConstraintDefinition.of("uq_accounts_code", "code"),
                        UniqueConstraintDefinition.of("uq_accounts_code", "code", "id")))));
        assertEquals(List.of(
                "drop index \"app\".\"ix_accounts_code\"",
                "create index \"ix_accounts_code\" on \"app\".\"accounts\" (\"code\" desc)"),
                sql(renderer.render(change(
                        SchemaOperation.Kind.CHANGE_INDEX,
                        "ix_accounts_code",
                        index(IndexKeyPart.asc("code")),
                        index(IndexKeyPart.desc("code"))))));

        CheckConstraintDefinition actualCheck = CheckConstraintDefinition.of(
                "ck_accounts_code", CheckPredicate.isNotNull("code"));
        CheckConstraintDefinition desiredCheck = CheckConstraintDefinition.of(
                "ck_accounts_code",
                CheckPredicate.compare("code", CheckPredicate.ComparisonOperator.NOT_EQUAL, ""));
        assertEquals(List.of(
                "alter table \"app\".\"accounts\" drop constraint \"ck_accounts_code\","
                        + " add constraint \"ck_accounts_code\" check (\"code\" <> E'')"),
                sql(renderer.render(change(
                        SchemaOperation.Kind.CHANGE_CHECK,
                        actualCheck.name(), actualCheck, desiredCheck))));

        ForeignKeyDefinition actualForeignKey = foreignKey(ReferentialAction.NO_ACTION);
        ForeignKeyDefinition desiredForeignKey = foreignKey(ReferentialAction.CASCADE);
        assertEquals(List.of(
                "alter table \"app\".\"accounts\" drop constraint \"fk_accounts_group\","
                        + " add constraint \"fk_accounts_group\" foreign key (\"code\")"
                        + " references \"app\".\"groups\" (\"code\") on delete cascade"),
                sql(renderer.render(change(
                        SchemaOperation.Kind.CHANGE_FOREIGN_KEY,
                        actualForeignKey.name(), actualForeignKey, desiredForeignKey))));
    }

    @Test
    void stagesRemovedDependenciesBeforeColumnsAndNewDependenciesAfterColumns() {
        ColumnDefinition legacy = column("legacy", "VARCHAR", 32, true);
        ColumnDefinition code = column("code", "VARCHAR", 64, true);
        RelationalTableDefinition actual = RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(ID)
                .addColumn(legacy)
                .addUnique(UniqueConstraintDefinition.of("uq_accounts_legacy", "legacy"))
                .addIndex(index("ix_accounts_legacy", IndexKeyPart.asc("legacy")))
                .addCheck(CheckConstraintDefinition.of(
                        "ck_accounts_legacy", CheckPredicate.isNotNull("legacy")))
                .addForeignKey(foreignKey(
                        "fk_accounts_legacy", "legacy", ReferentialAction.NO_ACTION))
                .build();
        RelationalTableDefinition desired = RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(ID)
                .addColumn(code)
                .addUnique(UniqueConstraintDefinition.of("uq_accounts_code", "code"))
                .addIndex(index("ix_accounts_code", IndexKeyPart.asc("code")))
                .addCheck(CheckConstraintDefinition.of(
                        "ck_accounts_code", CheckPredicate.isNotNull("code")))
                .addForeignKey(foreignKey(
                        "fk_accounts_code", "code", ReferentialAction.NO_ACTION))
                .build();
        RdbDialect dialect = RdbDialect.postgresql();

        ReviewedSchemaPlan plan = RelationalSchemaPlanReviewer.create(dialect).review(
                DatabaseDescriptor.of("PostgreSQL", "18", dialect),
                desired,
                SchemaSnapshot.present(actual),
                SchemaSnapshotCoverage.complete(),
                SchemaCompatibilityMode.EXACT);

        assertFalse(plan.requiresManualAction());
        assertEquals(List.of(
                "alter table \"app\".\"accounts\" drop constraint \"fk_accounts_legacy\"",
                "alter table \"app\".\"accounts\" drop constraint \"ck_accounts_legacy\"",
                "drop index \"app\".\"ix_accounts_legacy\"",
                "alter table \"app\".\"accounts\" drop constraint \"uq_accounts_legacy\"",
                "alter table \"app\".\"accounts\" add column \"code\" VARCHAR(64)",
                "alter table \"app\".\"accounts\" drop column \"legacy\"",
                "alter table \"app\".\"accounts\" add constraint \"uq_accounts_code\" unique (\"code\")",
                "create index \"ix_accounts_code\" on \"app\".\"accounts\" (\"code\" asc)",
                "alter table \"app\".\"accounts\" add constraint \"ck_accounts_code\" check (\"code\" is not null)",
                "alter table \"app\".\"accounts\" add constraint \"fk_accounts_code\" foreign key (\"code\")"
                        + " references \"app\".\"groups\" (\"code\")"),
                plan.requests().stream().map(request -> request.sql()).toList());
    }

    @Test
    void establishesRenamedIntegrityObjectsBeforeDroppingTheirOldProtection() {
        assertEquals(List.of(
                        "alter table \"app\".\"accounts\" add constraint \"uq_accounts_code_new\" unique (\"code\")",
                        "alter table \"app\".\"accounts\" drop constraint \"uq_accounts_code_old\""),
                replacementSql(
                        tableWithUnique("uq_accounts_code_old"),
                        tableWithUnique("uq_accounts_code_new")));
        assertEquals(List.of(
                        "create unique index \"ix_accounts_code_new\" on \"app\".\"accounts\" (\"code\" asc)",
                        "drop index \"app\".\"ix_accounts_code_old\""),
                replacementSql(
                        tableWithIndex("ix_accounts_code_old", true),
                        tableWithIndex("ix_accounts_code_new", true)));
        assertEquals(List.of(
                        "alter table \"app\".\"accounts\" add constraint \"ck_accounts_code_new\""
                                + " check (\"code\" is not null)",
                        "alter table \"app\".\"accounts\" drop constraint \"ck_accounts_code_old\""),
                replacementSql(
                        tableWithCheck("ck_accounts_code_old"),
                        tableWithCheck("ck_accounts_code_new")));
        assertEquals(List.of(
                        "alter table \"app\".\"accounts\" add constraint \"fk_accounts_group_new\""
                                + " foreign key (\"code\") references \"app\".\"groups\" (\"code\")",
                        "alter table \"app\".\"accounts\" drop constraint \"fk_accounts_group_old\""),
                replacementSql(
                        tableWithForeignKey("fk_accounts_group_old"),
                        tableWithForeignKey("fk_accounts_group_new")));
    }

    @Test
    void keepsRenamedProtectionManualWhenItsColumnChangesType() {
        RelationalTableDefinition actual = RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(ID).addColumn(CODE_32)
                .addUnique(UniqueConstraintDefinition.of("uq_accounts_code_old", "code"))
                .build();
        RelationalTableDefinition desired = RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(ID).addColumn(CODE_64)
                .addUnique(UniqueConstraintDefinition.of("uq_accounts_code_new", "code"))
                .build();

        ReviewedSchemaPlan plan = review(actual, desired);

        assertTrue(plan.requiresManualAction());
        assertEquals(List.of("alter table \"app\".\"accounts\" alter column \"code\" type VARCHAR(64)"),
                plan.requests().stream().map(request -> request.sql()).toList());
    }

    @Test
    void executesCandidateAndIndexChangesAfterTheirColumnsWiden() {
        RelationalTableDefinition actual = indexedCandidates(CODE_32, 16, false);
        RelationalTableDefinition desired = indexedCandidates(CODE_64, 32, true);

        ReviewedSchemaPlan plan = review(actual, desired);

        assertFalse(plan.requiresManualAction());
        assertEquals(List.of(
                "alter table \"app\".\"accounts\" alter column \"code\" type VARCHAR(64)",
                "alter table \"app\".\"accounts\" alter column \"state\" type VARCHAR(32)",
                "alter table \"app\".\"accounts\" drop constraint \"pk_accounts\","
                        + " add constraint \"pk_accounts\" primary key (\"id\", \"code\")",
                "alter table \"app\".\"accounts\" drop constraint \"uq_accounts_code\","
                        + " add constraint \"uq_accounts_code\" unique (\"code\", \"id\")",
                "drop index \"app\".\"ix_accounts_state\"",
                "create index \"ix_accounts_state\" on \"app\".\"accounts\" (\"state\" asc, \"id\" asc)"),
                plan.requests().stream().map(request -> request.sql()).toList());
    }

    @Test
    void keepsDependentCandidateChangesManualWhenTheirColumnChangeIsUnsafe() {
        ColumnDefinition mixedChange = ColumnDefinition.builder("code", "VARCHAR")
                .length(64).nullable(false).defaultValue(ColumnDefault.literal("active")).build();
        for (ColumnDefinition desiredCode : List.of(
                column("code", "VARCHAR", 16, false), mixedChange)) {
            ReviewedSchemaPlan plan = review(
                    indexedCandidates(CODE_32, 16, false),
                    indexedCandidates(desiredCode, 16, true));

            assertTrue(plan.requiresManualAction());
            assertEquals(Set.of(SchemaOperation.Kind.CHANGE_COLUMN,
                            SchemaOperation.Kind.CHANGE_PRIMARY_KEY, SchemaOperation.Kind.CHANGE_UNIQUE),
                    plan.steps().stream().filter(step -> !step.executable())
                            .map(step -> step.operation().kind()).collect(java.util.stream.Collectors.toSet()));
        }
    }

    @Test
    void keepsCheckAndForeignKeyChangesManualWhenTheirColumnWidens() {
        RelationalTableDefinition actual = table(
                CODE_32,
                PrimaryKeyDefinition.of("pk_accounts", "code"),
                UniqueConstraintDefinition.of("uq_accounts_code", "code"),
                index(IndexKeyPart.asc("code")),
                CheckConstraintDefinition.of("ck_accounts_code", CheckPredicate.isNotNull("code")),
                foreignKey(ReferentialAction.NO_ACTION));
        RelationalTableDefinition desired = table(
                CODE_64,
                PrimaryKeyDefinition.of("pk_accounts", "code", "id"),
                UniqueConstraintDefinition.of("uq_accounts_code", "code", "id"),
                index(IndexKeyPart.desc("code")),
                CheckConstraintDefinition.of(
                        "ck_accounts_code",
                        CheckPredicate.compare("code", CheckPredicate.ComparisonOperator.NOT_EQUAL, "")),
                foreignKey(ReferentialAction.CASCADE));
        RdbDialect dialect = RdbDialect.postgresql();

        ReviewedSchemaPlan plan = RelationalSchemaPlanReviewer.create(dialect).review(
                DatabaseDescriptor.of("PostgreSQL", "18", dialect),
                desired,
                SchemaSnapshot.present(actual),
                SchemaSnapshotCoverage.complete(),
                SchemaCompatibilityMode.EXACT);

        assertTrue(plan.requiresManualAction());
        assertEquals(Set.of(SchemaOperation.Kind.CHANGE_CHECK, SchemaOperation.Kind.CHANGE_FOREIGN_KEY),
                plan.steps().stream().filter(step -> !step.executable())
                        .map(step -> step.operation().kind()).collect(java.util.stream.Collectors.toSet()));
        assertEquals(List.of(
                "alter table \"app\".\"accounts\" alter column \"code\" type VARCHAR(64)",
                "alter table \"app\".\"accounts\" drop constraint \"pk_accounts\","
                        + " add constraint \"pk_accounts\" primary key (\"code\", \"id\")",
                "alter table \"app\".\"accounts\" drop constraint \"uq_accounts_code\","
                        + " add constraint \"uq_accounts_code\" unique (\"code\", \"id\")",
                "drop index \"app\".\"ix_accounts_code\"",
                "create index \"ix_accounts_code\" on \"app\".\"accounts\" (\"code\" desc)"),
                plan.requests().stream().map(request -> request.sql()).toList());
    }

    @Test
    void treatsSelfReferencingForeignKeyTargetColumnsAsDependencies() {
        ColumnDefinition parentCode = column("parent_code", "VARCHAR", 32, true);
        ForeignKeyDefinition actualForeignKey = ForeignKeyDefinition.builder("fk_accounts_parent")
                .addColumn("parent_code")
                .reference(ACCOUNTS)
                .addReferenceColumn("code")
                .build();
        ForeignKeyDefinition desiredForeignKey = ForeignKeyDefinition.builder("fk_accounts_parent")
                .addColumn("parent_code")
                .reference(ACCOUNTS)
                .addReferenceColumn("code")
                .onDelete(ReferentialAction.CASCADE)
                .build();
        RelationalTableDefinition actual = RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(ID).addColumn(CODE_32).addColumn(parentCode)
                .addForeignKey(actualForeignKey)
                .build();
        RelationalTableDefinition desired = RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(ID).addColumn(CODE_64).addColumn(parentCode)
                .addForeignKey(desiredForeignKey)
                .build();
        RdbDialect dialect = RdbDialect.postgresql();

        ReviewedSchemaPlan plan = RelationalSchemaPlanReviewer.create(dialect).review(
                DatabaseDescriptor.of("PostgreSQL", "18", dialect),
                desired,
                SchemaSnapshot.present(actual),
                SchemaSnapshotCoverage.complete(),
                SchemaCompatibilityMode.EXACT);

        assertTrue(plan.requiresManualAction());
        assertTrue(plan.requests().isEmpty());
    }

    @Test
    void keepsCandidateKeyChangesManualWhenAnObservedSelfReferenceDependsOnThem() {
        ColumnDefinition tenant = column("tenant", "VARCHAR", 32, false);
        ColumnDefinition parentCode = column("parent_code", "VARCHAR", 32, true);
        ForeignKeyDefinition selfReference = ForeignKeyDefinition.builder("fk_accounts_parent")
                .addColumn("parent_code")
                .reference(ACCOUNTS)
                .addReferenceColumn("code")
                .build();
        RelationalTableDefinition actual = RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(CODE_32).addColumn(parentCode)
                .primaryKey(PrimaryKeyDefinition.of("pk_accounts", "code"))
                .addForeignKey(selfReference)
                .build();
        RelationalTableDefinition desired = RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(CODE_32).addColumn(tenant).addColumn(parentCode)
                .primaryKey(PrimaryKeyDefinition.of("pk_accounts", "code", "tenant"))
                .addForeignKey(selfReference)
                .build();

        ReviewedSchemaPlan plan = review(actual, desired);

        assertTrue(plan.requiresManualAction());
        assertEquals(List.of("alter table \"app\".\"accounts\" add column \"tenant\" VARCHAR(32) not null"),
                plan.requests().stream().map(request -> request.sql()).toList());
    }

    @Test
    void keepsUniqueIndexRemovalManualWhenAnObservedSelfReferenceDependsOnIt() {
        ColumnDefinition parentCode = column("parent_code", "VARCHAR", 32, true);
        ForeignKeyDefinition selfReference = ForeignKeyDefinition.builder("fk_accounts_parent")
                .addColumn("parent_code")
                .reference(ACCOUNTS)
                .addReferenceColumn("code")
                .build();
        IndexDefinition candidateKey = IndexDefinition.builder("ux_accounts_code")
                .unique()
                .addKey(IndexKeyPart.asc("code"))
                .build();
        RelationalTableDefinition actual = RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(CODE_32).addColumn(parentCode)
                .addIndex(candidateKey)
                .addForeignKey(selfReference)
                .build();
        RelationalTableDefinition desired = RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(CODE_32).addColumn(parentCode)
                .addColumn(column("label", "VARCHAR", 32, true))
                .addForeignKey(selfReference)
                .build();

        ReviewedSchemaPlan plan = review(actual, desired);

        assertTrue(plan.requiresManualAction());
        assertEquals(List.of("alter table \"app\".\"accounts\" add column \"label\" VARCHAR(32)"),
                plan.requests().stream().map(request -> request.sql()).toList());
    }

    @Test
    void keepsSameColumnCandidateReplacementManualForARetainedSelfReference() {
        ColumnDefinition parentCode = column("parent_code", "VARCHAR", 32, true);
        ForeignKeyDefinition selfReference = ForeignKeyDefinition.builder("fk_accounts_parent")
                .addColumn("parent_code")
                .reference(ACCOUNTS)
                .addReferenceColumn("code")
                .build();
        RelationalTableDefinition actual = RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(CODE_32).addColumn(parentCode)
                .addUnique(UniqueConstraintDefinition.of("uq_accounts_code_old", "code"))
                .addForeignKey(selfReference)
                .build();
        RelationalTableDefinition desired = RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(CODE_32).addColumn(parentCode)
                .addUnique(UniqueConstraintDefinition.of("uq_accounts_code_new", "code"))
                .addForeignKey(selfReference)
                .build();

        ReviewedSchemaPlan plan = review(actual, desired);

        assertTrue(plan.requiresManualAction());
    }

    @Test
    void matchesRetainedSelfReferencesToPermutedCompleteCandidateKeyColumnsOnly() {
        ForeignKeyDefinition selfReference = ForeignKeyDefinition.builder("fk_accounts_parent")
                .addColumn("parent_tenant")
                .addColumn("parent_code")
                .reference(ACCOUNTS)
                .addReferenceColumn("tenant")
                .addReferenceColumn("code")
                .build();
        PrimaryKeyDefinition primary = PrimaryKeyDefinition.of(
                "pk_accounts", "code", "tenant");
        UniqueConstraintDefinition unique = UniqueConstraintDefinition.of(
                "uq_accounts_candidate", "code", "tenant");
        IndexDefinition uniqueIndex = uniqueIndex(
                "ux_accounts_candidate", "code", "tenant");

        List<ReviewedSchemaPlan> dependentChanges = List.of(
                review(candidateTable(primary, null, null, selfReference),
                        candidateTable(PrimaryKeyDefinition.of(
                                "pk_accounts", "code", "note"), null, null, selfReference)),
                review(candidateTable(primary, null, null, selfReference),
                        candidateTable(null, null, null, selfReference)),
                review(candidateTable(null, unique, null, selfReference),
                        candidateTable(null, UniqueConstraintDefinition.of(
                                "uq_accounts_candidate", "code", "note"), null, selfReference)),
                review(candidateTable(null, unique, null, selfReference),
                        candidateTable(null, null, null, selfReference)),
                review(candidateTable(null, null, uniqueIndex, selfReference),
                        candidateTable(null, null, uniqueIndex(
                                "ux_accounts_candidate", "code", "note"), selfReference)),
                review(candidateTable(null, null, uniqueIndex, selfReference),
                        candidateTable(null, null, null, selfReference)));

        dependentChanges.forEach(plan -> assertTrue(plan.requiresManualAction(),
                () -> "permuted retained self-reference dependency was missed: " + plan.steps()));

        UniqueConstraintDefinition exactBackingKey = UniqueConstraintDefinition.of(
                "uq_accounts_reference", "code", "tenant");
        assertFalse(review(
                candidateTable(PrimaryKeyDefinition.of("pk_accounts", "code"),
                        exactBackingKey, null, selfReference),
                candidateTable(PrimaryKeyDefinition.of("pk_accounts", "note"),
                        exactBackingKey, null, selfReference)).requiresManualAction());
        assertFalse(review(
                candidateTable(PrimaryKeyDefinition.of(
                                "pk_accounts", "code", "tenant", "note"),
                        exactBackingKey, null, selfReference),
                candidateTable(PrimaryKeyDefinition.of("pk_accounts", "code", "note"),
                        exactBackingKey, null, selfReference)).requiresManualAction());
    }

    @Test
    void removesAnObservedSelfReferenceBeforeChangingItsCandidateKey() {
        ColumnDefinition tenant = column("tenant", "VARCHAR", 32, false);
        ColumnDefinition parentCode = column("parent_code", "VARCHAR", 32, true);
        ColumnDefinition parentTenant = column("parent_tenant", "VARCHAR", 32, true);
        ForeignKeyDefinition selfReference = ForeignKeyDefinition.builder("fk_accounts_parent")
                .addColumn("parent_code")
                .addColumn("parent_tenant")
                .reference(ACCOUNTS)
                .addReferenceColumn("tenant")
                .addReferenceColumn("code")
                .build();
        RelationalTableDefinition actual = RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(CODE_32).addColumn(tenant).addColumn(parentCode).addColumn(parentTenant)
                .primaryKey(PrimaryKeyDefinition.of("pk_accounts", "code", "tenant"))
                .addForeignKey(selfReference)
                .build();
        RelationalTableDefinition desired = RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(CODE_32).addColumn(tenant).addColumn(parentCode).addColumn(parentTenant)
                .primaryKey(PrimaryKeyDefinition.of("pk_accounts", "tenant", "code"))
                .build();

        ReviewedSchemaPlan plan = review(actual, desired);

        assertFalse(plan.requiresManualAction());
        assertEquals(List.of(
                        "alter table \"app\".\"accounts\" drop constraint \"fk_accounts_parent\"",
                        "alter table \"app\".\"accounts\" drop constraint \"pk_accounts\","
                                + " add constraint \"pk_accounts\" primary key (\"tenant\", \"code\")"),
                plan.requests().stream().map(request -> request.sql()).toList());
    }

    @Test
    void doesNotTreatAnUnrelatedCandidateKeyAsASelfReferenceDependency() {
        ColumnDefinition tenant = column("tenant", "VARCHAR", 32, false);
        ColumnDefinition parentCode = column("parent_code", "VARCHAR", 32, true);
        ForeignKeyDefinition selfReference = ForeignKeyDefinition.builder("fk_accounts_parent")
                .addColumn("parent_code")
                .reference(ACCOUNTS)
                .addReferenceColumn("code")
                .build();
        RelationalTableDefinition actual = RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(CODE_32).addColumn(tenant).addColumn(parentCode)
                .primaryKey(PrimaryKeyDefinition.of("pk_accounts", "tenant"))
                .addUnique(UniqueConstraintDefinition.of("uq_accounts_code", "code"))
                .addForeignKey(selfReference)
                .build();
        RelationalTableDefinition desired = RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(CODE_32).addColumn(tenant).addColumn(parentCode)
                .primaryKey(PrimaryKeyDefinition.of("pk_accounts", "tenant", "code"))
                .addUnique(UniqueConstraintDefinition.of("uq_accounts_code", "code"))
                .addForeignKey(selfReference)
                .build();

        ReviewedSchemaPlan plan = review(actual, desired);

        assertFalse(plan.requiresManualAction());
    }

    @Test
    void rendersOnlyOneSupportedColumnFactAtATime() {
        ColumnDefinition nullableCode = column("code", "VARCHAR", 32, true);
        ColumnDefinition requiredCode = column("code", "VARCHAR", 32, false);
        ColumnDefinition defaultedCode = ColumnDefinition.builder("code", "VARCHAR")
                .length(32).nullable(false).defaultValue(ColumnDefault.literal("active")).build();
        ColumnDefinition commentedCode = ColumnDefinition.builder("code", "VARCHAR")
                .length(32).nullable(false).comment("业务编码").build();
        RelationalSchemaSqlRenderer renderer = renderer();

        assertEquals("alter table \"app\".\"accounts\" alter column \"code\" set not null",
                renderer.render(changeColumn(nullableCode, requiredCode)).getFirst().sql());
        assertEquals("alter table \"app\".\"accounts\" alter column \"code\" set default E'active'",
                renderer.render(changeColumn(requiredCode, defaultedCode)).getFirst().sql());
        assertEquals("alter table \"app\".\"accounts\" alter column \"code\" drop default",
                renderer.render(changeColumn(defaultedCode, requiredCode)).getFirst().sql());
        assertEquals("comment on column \"app\".\"accounts\".\"code\" is E'业务编码'",
                renderer.render(changeColumn(requiredCode, commentedCode)).getFirst().sql());
        assertEquals("comment on column \"app\".\"accounts\".\"code\" is null",
                renderer.render(changeColumn(commentedCode, requiredCode)).getFirst().sql());
    }

    @Test
    void rejectsNarrowingMixedAndGenerationColumnChanges() {
        ColumnDefinition widenedAndRequired = column("code", "VARCHAR", 64, false);
        ColumnDefinition nullableCode = column("code", "VARCHAR", 32, true);
        ColumnDefinition generatedId = ColumnDefinition.builder("id", "BIGINT")
                .nullable(false).generation(ValueGeneration.identity()).build();
        RelationalSchemaSqlRenderer renderer = renderer();

        assertThrows(UnsupportedOperationException.class,
                () -> renderer.render(changeColumn(CODE_64, CODE_32)));
        assertThrows(UnsupportedOperationException.class,
                () -> renderer.render(changeColumn(nullableCode, widenedAndRequired)));
        assertThrows(UnsupportedOperationException.class,
                () -> renderer.render(changeColumn(ID, generatedId)));
    }

    @Test
    void rejectsDestructiveOperationsWhosePublishedNameDoesNotMatchTheirPayload() {
        SchemaOperation mismatchedDrop = drop(
                SchemaOperation.Kind.DROP_INDEX,
                "ix_unrelated",
                index(IndexKeyPart.asc("code")));

        assertThrows(IllegalArgumentException.class, () -> renderer().render(mismatchedDrop));
    }

    @Test
    void transfersUniqueIndexProtectionBeforeRemovingTheOldDefinition() {
        IndexDefinition actual = IndexDefinition.builder("ix_accounts_code")
                .unique().addKey(IndexKeyPart.asc("code")).build();
        IndexDefinition desired = IndexDefinition.builder("ix_accounts_code")
                .unique().addKey(IndexKeyPart.desc("code")).build();

        List<String> requests = sql(renderer().render(change(
                SchemaOperation.Kind.CHANGE_INDEX, actual.name(), actual, desired)));
        assertEquals(3, requests.size());
        assertTrue(requests.getFirst().startsWith("create unique index \"fo_guard_"));
        assertTrue(requests.getFirst().endsWith("on \"app\".\"accounts\" (\"code\" desc)"));
        assertEquals("drop index \"app\".\"ix_accounts_code\"", requests.get(1));
        assertTrue(requests.getLast().endsWith("rename to \"ix_accounts_code\""));
    }

    @Test
    void rejectsDimensionGrowthForTypesWithoutProvenMonotonicSemantics() {
        ColumnDefinition vector3 = column("embedding", "VECTOR", 3, true);
        ColumnDefinition vector4 = column("embedding", "VECTOR", 4, true);

        assertThrows(UnsupportedOperationException.class,
                () -> renderer().render(changeColumn(vector3, vector4)));
    }

    @Test
    void supportsOnlyProvenPostgreSqlWideningFamilies() {
        ColumnDefinition smallInteger = column("value", "SMALLINT", null, true);
        ColumnDefinition integer = column("value", "INTEGER", null, true);
        ColumnDefinition bigInteger = column("value", "BIGINT", null, true);
        ColumnDefinition numeric10 = ColumnDefinition.builder("amount", "NUMERIC")
                .precision(10).scale(2).build();
        ColumnDefinition numeric20 = ColumnDefinition.builder("amount", "NUMERIC")
                .precision(20).scale(4).build();

        assertEquals("alter table \"app\".\"accounts\" alter column \"value\" type INTEGER",
                renderer().render(changeColumn(smallInteger, integer)).getFirst().sql());
        assertEquals("alter table \"app\".\"accounts\" alter column \"value\" type BIGINT",
                renderer().render(changeColumn(integer, bigInteger)).getFirst().sql());
        assertEquals("alter table \"app\".\"accounts\" alter column \"amount\" type NUMERIC(20,4)",
                renderer().render(changeColumn(numeric10, numeric20)).getFirst().sql());

        assertThrows(UnsupportedOperationException.class,
                () -> renderer().render(changeColumn(bigInteger, integer)));
    }

    @Test
    void executesCatalogQualifiedObservedPostgreSqlWideningFamilies() {
        assertExecutableWidening(
                ColumnDefinition.builder("value", "pg_catalog.int2").build(),
                ColumnDefinition.builder("value", "INTEGER").build(),
                "alter table \"app\".\"accounts\" alter column \"value\" type INTEGER");
        assertExecutableWidening(
                ColumnDefinition.builder("value", "pg_catalog.varchar(32)").build(),
                ColumnDefinition.builder("value", "VARCHAR").length(64).build(),
                "alter table \"app\".\"accounts\" alter column \"value\" type VARCHAR(64)");
        assertExecutableWidening(
                ColumnDefinition.builder("value", "pg_catalog.numeric(10,2)").build(),
                ColumnDefinition.builder("value", "NUMERIC").precision(20).scale(4).build(),
                "alter table \"app\".\"accounts\" alter column \"value\" type NUMERIC(20,4)");
    }

    @Test
    void rejectsTypeChangesForGeneratedColumnsEvenWhenTheStrategyIsUnchanged() {
        ColumnDefinition generatedInteger = ColumnDefinition.builder("id", "INTEGER")
                .nullable(false).generation(ValueGeneration.identity()).build();
        ColumnDefinition generatedBigInteger = ColumnDefinition.builder("id", "BIGINT")
                .nullable(false).generation(ValueGeneration.identity()).build();

        assertThrows(UnsupportedOperationException.class,
                () -> renderer().render(changeColumn(generatedInteger, generatedBigInteger)));
    }

    @Test
    void keepsNarrowingManualOutsidePostgreSql() {
        for (RdbDialect dialect : List.of(
                RdbDialect.mysql(), RdbDialect.oracle(), RdbDialect.sqlServer(), RdbDialect.h2())) {
            RelationalTableDefinition actual = simpleTable(CODE_64);
            RelationalTableDefinition desired = simpleTable(CODE_32);
            ReviewedSchemaPlan plan = RelationalSchemaPlanReviewer.create(dialect).review(
                    DatabaseDescriptor.of(dialect.name(), "test", dialect),
                    desired,
                    SchemaSnapshot.present(actual),
                    SchemaSnapshotCoverage.complete(),
                    SchemaCompatibilityMode.EXACT);

            assertTrue(plan.requiresManualAction(), dialect.name());
            assertTrue(plan.requests().isEmpty(), dialect.name());
        }
    }

    @Test
    void acceptsAProvenAbsentTableWithoutUnrelatedMetadataCoverage() {
        RdbDialect dialect = RdbDialect.postgresql();
        ReviewedSchemaPlan plan = RelationalSchemaPlanReviewer.create(dialect).reviewAbsent(
                DatabaseDescriptor.of("PostgreSQL", "18", dialect),
                ACCOUNTS,
                SchemaSnapshot.absent(ACCOUNTS),
                SchemaSnapshotCoverage.of(Set.of(SchemaSnapshotCoverage.Fact.TABLE_EXISTENCE)));

        assertFalse(plan.requiresManualAction());
        assertTrue(plan.steps().isEmpty());
    }

    private static RelationalSchemaSqlRenderer renderer() {
        return RelationalSchemaSqlRenderer.create(RdbDialect.postgresql().schema());
    }

    private static SchemaOperation drop(SchemaOperation.Kind kind, String name, Object actual) {
        return SchemaOperation.of(
                kind, ACCOUNTS, name, actual, null, SchemaOperation.Compatibility.REQUIRES_REVIEW);
    }

    private static SchemaOperation change(SchemaOperation.Kind kind,
                                          String name,
                                          Object actual,
                                          Object desired) {
        return SchemaOperation.of(
                kind, ACCOUNTS, name, actual, desired, SchemaOperation.Compatibility.REQUIRES_REVIEW);
    }

    private static SchemaOperation changeColumn(ColumnDefinition actual, ColumnDefinition desired) {
        return change(SchemaOperation.Kind.CHANGE_COLUMN, desired.name(), actual, desired);
    }

    private static List<String> sql(List<com.flying.orm.core.sql.render.SqlRequest> requests) {
        return requests.stream().map(request -> request.sql()).toList();
    }

    private static RelationalTableDefinition simpleTable(ColumnDefinition code) {
        return RelationalTableDefinition.builder(ACCOUNTS).addColumn(ID).addColumn(code).build();
    }

    private static List<String> replacementSql(RelationalTableDefinition actual,
                                               RelationalTableDefinition desired) {
        ReviewedSchemaPlan plan = review(actual, desired);
        assertFalse(plan.requiresManualAction());
        return plan.requests().stream().map(request -> request.sql()).toList();
    }

    private static ReviewedSchemaPlan review(RelationalTableDefinition actual,
                                             RelationalTableDefinition desired) {
        RdbDialect dialect = RdbDialect.postgresql();
        return RelationalSchemaPlanReviewer.create(dialect).review(
                DatabaseDescriptor.of("PostgreSQL", "18", dialect),
                desired,
                SchemaSnapshot.present(actual),
                SchemaSnapshotCoverage.complete(),
                SchemaCompatibilityMode.EXACT);
    }

    private static void assertExecutableWidening(ColumnDefinition actual,
                                                 ColumnDefinition desired,
                                                 String expectedSql) {
        ReviewedSchemaPlan plan = review(simpleTable(actual), simpleTable(desired));
        assertFalse(plan.requiresManualAction());
        assertEquals(List.of(expectedSql), plan.requests().stream()
                .map(request -> request.sql()).toList());
    }

    private static RelationalTableDefinition indexedCandidates(ColumnDefinition code,
                                                               int stateLength,
                                                               boolean expanded) {
        IndexDefinition.Builder index = IndexDefinition.builder("ix_accounts_state")
                .addKey(IndexKeyPart.asc("state"));
        if (expanded) {
            index.addKey(IndexKeyPart.asc("id"));
        }
        return RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(ID).addColumn(code).addColumn(column("state", "VARCHAR", stateLength, true))
                .primaryKey(PrimaryKeyDefinition.of("pk_accounts", expanded
                        ? new String[]{"id", "code"} : new String[]{"id"}))
                .addUnique(UniqueConstraintDefinition.of("uq_accounts_code", expanded
                        ? new String[]{"code", "id"} : new String[]{"code"}))
                .addIndex(index.build()).build();
    }

    private static RelationalTableDefinition tableWithUnique(String name) {
        return RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(ID).addColumn(CODE_32)
                .addUnique(UniqueConstraintDefinition.of(name, "code"))
                .build();
    }

    private static RelationalTableDefinition tableWithIndex(String name, boolean unique) {
        IndexDefinition.Builder index = IndexDefinition.builder(name).addKey(IndexKeyPart.asc("code"));
        if (unique) {
            index.unique();
        }
        return RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(ID).addColumn(CODE_32).addIndex(index.build()).build();
    }

    private static RelationalTableDefinition tableWithCheck(String name) {
        return RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(ID).addColumn(CODE_32)
                .addCheck(CheckConstraintDefinition.of(name, CheckPredicate.isNotNull("code")))
                .build();
    }

    private static RelationalTableDefinition tableWithForeignKey(String name) {
        return RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(ID).addColumn(CODE_32)
                .addForeignKey(foreignKey(name, "code", ReferentialAction.NO_ACTION))
                .build();
    }

    private static RelationalTableDefinition candidateTable(
            PrimaryKeyDefinition primaryKey,
            UniqueConstraintDefinition unique,
            IndexDefinition index,
            ForeignKeyDefinition foreignKey) {
        RelationalTableDefinition.Builder builder = RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(ID)
                .addColumn(CODE_32)
                .addColumn(column("tenant", "VARCHAR", 32, false))
                .addColumn(column("note", "VARCHAR", 32, false))
                .addColumn(column("parent_code", "VARCHAR", 32, true))
                .addColumn(column("parent_tenant", "VARCHAR", 32, true));
        if (primaryKey != null) {
            builder.primaryKey(primaryKey);
        }
        if (unique != null) {
            builder.addUnique(unique);
        }
        if (index != null) {
            builder.addIndex(index);
        }
        return builder.addForeignKey(foreignKey).build();
    }

    private static RelationalTableDefinition table(ColumnDefinition code,
                                                   PrimaryKeyDefinition primaryKey,
                                                   UniqueConstraintDefinition unique,
                                                   IndexDefinition index,
                                                   CheckConstraintDefinition check,
                                                   ForeignKeyDefinition foreignKey) {
        return RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(ID)
                .addColumn(code)
                .primaryKey(primaryKey)
                .addUnique(unique)
                .addIndex(index)
                .addCheck(check)
                .addForeignKey(foreignKey)
                .build();
    }

    private static ColumnDefinition column(String name, String type, Integer length, boolean nullable) {
        return ColumnDefinition.builder(name, type).length(length).nullable(nullable).build();
    }

    private static IndexDefinition index(IndexKeyPart key) {
        return index("ix_accounts_code", key);
    }

    private static IndexDefinition index(String name, IndexKeyPart key) {
        return IndexDefinition.builder(name).addKey(key).build();
    }

    private static IndexDefinition uniqueIndex(String name, String... columns) {
        IndexDefinition.Builder builder = IndexDefinition.builder(name).unique();
        for (String column : columns) {
            builder.addKey(IndexKeyPart.asc(column));
        }
        return builder.build();
    }

    private static ForeignKeyDefinition foreignKey(ReferentialAction onDelete) {
        return foreignKey("fk_accounts_group", "code", onDelete);
    }

    private static ForeignKeyDefinition foreignKey(String name,
                                                   String column,
                                                   ReferentialAction onDelete) {
        return ForeignKeyDefinition.builder(name)
                .addColumn(column)
                .reference(GROUPS)
                .addReferenceColumn("code")
                .onDelete(onDelete)
                .build();
    }
}
