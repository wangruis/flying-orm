package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.ReferentialAction;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

class MySqlRelationalSchemaReplacementTest {

    private static final RelationIdentity TABLE = RelationIdentity.of(null, "app", "accounts");

    @Test
    void wideningKeepsObservedStorageOptionsBeforeColumnConstraints() {
        ColumnDefinition actual = ColumnDefinition.builder("code", "VARCHAR").length(32).nullable(false)
                .charset("utf8mb4").collation("utf8mb4_0900_ai_ci").comment("账号").build();
        ColumnDefinition desired = ColumnDefinition.builder("code", "VARCHAR").length(64).nullable(false)
                .comment("账号").build();
        SchemaOperation operation = SchemaOperation.of(SchemaOperation.Kind.CHANGE_COLUMN,
                TABLE, "code", actual, desired, SchemaOperation.Compatibility.REQUIRES_REVIEW);
        assertEquals(List.of("alter table `app`.`accounts` modify column `code` VARCHAR(64)"
                        + " character set `utf8mb4` collate `utf8mb4_0900_ai_ci` not null comment '账号'"),
                RelationalSchemaSqlRenderer.create(RdbDialect.mysql().schema())
                        .render(operation, new LinkedHashMap<>(), false)
                        .stream().map(request -> request.sql()).toList());
    }

    @Test
    void replacesCandidateKeysAndChecksInOneAlterStatement() {
        assertReplacement(SchemaOperation.Kind.CHANGE_PRIMARY_KEY,
                PrimaryKeyDefinition.of("pk_accounts", "id"),
                PrimaryKeyDefinition.of("pk_accounts", "id", "code"),
                "drop primary key, add constraint `pk_accounts` primary key (`id`, `code`)");
        assertReplacement(SchemaOperation.Kind.CHANGE_UNIQUE,
                UniqueConstraintDefinition.of("uq_accounts", "code"),
                UniqueConstraintDefinition.of("uq_accounts", "code", "id"),
                "drop index `uq_accounts`, add constraint `uq_accounts` unique (`code`, `id`)");
        assertReplacement(SchemaOperation.Kind.CHANGE_CHECK,
                CheckConstraintDefinition.of("ck_accounts", CheckPredicate.isNotNull("code")),
                CheckConstraintDefinition.of("ck_accounts", CheckPredicate.compare(
                        "code", CheckPredicate.ComparisonOperator.NOT_EQUAL, "")),
                "drop check `ck_accounts`, add constraint `ck_accounts` check (`code` <> '')");
    }

    @Test
    void replacesSameNamedForeignKeyWithSeparateStatements() {
        SchemaOperation operation = SchemaOperation.of(SchemaOperation.Kind.CHANGE_FOREIGN_KEY,
                TABLE, "fk_accounts", foreignKey(ReferentialAction.NO_ACTION),
                foreignKey(ReferentialAction.CASCADE), SchemaOperation.Compatibility.REQUIRES_REVIEW);
        assertEquals(List.of(
                "alter table `app`.`accounts` drop foreign key `fk_accounts`",
                "alter table `app`.`accounts` add constraint `fk_accounts` foreign key (`id`)"
                        + " references `app`.`groups` (`id`) on delete cascade"),
                RelationalSchemaSqlRenderer.create(RdbDialect.mysql().schema()).render(operation)
                        .stream().map(request -> request.sql()).toList());
    }

    @Test
    void replacesUniqueIndexesInOneAlterStatementWithoutADropCreateGap() {
        IndexDefinition actual = IndexDefinition.builder("ix_accounts").unique()
                .addKey(IndexKeyPart.asc("code")).build();
        IndexDefinition desired = IndexDefinition.builder("ix_accounts").unique()
                .addKey(IndexKeyPart.asc("code")).addKey(IndexKeyPart.desc("id")).build();
        SchemaOperation operation = SchemaOperation.of(SchemaOperation.Kind.CHANGE_INDEX,
                TABLE, desired.name(), actual, desired, SchemaOperation.Compatibility.REQUIRES_REVIEW);

        assertEquals(List.of("alter table `app`.`accounts` drop index `ix_accounts`,"
                        + " add unique index `ix_accounts` (`code` asc, `id` desc)"),
                RelationalSchemaSqlRenderer.create(RdbDialect.mysql().schema()).render(operation)
                        .stream().map(request -> request.sql()).toList());
    }

    @Test
    void reviewsSingleStatementCandidateReplacementAfterSupportedWidening() {
        RdbDialect dialect = RdbDialect.mysql();
        ReviewedSchemaPlan plan = RelationalSchemaPlanReviewer.create(dialect).review(
                DatabaseDescriptor.of("MySQL", "8.4", dialect), table(64, true),
                SchemaSnapshot.present(table(32, false)), SchemaSnapshotCoverage.complete(),
                SchemaCompatibilityMode.EXACT);

        assertFalse(plan.requiresManualAction());
        assertEquals(List.of(
                "alter table `app`.`accounts` modify column `code` VARCHAR(64) not null",
                "alter table `app`.`accounts` drop primary key,"
                        + " add constraint `pk_accounts` primary key (`id`, `code`)",
                "alter table `app`.`accounts` drop index `uq_accounts`,"
                        + " add constraint `uq_accounts` unique (`code`, `id`)"),
                plan.requests().stream().map(request -> request.sql()).toList());
    }

    private static void assertReplacement(SchemaOperation.Kind kind, Object actual, Object desired,
                                          String expectedAction) {
        String name = switch (desired) {
            case PrimaryKeyDefinition value -> value.name();
            case UniqueConstraintDefinition value -> value.name();
            case CheckConstraintDefinition value -> value.name();
            case ForeignKeyDefinition value -> value.name();
            default -> throw new AssertionError(desired);
        };
        SchemaOperation operation = SchemaOperation.of(kind, TABLE, name, actual, desired,
                SchemaOperation.Compatibility.REQUIRES_REVIEW);
        assertEquals(List.of("alter table `app`.`accounts` " + expectedAction),
                RelationalSchemaSqlRenderer.create(RdbDialect.mysql().schema()).render(operation)
                        .stream().map(request -> request.sql()).toList());
    }

    private static RelationalTableDefinition table(int length, boolean expanded) {
        return RelationalTableDefinition.builder(TABLE)
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("code", "VARCHAR").length(length).nullable(false).build())
                .primaryKey(PrimaryKeyDefinition.of("pk_accounts", expanded
                        ? new String[]{"id", "code"} : new String[]{"id"}))
                .addUnique(UniqueConstraintDefinition.of("uq_accounts", expanded
                        ? new String[]{"code", "id"} : new String[]{"code"})).build();
    }

    private static ForeignKeyDefinition foreignKey(ReferentialAction action) {
        return ForeignKeyDefinition.builder("fk_accounts").addColumn("id")
                .reference(RelationIdentity.of(null, "app", "groups")).addReferenceColumn("id")
                .onDelete(action).build();
    }
}
