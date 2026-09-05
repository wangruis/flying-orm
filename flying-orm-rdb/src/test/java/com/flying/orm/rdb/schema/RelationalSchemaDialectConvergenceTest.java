package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.ReferentialAction;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

class RelationalSchemaDialectConvergenceTest {

    @Test
    void comparesDesiredLogicalTypesWithTheDialectsObservedPhysicalTypes() {
        assertConverged(
                RdbDialect.oracle(),
                table(ColumnDefinition.builder("business_date", "DATE").build(),
                      ColumnDefinition.builder("description", "TEXT").build()),
                table(ColumnDefinition.builder("business_date", "ORACLE_DATE").build(),
                      ColumnDefinition.builder("description", "CLOB").build()));
        assertConverged(
                RdbDialect.mysql(),
                table(ColumnDefinition.builder("description", "CLOB").build(),
                      ColumnDefinition.builder("payload", "BINARY").build()),
                table(ColumnDefinition.builder("description", "LONGTEXT").build(),
                      ColumnDefinition.builder("payload", "LONGBLOB").build()));
        assertConverged(
                RdbDialect.h2(),
                table(ColumnDefinition.builder("description", "NCLOB").build()),
                table(ColumnDefinition.builder("description", "CLOB").build()));
    }

    @Test
    void h2UnquotedIdentifiersConvergeAfterTheDatabaseFoldsTheirCase() {
        RelationalTableDefinition desired = RelationalTableDefinition.builder(
                        RelationIdentity.table("SampleTable"))
                .addColumn(ColumnDefinition.builder("EventId", "BIGINT").nullable(false).build())
                .primaryKey(PrimaryKeyDefinition.of("PkSample", "EventId"))
                .addUnique(UniqueConstraintDefinition.of("UkSample", "EventId"))
                .build();
        RelationalTableDefinition actual = RelationalTableDefinition.builder(
                        RelationIdentity.table("sampletable"))
                .addColumn(ColumnDefinition.builder("eventid", "BIGINT").nullable(false).build())
                .primaryKey(PrimaryKeyDefinition.of("pksample", "eventid"))
                .addUnique(UniqueConstraintDefinition.of("uksample", "eventid"))
                .build();

        assertConverged(RdbDialect.h2(), desired, actual);
    }

    @Test
    void checkLiteralComparisonUsesDatabaseValuesInsteadOfJavaWrapperIdentity() {
        Instant instant = Instant.parse("2026-09-03T12:00:00Z");
        RelationalTableDefinition desired = RelationalTableDefinition.builder(
                        RelationIdentity.table("sample"))
                .addColumn(ColumnDefinition.builder("small_value", "INTEGER").build())
                .addColumn(ColumnDefinition.builder("decimal_value", "DECIMAL").build())
                .addColumn(ColumnDefinition.builder("created_at", "TIMESTAMPTZ").build())
                .addCheck(CheckConstraintDefinition.of(
                        "ck_small", CheckPredicate.compare(
                                "small_value", CheckPredicate.ComparisonOperator.GREATER_THAN,
                                (short) 7)))
                .addCheck(CheckConstraintDefinition.of(
                        "ck_decimal", CheckPredicate.in(
                                "decimal_value", java.util.List.of(
                                        new BigInteger("9"), 1.5F))))
                .addCheck(CheckConstraintDefinition.of(
                        "ck_created", CheckPredicate.compare(
                                "created_at", CheckPredicate.ComparisonOperator.GREATER_THAN, instant)))
                .build();
        RelationalTableDefinition actual = RelationalTableDefinition.builder(
                        RelationIdentity.table("sample"))
                .addColumn(ColumnDefinition.builder("small_value", "INTEGER").build())
                .addColumn(ColumnDefinition.builder("decimal_value", "DECIMAL").build())
                .addColumn(ColumnDefinition.builder("created_at", "TIMESTAMPTZ").build())
                .addCheck(CheckConstraintDefinition.of(
                        "ck_small", CheckPredicate.compare(
                                "small_value", CheckPredicate.ComparisonOperator.GREATER_THAN, 7)))
                .addCheck(CheckConstraintDefinition.of(
                        "ck_decimal", CheckPredicate.in(
                                "decimal_value", java.util.List.of(
                                        new BigDecimal("9"), new BigDecimal("1.5")))))
                .addCheck(CheckConstraintDefinition.of(
                        "ck_created", CheckPredicate.compare(
                                "created_at", CheckPredicate.ComparisonOperator.GREATER_THAN,
                                OffsetDateTime.ofInstant(instant, ZoneOffset.UTC))))
                .build();

        assertConverged(RdbDialect.postgresql(), desired, actual);
    }

    @Test
    void defaultLiteralComparisonUsesDatabaseValuesInsteadOfJavaWrapperIdentity() {
        Instant instant = Instant.parse("2026-09-03T12:00:00Z");
        RelationalTableDefinition desired = table(
                ColumnDefinition.builder("retry_count", "INTEGER")
                        .defaultValue(ColumnDefault.literal((short) 7)).build(),
                ColumnDefinition.builder("created_at", "TIMESTAMPTZ")
                        .defaultValue(ColumnDefault.literal(instant)).build());
        RelationalTableDefinition actual = table(
                ColumnDefinition.builder("retry_count", "INTEGER")
                        .defaultValue(ColumnDefault.literal(7)).build(),
                ColumnDefinition.builder("created_at", "TIMESTAMPTZ")
                        .defaultValue(ColumnDefault.literal(
                                OffsetDateTime.ofInstant(instant, ZoneOffset.UTC))).build());

        assertConverged(RdbDialect.postgresql(), desired, actual);
    }

    @Test
    void generationComparisonHonorsUnspecifiedCacheAndRejectsUnverifiableMysqlOptions() {
        RelationalTableDefinition desired = table(ColumnDefinition.builder("id", "BIGINT")
                .generation(ValueGeneration.identity(1, 1, 0)).build());
        RelationalTableDefinition actual = table(ColumnDefinition.builder("id", "BIGINT")
                .generation(ValueGeneration.identity(1, 1, 100)).build());
        assertConverged(RdbDialect.postgresql(), desired, actual);

        RdbDialect mysql = RdbDialect.mysql();
        RelationalTableDefinition unsupported = table(ColumnDefinition.builder("id", "BIGINT")
                .generation(ValueGeneration.identity(7, 1, 64)).build());
        ReviewedSchemaPlan plan = RelationalSchemaPlanReviewer.create(mysql).review(
                DatabaseDescriptor.of("mysql", "8.4", mysql), unsupported,
                SchemaSnapshot.absent(unsupported.identity()), SchemaSnapshotCoverage.complete(),
                SchemaCompatibilityMode.EXACT);
        assertTrue(plan.requiresManualAction());
        assertTrue(plan.requests().isEmpty());
    }

    @Test
    void h2RendersAndComparesTheDeclaredIdentityCache() {
        RdbDialect h2 = RdbDialect.h2();
        RelationalTableDefinition desired = table(ColumnDefinition.builder("id", "BIGINT")
                .generation(ValueGeneration.identity(3, 2, 64)).build());
        ReviewedSchemaPlan create = RelationalSchemaPlanReviewer.create(h2).review(
                DatabaseDescriptor.of("h2", "2.3", h2), desired,
                SchemaSnapshot.absent(desired.identity()), SchemaSnapshotCoverage.complete(),
                SchemaCompatibilityMode.EXACT);
        assertFalse(create.requiresManualAction());
        assertTrue(create.requests().getFirst().sql().toLowerCase().contains("cache 64"));
        assertConverged(h2, desired, desired);
    }

    @Test
    void oraclePersistsNamedSequenceIdentityAndRejectsAnUnverifiableStart() {
        RdbDialect oracle = RdbDialect.oracle();
        RelationalTableDefinition desired = table(ColumnDefinition.builder("id", "BIGINT")
                .generation(ValueGeneration.sequence("sample_seq")).build());
        ReviewedSchemaPlan create = RelationalSchemaPlanReviewer.create(oracle).review(
                DatabaseDescriptor.of("oracle", "19c", oracle), desired,
                SchemaSnapshot.absent(desired.identity()), SchemaSnapshotCoverage.complete(),
                SchemaCompatibilityMode.EXACT);

        assertFalse(create.requiresManualAction());
        assertTrue(create.requests().stream().map(request -> request.sql().toLowerCase())
                         .anyMatch(sql -> sql.contains("[[flying-orm:v1:sequence:sample_seq]]")));

        RelationalTableDefinition unsupported = table(ColumnDefinition.builder("id", "BIGINT")
                .generation(ValueGeneration.sequence("sample_seq", 7, 1, 100)).build());
        ReviewedSchemaPlan manual = RelationalSchemaPlanReviewer.create(oracle).review(
                DatabaseDescriptor.of("oracle", "19c", oracle), unsupported,
                SchemaSnapshot.absent(unsupported.identity()), SchemaSnapshotCoverage.complete(),
                SchemaCompatibilityMode.EXACT);
        assertTrue(manual.requiresManualAction());
        assertTrue(manual.requests().isEmpty());
    }

    @Test
    void unsupportedSetDefaultForeignKeyActionsStopBeforeSqlGeneration() {
        assertUnsupportedForeignKeyAction(
                RdbDialect.mysql(), ReferentialAction.SET_DEFAULT, ReferentialAction.NO_ACTION);
        assertUnsupportedForeignKeyAction(
                RdbDialect.mysql(), ReferentialAction.NO_ACTION, ReferentialAction.SET_DEFAULT);
        assertUnsupportedForeignKeyAction(
                RdbDialect.oracle(), ReferentialAction.SET_DEFAULT, ReferentialAction.NO_ACTION);
    }

    @Test
    void postgresqlKeepsSupportingSetDefaultForeignKeyActions() {
        RdbDialect dialect = RdbDialect.postgresql();
        RelationalTableDefinition desired = tableWithForeignKeyActions(
                ReferentialAction.SET_DEFAULT, ReferentialAction.SET_DEFAULT);

        ReviewedSchemaPlan create = RelationalSchemaPlanReviewer.create(dialect).review(
                DatabaseDescriptor.of(dialect.name(), "test", dialect), desired,
                SchemaSnapshot.absent(desired.identity()), SchemaSnapshotCoverage.complete(),
                SchemaCompatibilityMode.EXACT);

        assertFalse(create.requiresManualAction());
        assertTrue(create.requests().getFirst().sql().toLowerCase(java.util.Locale.ROOT)
                .contains("on delete set default"));
        assertTrue(create.requests().getFirst().sql().toLowerCase(java.util.Locale.ROOT)
                .contains("on update set default"));
    }

    private static void assertConverged(RdbDialect dialect,
                                        RelationalTableDefinition desired,
                                        RelationalTableDefinition actual) {
        ReviewedSchemaPlan plan = RelationalSchemaPlanReviewer.create(dialect).review(
                DatabaseDescriptor.of(dialect.name(), "test", dialect),
                desired,
                SchemaSnapshot.present(actual),
                SchemaSnapshotCoverage.complete(),
                SchemaCompatibilityMode.EXACT);
        assertTrue(plan.steps().isEmpty(), () -> plan.operations().toString());
    }

    private static RelationalTableDefinition table(ColumnDefinition... columns) {
        RelationalTableDefinition.Builder builder = RelationalTableDefinition.builder(
                RelationIdentity.table("sample"));
        for (ColumnDefinition column : columns) {
            builder.addColumn(column);
        }
        return builder.build();
    }

    private static void assertUnsupportedForeignKeyAction(RdbDialect dialect,
                                                          ReferentialAction onDelete,
                                                          ReferentialAction onUpdate) {
        RelationalTableDefinition desired = tableWithForeignKeyActions(onDelete, onUpdate);
        ReviewedSchemaPlan create = RelationalSchemaPlanReviewer.create(dialect).review(
                DatabaseDescriptor.of(dialect.name(), "test", dialect), desired,
                SchemaSnapshot.absent(desired.identity()), SchemaSnapshotCoverage.complete(),
                SchemaCompatibilityMode.EXACT);
        assertTrue(create.requiresManualAction(), dialect.name());
        assertTrue(create.requests().isEmpty(), dialect.name());

        RelationalTableDefinition withoutForeignKey = table(
                ColumnDefinition.builder("id", "BIGINT").nullable(false).build(),
                ColumnDefinition.builder("parent_id", "BIGINT").build());
        ReviewedSchemaPlan alter = RelationalSchemaPlanReviewer.create(dialect).review(
                DatabaseDescriptor.of(dialect.name(), "test", dialect), desired,
                SchemaSnapshot.present(withoutForeignKey), SchemaSnapshotCoverage.complete(),
                SchemaCompatibilityMode.EXACT);
        assertTrue(alter.requiresManualAction(), dialect.name());
        assertTrue(alter.requests().isEmpty(), dialect.name());
    }

    private static RelationalTableDefinition tableWithForeignKeyActions(ReferentialAction onDelete,
                                                                         ReferentialAction onUpdate) {
        return RelationalTableDefinition.builder(RelationIdentity.table("sample"))
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("parent_id", "BIGINT").build())
                .addForeignKey(ForeignKeyDefinition.builder("fk_sample_parent")
                        .addColumn("parent_id")
                        .reference(RelationIdentity.table("parent_sample"))
                        .addReferenceColumn("id")
                        .onDelete(onDelete)
                        .onUpdate(onUpdate)
                        .build())
                .build();
    }
}
