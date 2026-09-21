package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaRiskClassifierTest {

    private static final RelationIdentity ACCOUNTS = RelationIdentity.table("accounts");
    private static final ColumnDefinition ID =
            ColumnDefinition.builder("id", "BIGINT").nullable(false).build();
    private static final ColumnDefinition CODE =
            ColumnDefinition.builder("code", "VARCHAR").length(40).build();
    private static final RelationalTableDefinition EXISTING_TABLE = RelationalTableDefinition.builder(ACCOUNTS)
            .addColumn(ID)
            .addColumn(CODE)
            .build();

    @Test
    void classifiesCreationAsLowRiskOnlyWhenTheTableIsKnownAbsent() {
        SchemaOperation create = SchemaOperation.of(
                SchemaOperation.Kind.CREATE_TABLE,
                ACCOUNTS,
                "accounts",
                null,
                EXISTING_TABLE,
                SchemaOperation.Compatibility.SAFE_INCREMENTAL);

        assertEquals(SchemaMigrationRiskLevel.LOW, SchemaRiskClassifier.classify(
                create, SchemaSnapshot.absent(ACCOUNTS), DialectCapabilities.empty()));
        assertTrue(SchemaRiskClassifier.safeIncremental(
                create, SchemaSnapshot.absent(ACCOUNTS), DialectCapabilities.empty()));
        assertFalse(SchemaRiskClassifier.safeIncremental(
                create, SchemaSnapshot.present(EXISTING_TABLE), DialectCapabilities.empty()));

        ForeignKeyDefinition foreignKey = ForeignKeyDefinition.builder("fk_accounts_customer")
                .addColumn("id")
                .reference(RelationIdentity.table("customers"))
                .addReferenceColumn("id")
                .build();
        RelationalTableDefinition tableWithForeignKey = RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(ID)
                .addForeignKey(foreignKey)
                .build();
        SchemaOperation createWithForeignKey = SchemaOperation.of(
                SchemaOperation.Kind.CREATE_TABLE,
                ACCOUNTS,
                "accounts",
                null,
                tableWithForeignKey,
                SchemaOperation.Compatibility.SAFE_INCREMENTAL);
        assertEquals(SchemaMigrationRiskLevel.HIGH, SchemaRiskClassifier.classify(
                createWithForeignKey, SchemaSnapshot.absent(ACCOUNTS), DialectCapabilities.empty()));
    }

    @Test
    void constraintsNarrowingRemovalAndUnverifiedWorkNeverDefaultToSafe() {
        ColumnDefinition required = ColumnDefinition.builder("required", "VARCHAR")
                .length(40)
                .nullable(false)
                .build();
        ColumnDefinition widenedCode = ColumnDefinition.builder("code", "BIGINT").build();
        ColumnDefinition optionalWithDefault = ColumnDefinition.builder("state", "VARCHAR")
                .defaultValue(ColumnDefault.literal("new"))
                .build();
        UniqueConstraintDefinition unique = UniqueConstraintDefinition.of("uk_accounts_code", "code");
        ForeignKeyDefinition foreignKey = ForeignKeyDefinition.builder("fk_accounts_customer")
                .addColumn("id")
                .reference(RelationIdentity.table("customers"))
                .addReferenceColumn("id")
                .build();
        CheckConstraintDefinition check = CheckConstraintDefinition.of(
                "ck_accounts_id", CheckPredicate.compare(
                        "id", CheckPredicate.ComparisonOperator.GREATER_THAN, 0));
        List<ExpectedRisk> cases = List.of(
                expected(SchemaOperation.Kind.ADD_UNIQUE, "uk_accounts_code", null, unique,
                         SchemaMigrationRiskLevel.HIGH),
                expected(SchemaOperation.Kind.ADD_FOREIGN_KEY, "fk_accounts_customer", null, foreignKey,
                         SchemaMigrationRiskLevel.CRITICAL),
                expected(SchemaOperation.Kind.ADD_CHECK, "ck_accounts_id", null, check,
                         SchemaMigrationRiskLevel.HIGH),
                expected(SchemaOperation.Kind.ADD_COLUMN, "required", null, required,
                         SchemaMigrationRiskLevel.HIGH, SchemaOperation.Compatibility.SAFE_INCREMENTAL),
                expected(SchemaOperation.Kind.ADD_COLUMN, "state", null, optionalWithDefault,
                         SchemaMigrationRiskLevel.HIGH, SchemaOperation.Compatibility.SAFE_INCREMENTAL),
                expected(SchemaOperation.Kind.CHANGE_COLUMN, "code", CODE, widenedCode,
                         SchemaMigrationRiskLevel.CRITICAL),
                expected(SchemaOperation.Kind.DROP_COLUMN, "code", CODE, null,
                         SchemaMigrationRiskLevel.CRITICAL),
                expected(SchemaOperation.Kind.VERIFY_MANUALLY, "rename:code->external_code", null, null,
                         SchemaMigrationRiskLevel.HIGH),
                expected(SchemaOperation.Kind.VERIFY_MANUALLY, "backfill:required", null, null,
                         SchemaMigrationRiskLevel.HIGH));
        SchemaSnapshot actual = SchemaSnapshot.present(EXISTING_TABLE);

        for (ExpectedRisk expected : cases) {
            SchemaMigrationRiskLevel risk = SchemaRiskClassifier.classify(
                    expected.operation(), actual, DialectCapabilities.empty());
            assertEquals(expected.risk(), risk, expected.operation()::toString);
            assertFalse(SchemaRiskClassifier.safeIncremental(
                    expected.operation(), actual, DialectCapabilities.empty()),
                    expected.operation()::toString);
        }
    }

    private static ExpectedRisk expected(SchemaOperation.Kind kind,
                                         String objectName,
                                         Object actual,
                                         Object desired,
                                         SchemaMigrationRiskLevel risk) {
        return expected(kind, objectName, actual, desired, risk, SchemaOperation.Compatibility.REQUIRES_REVIEW);
    }

    private static ExpectedRisk expected(SchemaOperation.Kind kind,
                                         String objectName,
                                         Object actual,
                                         Object desired,
                                         SchemaMigrationRiskLevel risk,
                                         SchemaOperation.Compatibility compatibility) {
        return new ExpectedRisk(SchemaOperation.of(
                kind, ACCOUNTS, objectName, actual, desired, compatibility), risk);
    }

    private record ExpectedRisk(SchemaOperation operation, SchemaMigrationRiskLevel risk) {
    }
}
