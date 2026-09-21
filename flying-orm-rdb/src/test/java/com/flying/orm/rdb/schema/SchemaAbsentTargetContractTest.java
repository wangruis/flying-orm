package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SchemaAbsentTargetContractTest {

    private static final RelationIdentity ACCOUNTS =
            RelationIdentity.of(null, "security", "accounts");
    private static final DatabaseDescriptor DATABASE = DatabaseDescriptor.of(
            "PostgreSQL", "17", "postgresql", DialectCapabilities.empty());

    @Test
    void freezesExplicitAbsentTargetIntoTheReviewedPlanIdentity() {
        String desiredFingerprint = absentFingerprint(ACCOUNTS);

        ReviewedSchemaPlan plan = ReviewedSchemaPlan.builder(DATABASE)
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .desiredAbsent(ACCOUNTS)
                .desiredFingerprint(desiredFingerprint)
                .actualFingerprint("actual")
                .build();
        ReviewedSchemaPlan same = ReviewedSchemaPlan.builder(DATABASE)
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .desiredAbsent(ACCOUNTS)
                .desiredFingerprint(desiredFingerprint)
                .actualFingerprint("actual")
                .build();

        assertEquals(ReviewedSchemaPlan.TargetState.ABSENT, plan.targetState().orElseThrow());
        assertEquals(ACCOUNTS, plan.targetIdentity().orElseThrow());
        assertTrue(plan.desiredTable().isEmpty());
        assertEquals(plan.fingerprint(), same.fingerprint());

        ReviewedSchemaPlan legacy = ReviewedSchemaPlan.builder(DATABASE)
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .desiredFingerprint(desiredFingerprint)
                .actualFingerprint("actual")
                .build();
        assertTrue(legacy.targetState().isEmpty());
        assertTrue(legacy.targetIdentity().isEmpty());
        assertNotEquals(legacy.fingerprint(), plan.fingerprint());
    }

    @Test
    void rejectsAbsentTargetWhoseDesiredFingerprintNamesAnotherState() {
        assertThrows(IllegalArgumentException.class, () -> ReviewedSchemaPlan.builder(DATABASE)
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .desiredAbsent(ACCOUNTS)
                .desiredFingerprint("not-the-absent-snapshot")
                .actualFingerprint("actual")
                .build());
    }

    @Test
    void rejectsStepsThatCanExecuteAgainstAnotherTargetOrOppositeState() {
        RelationalTableDefinition accounts = RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .build();
        RelationIdentity audit = RelationIdentity.of(null, "security", "audit_log");
        RelationIdentity caseDifferentAccounts =
                RelationIdentity.of(null, "security", "Accounts");
        SchemaOperation dropAudit = SchemaOperation.of(
                SchemaOperation.Kind.DROP_TABLE,
                audit,
                audit.table(),
                RelationalTableDefinition.builder(audit)
                        .addColumn(ColumnDefinition.builder("id", "BIGINT").build())
                        .build(),
                null,
                SchemaOperation.Compatibility.REQUIRES_REVIEW);
        SchemaOperation dropCaseDifferentAccounts = SchemaOperation.of(
                SchemaOperation.Kind.DROP_TABLE,
                caseDifferentAccounts,
                caseDifferentAccounts.table(),
                RelationalTableDefinition.builder(caseDifferentAccounts)
                        .addColumn(ColumnDefinition.builder("id", "BIGINT").build())
                        .build(),
                null,
                SchemaOperation.Compatibility.REQUIRES_REVIEW);
        SchemaOperation addColumn = SchemaOperation.of(
                SchemaOperation.Kind.ADD_COLUMN,
                ACCOUNTS,
                "note",
                null,
                ColumnDefinition.builder("note", "VARCHAR").length(80).build(),
                SchemaOperation.Compatibility.SAFE_INCREMENTAL);
        SchemaOperation dropAccounts = SchemaOperation.of(
                SchemaOperation.Kind.DROP_TABLE,
                ACCOUNTS,
                ACCOUNTS.table(),
                accounts,
                null,
                SchemaOperation.Compatibility.REQUIRES_REVIEW);

        assertThrows(IllegalArgumentException.class, () -> absentPlan(dropAudit));
        assertThrows(IllegalArgumentException.class, () -> absentPlan(dropCaseDifferentAccounts));
        assertThrows(IllegalArgumentException.class, () -> absentPlan(addColumn));
        assertThrows(IllegalArgumentException.class, () -> ReviewedSchemaPlan.builder(DATABASE)
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .desiredTable(accounts)
                .desiredFingerprint(com.flying.orm.core.metadata.RelationalMetadataFingerprint.of(accounts))
                .actualFingerprint("actual")
                .addStep(SchemaPlanStep.manual(
                        0, dropAccounts, SchemaMigrationRiskLevel.CRITICAL, java.util.List.of()))
                .build());
    }

    @Test
    void diffsExplicitAbsenceWithoutInferringDeletionFromAMissingDesiredTable() {
        RelationalTableDefinition present = RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(ColumnDefinition.builder("id", "BIGINT").nullable(false).build())
                .build();

        SchemaCompatibilityReport drop = SchemaDiffer.diffAbsent(
                ACCOUNTS,
                SchemaSnapshot.present(present),
                DialectCapabilities.empty(),
                SchemaCompatibilityMode.EXACT);
        SchemaOperation operation = drop.operations().getFirst();
        assertEquals(SchemaOperation.Kind.DROP_TABLE, operation.kind());
        RelationalTableDefinition actual = (RelationalTableDefinition) operation.actual();
        assertEquals(present.identity(), actual.identity());
        assertEquals(present.columns(), actual.columns());
        assertNull(operation.desired());
        assertEquals(SchemaOperation.Compatibility.REQUIRES_REVIEW, operation.compatibility());

        assertTrue(SchemaDiffer.diffAbsent(
                ACCOUNTS,
                SchemaSnapshot.absent(ACCOUNTS),
                DialectCapabilities.empty(),
                SchemaCompatibilityMode.EXACT).exact());
        assertEquals(SchemaOperation.Kind.VERIFY_MANUALLY, SchemaDiffer.diffAbsent(
                ACCOUNTS,
                SchemaSnapshot.unknown(ACCOUNTS),
                DialectCapabilities.empty(),
                SchemaCompatibilityMode.EXACT).operations().getFirst().kind());

        SchemaSnapshot incompletePresent = SchemaSnapshot.builder(ACCOUNTS)
                .tablePresent()
                .columns(present.columns())
                .build();
        assertEquals(SchemaOperation.Kind.VERIFY_MANUALLY, SchemaDiffer.diffAbsent(
                ACCOUNTS,
                incompletePresent,
                DialectCapabilities.empty(),
                SchemaCompatibilityMode.EXACT).operations().getFirst().kind());
    }

    private static String absentFingerprint(RelationIdentity identity) {
        return SchemaSnapshotFingerprint.of(SchemaSnapshot.absent(identity));
    }

    private static ReviewedSchemaPlan absentPlan(SchemaOperation operation) {
        return ReviewedSchemaPlan.builder(DATABASE)
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .desiredAbsent(ACCOUNTS)
                .desiredFingerprint(absentFingerprint(ACCOUNTS))
                .actualFingerprint("actual")
                .addStep(SchemaPlanStep.manual(
                        0, operation, SchemaMigrationRiskLevel.CRITICAL, java.util.List.of()))
                .build();
    }
}
