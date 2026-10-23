package com.flying.orm.rdb.schema;

import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.DialectCapabilities;
import org.junit.jupiter.api.Test;
import java.util.List;

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
    void targetStateIsDerivedFromTheDeclaredTargetRatherThanStoredTwice() {
        for (Class<?> type : List.of(SchemaMigrationPlan.class, SchemaMigrationPlan.Builder.class)) {
            assertEquals(0L, java.util.Arrays.stream(type.getDeclaredFields())
                    .filter(field -> field.getType() == SchemaMigrationPlan.TargetState.class).count());
        }
        RelationalTableDefinition target = RelationalTableDefinition.builder(ACCOUNTS)
                .addColumn(ColumnDefinition.builder("id", "BIGINT").build()).build();
        SchemaMigrationPlan.Builder builder = SchemaMigrationPlan.builder(DATABASE)
                .compatibilityMode(SchemaCompatibilityMode.EXACT);
        SchemaMigrationPlan unspecified = builder.build();
        SchemaMigrationPlan present = builder.desiredTable(target).build();
        assertTrue(unspecified.targetState().isEmpty());
        assertTrue(unspecified.targetIdentity().isEmpty());
        assertEquals(SchemaMigrationPlan.TargetState.PRESENT, present.targetState().orElseThrow());
        assertEquals(target.identity(), present.targetIdentity().orElseThrow());
        assertThrows(IllegalStateException.class, () -> builder.desiredAbsent(ACCOUNTS));
        assertThrows(IllegalStateException.class, () -> SchemaMigrationPlan.builder(DATABASE)
                .desiredAbsent(ACCOUNTS).desiredTable(target));
    }

    @Test
    void freezesExplicitAbsentTargetIntoTheReviewedPlanIdentity() {

        SchemaMigrationPlan plan = SchemaMigrationPlan.builder(DATABASE)
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .desiredAbsent(ACCOUNTS)
                .build();
        SchemaMigrationPlan same = SchemaMigrationPlan.builder(DATABASE)
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .desiredAbsent(ACCOUNTS)
                .build();

        assertEquals(SchemaMigrationPlan.TargetState.ABSENT, plan.targetState().orElseThrow());
        assertEquals(ACCOUNTS, plan.targetIdentity().orElseThrow());
        assertTrue(plan.desiredTable().isEmpty());
        assertEquals(plan.requests(), same.requests());

        SchemaMigrationPlan legacy = SchemaMigrationPlan.builder(DATABASE)
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .build();
        assertTrue(legacy.targetState().isEmpty());
        assertTrue(legacy.targetIdentity().isEmpty());
        assertNotEquals(legacy.targetIdentity(), plan.targetIdentity());
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
        assertThrows(IllegalArgumentException.class, () -> SchemaMigrationPlan.builder(DATABASE)
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .desiredTable(accounts)
                .addStep(new SchemaPlanStep(dropAccounts, List.of()))
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

    private static SchemaMigrationPlan absentPlan(SchemaOperation operation) {
        return SchemaMigrationPlan.builder(DATABASE)
                .compatibilityMode(SchemaCompatibilityMode.EXACT)
                .desiredAbsent(ACCOUNTS)
                .addStep(new SchemaPlanStep(operation, List.of()))
                .build();
    }
}
