package com.flying.orm.core.metadata;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RelationalMetadataInvariantTest {

    @Test
    void rejectsDuplicatePrimaryKeyMembers() {
        assertThrows(IllegalArgumentException.class,
                     () -> PrimaryKeyDefinition.of("pk_users", "id", "id"));
    }

    @Test
    void rejectsDuplicateUniqueConstraintMembers() {
        assertThrows(IllegalArgumentException.class,
                     () -> UniqueConstraintDefinition.of("uk_users_email", "email", "email"));
    }

    @Test
    void rejectsDuplicateIndexMembersRegardlessOfDirection() {
        assertThrows(IllegalArgumentException.class, () -> IndexDefinition.builder("ix_users_email")
                .addKey(IndexKeyPart.asc("email"))
                .addKey(IndexKeyPart.desc("email"))
                .build());
    }

    @Test
    void rejectsDuplicateForeignKeyMembersOnEitherSide() {
        assertThrows(IllegalArgumentException.class, () -> ForeignKeyDefinition.builder("fk_orders_users")
                .addColumn("tenant_id")
                .addColumn("tenant_id")
                .reference(RelationIdentity.table("users"))
                .addReferenceColumn("tenant_id")
                .addReferenceColumn("id")
                .build());
        assertThrows(IllegalArgumentException.class, () -> ForeignKeyDefinition.builder("fk_orders_users")
                .addColumn("tenant_id")
                .addColumn("user_id")
                .reference(RelationIdentity.table("users"))
                .addReferenceColumn("id")
                .addReferenceColumn("id")
                .build());
    }

    @Test
    void rejectsUnknownColumnsOnManagedForeignKeyTargets() {
        RelationalTableDefinition users = table("users")
                .addColumn(requiredColumn("id"))
                .primaryKey(PrimaryKeyDefinition.of("pk_users", "id"))
                .build();
        RelationalTableDefinition orders = table("orders")
                .addColumn(nullableColumn("user_id"))
                .addForeignKey(foreignKey("fk_orders_users", "user_id", "users", "missing_id"))
                .build();

        assertThrows(IllegalArgumentException.class,
                     () -> RelationalSchemaDefinition.of(List.of(users, orders)));
    }

    @Test
    void rejectsManagedForeignKeyTargetsThatAreNotCandidateKeys() {
        RelationalTableDefinition users = table("users")
                .addColumn(requiredColumn("id"))
                .addColumn(requiredColumn("code"))
                .primaryKey(PrimaryKeyDefinition.of("pk_users", "id"))
                .build();
        RelationalTableDefinition orders = table("orders")
                .addColumn(nullableColumn("user_code"))
                .addForeignKey(foreignKey("fk_orders_users", "user_code", "users", "code"))
                .build();

        assertThrows(IllegalArgumentException.class,
                     () -> RelationalSchemaDefinition.of(List.of(users, orders)));
    }

    @Test
    void acceptsManagedForeignKeysTargetingEveryCandidateKeyKind() {
        RelationalTableDefinition users = table("users")
                .addColumn(requiredColumn("id"))
                .addColumn(requiredColumn("email"))
                .addColumn(requiredColumn("external_id"))
                .primaryKey(PrimaryKeyDefinition.of("pk_users", "id"))
                .addUnique(UniqueConstraintDefinition.of("uk_users_email", "email"))
                .addIndex(IndexDefinition.builder("ux_users_external_id")
                                  .unique()
                                  .addKey(IndexKeyPart.asc("external_id"))
                                  .build())
                .build();
        RelationalTableDefinition orders = table("orders")
                .addColumn(nullableColumn("user_id"))
                .addColumn(nullableColumn("user_email"))
                .addColumn(nullableColumn("external_user_id"))
                .addForeignKey(foreignKey("fk_orders_user_id", "user_id", "users", "id"))
                .addForeignKey(foreignKey("fk_orders_user_email", "user_email", "users", "email"))
                .addForeignKey(foreignKey("fk_orders_external_user", "external_user_id", "users", "external_id"))
                .build();

        assertDoesNotThrow(() -> RelationalSchemaDefinition.of(List.of(users, orders)));
    }

    @Test
    void allowsAForeignKeyAndItsIndependentSupportingIndexToShareAName() {
        assertDoesNotThrow(() -> table("child")
                .addColumn(nullableColumn("parent_id"))
                .addIndex(IndexDefinition.builder("fk_child_parent")
                                  .addKey(IndexKeyPart.asc("parent_id"))
                                  .build())
                .addForeignKey(foreignKey(
                        "fk_child_parent", "parent_id", "parent", "id"))
                .build());
    }

    @Test
    void allowsACheckConstraintAndAnIndexToShareAName() {
        assertDoesNotThrow(() -> table("accounts")
                .addColumn(requiredColumn("id"))
                .addCheck(CheckConstraintDefinition.of(
                        "ix_accounts_id",
                        CheckPredicate.compare(
                                "id", CheckPredicate.ComparisonOperator.GREATER_THAN, 0)))
                .addIndex(IndexDefinition.builder("ix_accounts_id")
                                  .addKey(IndexKeyPart.asc("id"))
                                  .build())
                .build());
    }

    @Test
    void keepsPrimaryAndUniqueNamesReservedFromExplicitIndexes() {
        assertThrows(IllegalArgumentException.class, () -> table("accounts")
                .addColumn(requiredColumn("id"))
                .primaryKey(PrimaryKeyDefinition.of("ix_accounts_id", "id"))
                .addIndex(IndexDefinition.builder("ix_accounts_id")
                                  .addKey(IndexKeyPart.asc("id"))
                                  .build())
                .build());
        assertThrows(IllegalArgumentException.class, () -> table("accounts")
                .addColumn(requiredColumn("id"))
                .addUnique(UniqueConstraintDefinition.of("ix_accounts_id", "id"))
                .addIndex(IndexDefinition.builder("ix_accounts_id")
                                  .addKey(IndexKeyPart.asc("id"))
                                  .build())
                .build());
    }

    @Test
    void leavesExternalSchemaForeignKeysUnvalidated() {
        RelationalTableDefinition orders = table("orders")
                .addColumn(nullableColumn("user_id"))
                .addForeignKey(ForeignKeyDefinition.builder("fk_orders_external_users")
                                       .addColumn("user_id")
                                       .reference(RelationIdentity.of(null, "external", "users"))
                                       .addReferenceColumn("unknown_external_column")
                                       .build())
                .build();

        assertDoesNotThrow(() -> RelationalSchemaDefinition.of(List.of(orders)));
    }

    @Test
    void rejectsNonFiniteFloatingPointColumnDefaults() {
        assertThrows(IllegalArgumentException.class, () -> ColumnDefault.literal(Float.NaN));
        assertThrows(IllegalArgumentException.class, () -> ColumnDefault.literal(Float.POSITIVE_INFINITY));
        assertThrows(IllegalArgumentException.class, () -> ColumnDefault.literal(Float.NEGATIVE_INFINITY));
        assertThrows(IllegalArgumentException.class, () -> ColumnDefault.literal(Double.NaN));
        assertThrows(IllegalArgumentException.class, () -> ColumnDefault.literal(Double.POSITIVE_INFINITY));
        assertThrows(IllegalArgumentException.class, () -> ColumnDefault.literal(Double.NEGATIVE_INFINITY));
    }

    @Test
    void normalizesEnumColumnDefaultsToTheirDatabaseLiteral() {
        ColumnDefault enumDefault = ColumnDefault.literal(Status.ACTIVE);

        assertEquals(ColumnDefault.literal("ACTIVE"), enumDefault);
        assertEquals("ACTIVE", enumDefault.value().orElseThrow());
    }

    private enum Status {
        ACTIVE
    }

    private static RelationalTableDefinition.Builder table(String name) {
        return RelationalTableDefinition.builder(RelationIdentity.table(name));
    }

    private static ColumnDefinition requiredColumn(String name) {
        return ColumnDefinition.builder(name, "BIGINT").nullable(false).build();
    }

    private static ColumnDefinition nullableColumn(String name) {
        return ColumnDefinition.builder(name, "BIGINT").build();
    }

    private static ForeignKeyDefinition foreignKey(
            String name,
            String localColumn,
            String referencedTable,
            String referencedColumn
    ) {
        return ForeignKeyDefinition.builder(name)
                .addColumn(localColumn)
                .reference(RelationIdentity.table(referencedTable))
                .addReferenceColumn(referencedColumn)
                .build();
    }
}
