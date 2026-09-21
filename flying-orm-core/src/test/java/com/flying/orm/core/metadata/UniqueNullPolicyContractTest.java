package com.flying.orm.core.metadata;

import com.flying.orm.core.annotation.TableUnique;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class UniqueNullPolicyContractTest {

    @Test
    void explicitNullPolicyPreservesLegacyConstructionAndAnnotationDefaults() {
        Class<?> policy = assertDoesNotThrow(() -> Class.forName("com.flying.orm.core.metadata.UniqueNullPolicy"));
        assertEquals(List.of("DEFAULT", "DISTINCT"), Arrays.stream(policy.getEnumConstants())
                .map(Object::toString).toList());
        var accessor = assertDoesNotThrow(() -> UniqueConstraintDefinition.class.getMethod("nullPolicy"));
        assertEquals("DEFAULT", assertDoesNotThrow(() -> accessor.invoke(
                new UniqueConstraintDefinition("uq_email", List.of("email")))).toString());
        assertEquals("DEFAULT", assertDoesNotThrow(() -> accessor.invoke(
                UniqueConstraintDefinition.of("uq_email", "email"))).toString());
        assertEquals("DEFAULT", assertDoesNotThrow(() -> TableUnique.class.getMethod("nullPolicy"))
                .getDefaultValue().toString());
    }

    @Test
    void explicitDistinctPolicyParticipatesInMetadataIdentityAndFingerprint() {
        Class<?> policy = assertDoesNotThrow(() -> Class.forName("com.flying.orm.core.metadata.UniqueNullPolicy"));
        var constructor = assertDoesNotThrow(() -> UniqueConstraintDefinition.class.getConstructor(
                String.class, List.class, policy));
        UniqueConstraintDefinition legacy = UniqueConstraintDefinition.of("uq_email", "email");
        UniqueConstraintDefinition distinct = assertDoesNotThrow(() -> constructor.newInstance(
                "uq_email", List.of("email"), policy.getEnumConstants()[1]));
        UniqueConstraintDefinition explicitDefault = assertDoesNotThrow(() -> constructor.newInstance(
                "uq_email", List.of("email"), policy.getEnumConstants()[0]));

        assertEquals(legacy, explicitDefault);
        assertNotEquals(legacy, distinct);
        assertEquals(RelationalMetadataFingerprint.of(table(legacy)),
                RelationalMetadataFingerprint.of(table(explicitDefault)));
        assertNotEquals(RelationalMetadataFingerprint.of(table(legacy)),
                RelationalMetadataFingerprint.of(table(distinct)));
    }

    @Test
    void defaultUniqueFingerprintRetainsItsEncoding() {
        assertEquals("c96d146e9d9c8ad011dcee0e61d40beab03b35d64cc5aa88a4410f8ae3593d11",
                RelationalMetadataFingerprint.of(table(UniqueConstraintDefinition.of("uq_email", "email"))));
    }

    @Test
    void distinctUniqueCannotServeAsAPortableForeignKeyTarget() {
        RelationalTableDefinition target = table(new UniqueConstraintDefinition(
                "uq_email", List.of("email"), UniqueNullPolicy.DISTINCT));
        RelationalTableDefinition reference = RelationalTableDefinition.builder(RelationIdentity.table("messages"))
                .addColumn(ColumnDefinition.builder("email", "VARCHAR").build())
                .addForeignKey(ForeignKeyDefinition.builder("fk_email").addColumn("email")
                        .reference(target.identity()).addReferenceColumn("email").build())
                .build();

        assertThrows(IllegalArgumentException.class, () -> RelationalSchemaDefinition.of(List.of(target, reference)));
        assertDoesNotThrow(() -> RelationalSchemaDefinition.of(List.of(
                table(UniqueConstraintDefinition.of("uq_email", "email")), reference)));
    }

    @Test
    void aSeparateDefaultKeyRetainsForeignKeyEligibility() {
        RelationalTableDefinition target = RelationalTableDefinition.builder(RelationIdentity.table("accounts"))
                .addColumn(ColumnDefinition.builder("email", "VARCHAR").nullable(true).build())
                .addUnique(new UniqueConstraintDefinition("uq_distinct", List.of("email"), UniqueNullPolicy.DISTINCT))
                .addUnique(UniqueConstraintDefinition.of("uq_email", "email")).build();
        RelationalTableDefinition reference = RelationalTableDefinition.builder(RelationIdentity.table("messages"))
                .addColumn(ColumnDefinition.builder("email", "VARCHAR").build())
                .addForeignKey(ForeignKeyDefinition.builder("fk_email").addColumn("email")
                        .reference(target.identity()).addReferenceColumn("email").build())
                .build();

        assertDoesNotThrow(() -> RelationalSchemaDefinition.of(List.of(target, reference)));
    }

    @Test
    void explicitPolicyKeepsColumnSnapshotAndRejectsNullPolicy() {
        var columns = new java.util.ArrayList<>(List.of("email"));
        var unique = new UniqueConstraintDefinition("uq_email", columns, UniqueNullPolicy.DISTINCT);
        columns.set(0, "changed");

        assertEquals(List.of("email"), unique.columns());
        assertThrows(NullPointerException.class, () -> new UniqueConstraintDefinition("uq_email", List.of("email"), null));
    }

    private static RelationalTableDefinition table(UniqueConstraintDefinition unique) {
        return RelationalTableDefinition.builder(RelationIdentity.table("accounts"))
                .addColumn(ColumnDefinition.builder("email", "VARCHAR").nullable(true).build())
                .addUnique(unique).build();
    }
}
