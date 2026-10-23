package com.flying.orm.core.metadata;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class RelationalMetadataFingerprintTest {

    @Test
    void emptySchemasRetainTheirDeclaredNamespaceInTheFingerprint() {
        RelationalSchemaDefinition first = RelationalSchemaDefinition.builder("catalog", "first").build();
        RelationalSchemaDefinition second = RelationalSchemaDefinition.builder("catalog", "second").build();
        RelationalSchemaDefinition otherCatalog = RelationalSchemaDefinition.builder("other", "first").build();

        assertNotEquals(RelationalMetadataFingerprint.of(first), RelationalMetadataFingerprint.of(second));
        assertNotEquals(RelationalMetadataFingerprint.of(first), RelationalMetadataFingerprint.of(otherCatalog));
        assertEquals(RelationalMetadataFingerprint.of(first), RelationalMetadataFingerprint.of(
                RelationalSchemaDefinition.builder("catalog", "first").build()));
    }

    @Test
    void schemaFingerprintIgnoresTableOrderButIncludesEveryTable() {
        RelationalTableDefinition first = table("a", "b", false);
        RelationalTableDefinition second = RelationalTableDefinition.builder("other")
                .addColumn(ColumnDefinition.builder("id", "BIGINT").build()).build();
        String schema = RelationalMetadataFingerprint.of(
                RelationalSchemaDefinition.of(java.util.List.of(first, second)));

        assertEquals(schema, RelationalMetadataFingerprint.of(
                RelationalSchemaDefinition.of(java.util.List.of(second, first))));
        assertNotEquals(schema, RelationalMetadataFingerprint.of(
                RelationalSchemaDefinition.of(java.util.List.of(first))));
    }

    @Test
    void namedConstraintCollectionOrderIsStableButCompositeColumnOrderIsSemantic() {
        RelationalTableDefinition first = table("a", "b", false);
        RelationalTableDefinition reorderedConstraints = table("a", "b", true);
        RelationalTableDefinition reversedPrimaryKey = table("b", "a", false);

        assertEquals(RelationalMetadataFingerprint.of(first),
                     RelationalMetadataFingerprint.of(reorderedConstraints));
        assertNotEquals(RelationalMetadataFingerprint.of(first),
                        RelationalMetadataFingerprint.of(reversedPrimaryKey));
    }

    @Test
    void ormCodecIdentityIsNotAPhysicalDdlFact() {
        RelationalTableDefinition standard = RelationalTableDefinition.builder(RelationIdentity.table("sample"))
                .addColumn(ColumnDefinition.builder("payload", "JSON").build())
                .build();
        RelationalTableDefinition customCodec = RelationalTableDefinition.builder(RelationIdentity.table("sample"))
                .addColumn(ColumnDefinition.builder("payload", "JSON").codecId("json-codec").build())
                .build();

        assertEquals(RelationalMetadataFingerprint.of(standard),
                     RelationalMetadataFingerprint.of(customCodec));
    }

    private static RelationalTableDefinition table(String firstKey, String secondKey, boolean reverseUniqueOrder) {
        RelationalTableDefinition.Builder builder = RelationalTableDefinition.builder(RelationIdentity.table("sample"))
                .addColumn(ColumnDefinition.builder("a", "BIGINT").nullable(false).build())
                .addColumn(ColumnDefinition.builder("b", "BIGINT").nullable(false).build())
                .primaryKey(PrimaryKeyDefinition.of("pk_sample", firstKey, secondKey));
        UniqueConstraintDefinition first = UniqueConstraintDefinition.of("uk_sample_a", "a");
        UniqueConstraintDefinition second = UniqueConstraintDefinition.of("uk_sample_b", "b");
        return (reverseUniqueOrder
                ? builder.addUnique(second).addUnique(first)
                : builder.addUnique(first).addUnique(second)).build();
    }
}
