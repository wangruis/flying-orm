package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.FieldFill;
import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.metadata.ColumnDefinition;
import com.flying.orm.core.metadata.ColumnDefault;
import com.flying.orm.core.protection.FieldProtectionRegistry;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class EntityMetadataStructureFingerprintTest {

    @Test
    void physicalFingerprintIgnoresEntityDeclarationOrder() {
        EntityFieldMetadata first = field("first", "VARCHAR");
        EntityFieldMetadata second = field("second", "BIGINT");
        assertEquals(metadata("records", first, second).structureFingerprint(),
                metadata("records", second, first).structureFingerprint());
    }

    @Test
    void literalDefaultContentsAffectThePhysicalFingerprint() {
        ColumnDefinition base = ColumnDefinition.builder("value", "VARCHAR").build();
        assertNotEquals(metadata("records", field(base.toBuilder()
                        .defaultValue(ColumnDefault.literal("ready")).build())).structureFingerprint(),
                metadata("records", field(base.toBuilder()
                        .defaultValue(ColumnDefault.literal("pending")).build())).structureFingerprint());
    }

    @TestFactory
    Stream<DynamicTest> distinguishesDefaultsAndTextStorageOptions() {
        ColumnDefinition base = ColumnDefinition.builder("value", "VARCHAR").build();
        return Stream.of(
                base.toBuilder().defaultValue(ColumnDefault.literal("ready")).build(),
                base.toBuilder().defaultValue(ColumnDefault.literal("pending")).build(),
                base.toBuilder().defaultConstraintName("df_value").build(),
                base.toBuilder().charset("utf8").build(),
                base.toBuilder().collation("utf8_bin").build())
                .map(column -> DynamicTest.dynamicTest(column.toString(), () ->
                        assertNotEquals(metadata("records", field(base)).structureFingerprint(),
                                metadata("records", field(column)).structureFingerprint())));
    }

    @Test
    void distinguishesPhysicalTableNamesThatDifferOnlyByCase() {
        EntityMetadata<Object> upperCaseTable = metadata("CustomerData");
        EntityMetadata<Object> lowerCaseTable = metadata("customerdata");

        assertNotEquals(upperCaseTable.structureFingerprint(), lowerCaseTable.structureFingerprint());
    }

    @Test
    void distinguishesFieldShapesWhoseNamesAndTypesContainDescriptorSeparators() {
        EntityMetadata<Object> colonInName = metadata("records", field("a:1", "2"));
        EntityMetadata<Object> colonInType = metadata("records", field("a", "1:2"));

        assertNotEquals(colonInName.structureFingerprint(), colonInType.structureFingerprint());
    }

    @Test
    void distinguishesPhysicalColumnNamesThatDifferOnlyByCase() {
        EntityMetadata<Object> upperCaseColumn = metadata("records", field("CustomerId", "BIGINT"));
        EntityMetadata<Object> lowerCaseColumn = metadata("records", field("customerid", "BIGINT"));

        assertNotEquals(upperCaseColumn.structureFingerprint(), lowerCaseColumn.structureFingerprint());
    }

    private static EntityMetadata<Object> metadata(String table) {
        return metadata(table, new EntityFieldMetadata[0]);
    }

    private static EntityMetadata<Object> metadata(String table, EntityFieldMetadata... fields) {
        return EntityMetadata.create(
                Object.class,
                "customer-form",
                com.flying.orm.core.metadata.RelationIdentity.table(table),
                table,
                List.of(fields),
                null,
                TenantStrategy.NONE,
                FieldProtectionRegistry.empty(), java.util.Map.of());
    }

    private static EntityFieldMetadata field(String columnName, String dataType) {
        return field(ColumnDefinition.builder(columnName, dataType).build());
    }

    private static EntityFieldMetadata field(ColumnDefinition column) {
        return new EntityFieldMetadata(
                column.name(), Object.class, new DynamicField(column, false, false), null,
                false, false, null, null, IdType.NONE,
                EntityEnumStorage.NONE, null,
                true, false, true, 0,
                FieldFill.DEFAULT, FieldStrategy.DEFAULT, FieldStrategy.DEFAULT,
                true, true);
    }
}
