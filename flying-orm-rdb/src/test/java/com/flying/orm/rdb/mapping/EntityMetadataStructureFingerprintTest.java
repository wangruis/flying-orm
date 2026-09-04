package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.FieldFill;
import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.metadata.ValueGeneration;
import com.flying.orm.core.protection.FieldProtectionRegistry;
import com.flying.orm.core.type.DatabaseType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNotEquals;

class EntityMetadataStructureFingerprintTest {

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
                table,
                List.of(fields),
                null,
                TenantStrategy.NONE,
                FieldProtectionRegistry.empty());
    }

    private static EntityFieldMetadata field(String columnName, String dataType) {
        return new EntityFieldMetadata(
                "value", columnName, DatabaseType.of(dataType),
                false, false, false, null, null,
                null, null, null, ValueGeneration.none(), IdType.NONE,
                EntityEnumStorage.NONE, null,
                true, false, true, 0,
                FieldFill.DEFAULT, FieldStrategy.DEFAULT, FieldStrategy.DEFAULT,
                true, true, true, false);
    }
}
