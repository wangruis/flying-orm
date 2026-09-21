package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntitySchemaDescriptorImmutabilityTest {

    @Test
    void publishesSnapshotsThatAreIsolatedFromLaterBuilderChanges() {
        var builder = EntitySchemaDescriptor.builder(BuilderEntity.class)
                .primaryKey(PrimaryKeyDefinition.of("pk_builder_entities", "id"))
                .unique("business-key", UniqueConstraintDefinition.of(
                        "uq_builder_business", "tenant_code", "external_code"))
                .index("tenant-index", IndexDefinition.builder("idx_builder_tenant")
                        .addKey(IndexKeyPart.asc("tenant_code"))
                        .build())
                .foreignKey("tenant-foreign-key", foreignKey(
                        "fk_builder_tenant", "tenant_code", "code"))
                .check("external-code-check", CheckConstraintDefinition.of(
                        "ck_builder_external_code", CheckPredicate.isNotNull("external_code")));

        EntitySchemaDescriptor<BuilderEntity> first = builder.build();
        String firstFingerprint = first.relationalFingerprint();

        builder.unique("tenant-key", UniqueConstraintDefinition.of(
                       "uq_builder_tenant", "tenant_code"))
               .index("external-index", IndexDefinition.builder("idx_builder_external")
                       .addKey(IndexKeyPart.desc("external_code"))
                       .build())
               .foreignKey("external-foreign-key", foreignKey(
                       "fk_builder_external", "external_code", "external_code"))
               .check("tenant-check", CheckConstraintDefinition.of(
                       "ck_builder_tenant", CheckPredicate.isNotNull("tenant_code")));
        EntitySchemaDescriptor<BuilderEntity> second = builder.build();

        assertEquals(firstFingerprint, first.relationalFingerprint());
        assertEquals(1, first.table().uniqueConstraints().size());
        assertEquals(1, first.table().indexes().size());
        assertEquals(1, first.table().foreignKeys().size());
        assertEquals(1, first.table().checks().size());
        assertEquals(2, second.table().uniqueConstraints().size());
        assertEquals(2, second.table().indexes().size());
        assertEquals(2, second.table().foreignKeys().size());
        assertEquals(2, second.table().checks().size());

        assertThrows(UnsupportedOperationException.class, () -> first.table().columns().clear());
        assertThrows(UnsupportedOperationException.class, () -> first.table().uniqueConstraints().clear());
        assertThrows(UnsupportedOperationException.class, () -> first.table().indexes().clear());
        assertThrows(UnsupportedOperationException.class, () -> first.table().foreignKeys().clear());
        assertThrows(UnsupportedOperationException.class, () -> first.table().checks().clear());
        assertThrows(UnsupportedOperationException.class, () -> first.schema().tables().clear());
    }

    private static ForeignKeyDefinition foreignKey(String name, String localColumn, String targetColumn) {
        return ForeignKeyDefinition.builder(name)
                .addColumn(localColumn)
                .referencedTable(RelationIdentity.table("tenants"))
                .addReferenceColumn(targetColumn)
                .build();
    }

    @TableName("builder_entities")
    private static final class BuilderEntity {

        @TableId(type = IdType.INPUT)
        private Long id;

        @TableField("tenant_code")
        private String tenantCode;

        @TableField("external_code")
        private String externalCode;
    }
}
