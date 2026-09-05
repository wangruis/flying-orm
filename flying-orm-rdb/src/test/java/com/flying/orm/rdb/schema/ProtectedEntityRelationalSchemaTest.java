package com.flying.orm.rdb.schema;

import com.flying.orm.core.annotation.EncryptedField;
import com.flying.orm.core.annotation.TableColumn;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableIndex;
import com.flying.orm.core.annotation.TableIndexColumn;
import com.flying.orm.core.annotation.TableForeignKey;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.TablePrimaryKey;
import com.flying.orm.core.annotation.TableUnique;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.ReferentialAction;
import com.flying.orm.core.metadata.RelationalMetadataFingerprint;
import com.flying.orm.core.metadata.RelationalSchemaDefinition;
import com.flying.orm.core.metadata.RelationalSchemaFingerprint;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.rdb.mapping.EntitySchemaDescriptor;
import com.flying.orm.rdb.mapping.EntityModelRegistry;
import com.flying.orm.rdb.mapping.MappingException;
import com.flying.orm.rdb.cache.CacheRegionPolicy;
import com.flying.orm.rdb.dialect.DatabaseDescriptor;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.protection.ProtectedContainsLayout;
import com.flying.orm.rdb.protection.ProtectedFormLayout;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtectedEntityRelationalSchemaTest {

    @Test
    void entitySchemaPublishesTheSamePhysicalOwnerRequiredByProtectedCrud() {
        EntitySchemaDescriptor<ProtectedAccount> descriptor =
                EntitySchemaDescriptor.builder(ProtectedAccount.class).build();
        var protectedCrudLayout = ProtectedFormLayout.physical(descriptor.form());

        assertEquals(
                protectedCrudLayout.fields().stream()
                        .map(field -> field.name() + ':' + field.databaseType().canonical())
                        .toList(),
                descriptor.table().columns().stream()
                        .map(column -> column.name() + ':' + column.databaseType().canonical())
                        .toList());

        String exactColumn = protectedCrudLayout.fields().stream()
                .filter(field -> ProtectedFormLayout.isHashType(field.databaseType()))
                .filter(field -> !field.nullable())
                .findFirst().orElseThrow().name();
        assertEquals(exactColumn,
                descriptor.table().uniqueConstraints().getFirst().columns().getFirst());
        assertEquals(exactColumn,
                descriptor.table().indexes().getFirst().keys().getFirst().column());
        assertEquals(IndexKeyPart.Direction.DESC,
                descriptor.table().indexes().getFirst().keys().getFirst().direction());
        assertEquals("uq_protected_mobile",
                descriptor.table().uniqueConstraints().getFirst().name());
        assertEquals("idx_protected_mobile",
                descriptor.table().indexes().getFirst().name());
    }

    @Test
    void containsPublishesOneCanonicalAuxiliaryRelation() {
        EntitySchemaDescriptor<ContainsAccount> descriptor =
                EntitySchemaDescriptor.builder(ContainsAccount.class).build();
        var legacy = ProtectedContainsLayout.resolve(descriptor.form()).orElseThrow();

        assertEquals(2, descriptor.schema().tables().size());
        assertSame(descriptor.table(), descriptor.schema().tables().getFirst());
        var side = descriptor.schema().tables().get(1);
        assertEquals(legacy.table().relationIdentity().orElseThrow(), side.identity());
        assertEquals(legacy.table().fields().stream().map(field -> field.name()).toList(),
                side.columns().stream().map(column -> column.name()).toList());
        assertEquals(legacy.indexes().stream().map(index -> index.name()).toList(),
                side.indexes().stream().map(index -> index.name()).toList());
        assertEquals(1, side.foreignKeys().size());
        assertEquals(ReferentialAction.CASCADE, side.foreignKeys().getFirst().onDelete());
        assertEquals(descriptor.table().identity(), side.foreignKeys().getFirst().reference());
    }

    @Test
    void containsProtocolColumnsKeepTheirTypesWhenTheOwnerHasSameNamedBusinessColumns() {
        EntitySchemaDescriptor<ContainsProtocolNameCollisionAccount> descriptor =
                EntitySchemaDescriptor.builder(ContainsProtocolNameCollisionAccount.class).build();
        var legacy = ProtectedContainsLayout.resolve(descriptor.form()).orElseThrow();
        var side = descriptor.schema().tables().get(1);

        assertEquals(com.flying.orm.core.type.DatabaseType.of("BIGINT"),
                descriptor.table().findColumn("field_tag").orElseThrow().databaseType());
        assertEquals(com.flying.orm.core.type.DatabaseType.of("BIGINT"),
                descriptor.table().findColumn("token_hash").orElseThrow().databaseType());
        assertEquals(legacy.table().field("field_tag").databaseType(),
                side.findColumn("field_tag").orElseThrow().databaseType());
        assertEquals(legacy.table().field("field_tag").length(),
                side.findColumn("field_tag").orElseThrow().length());
        assertEquals(legacy.table().field("token_hash").databaseType(),
                side.findColumn("token_hash").orElseThrow().databaseType());
    }

    @Test
    void ordinaryEntityKeepsItsTableAndFingerprintContract() {
        EntitySchemaDescriptor<OrdinaryAccount> descriptor =
                EntitySchemaDescriptor.builder(OrdinaryAccount.class).build();

        assertEquals(1, descriptor.schema().tables().size());
        assertSame(descriptor.table(), descriptor.schema().tables().getFirst());
        assertEquals(RelationalMetadataFingerprint.of(descriptor.table()),
                descriptor.relationalFingerprint());
        assertEquals(descriptor.relationalFingerprint(),
                RelationalSchemaFingerprint.of(descriptor.schema()));
        assertFalse(descriptor.form().protections().protectedField("name"));
    }

    @Test
    void multiRelationFingerprintDoesNotDependOnCollectionOrder() {
        EntitySchemaDescriptor<ContainsAccount> descriptor =
                EntitySchemaDescriptor.builder(ContainsAccount.class).build();
        var reversed = RelationalSchemaDefinition.of(java.util.List.of(
                descriptor.schema().tables().get(1), descriptor.schema().tables().getFirst()));

        assertEquals(descriptor.relationalFingerprint(),
                RelationalSchemaFingerprint.of(reversed));
    }

    @Test
    void relationalPlanningIncludesContainsAfterItsOwner() {
        EntitySchemaDescriptor<ContainsAccount> descriptor =
                EntitySchemaDescriptor.builder(ContainsAccount.class).build();
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntitySchemaSynchronizer synchronizer = new EntitySchemaSynchronizer(
                    models, null, null, null, null);

            MultiTableSchemaPlanner.Plan plan = synchronizer.plan(
                    DatabaseDescriptor.of("H2", "2.3", RdbDialect.h2()),
                    java.util.List.of(ContainsAccount.class),
                    MultiTableSchemaPlanner.ForeignKeyCycleSupport.MANUAL_REQUIRED);

            assertEquals(descriptor.schema().tables().stream()
                            .map(table -> table.identity()).toList(),
                    plan.firstPhase().stream().map(operation -> operation.relation()).toList());
        }
    }

    @Test
    void relationalPlanningRejectsForeignKeysToManagedProtectedColumns() {
        try (EntityModelRegistry models = EntityModelRegistry.create(CacheRegionPolicy.disabled())) {
            EntitySchemaSynchronizer synchronizer = new EntitySchemaSynchronizer(
                    models, null, null, null, null);

            assertThrows(MappingException.class, () -> synchronizer.plan(
                    DatabaseDescriptor.of("H2", "2.3", RdbDialect.h2()),
                    java.util.List.of(ProtectedTarget.class, ProtectedReference.class),
                    MultiTableSchemaPlanner.ForeignKeyCycleSupport.MANUAL_REQUIRED));
        }
    }

    @TableName("protected_accounts")
    @TablePrimaryKey(name = "pk_protected_accounts", properties = "id")
    @TableUnique(id = "mobile-unique", name = "uq_protected_mobile", properties = "mobile")
    @TableIndex(id = "mobile-index", name = "idx_protected_mobile",
            columns = @TableIndexColumn(property = "mobile", direction = TableIndexColumn.Direction.DESC))
    private static final class ProtectedAccount {

        @TableId
        @TableColumn(databaseTypeId = "BIGINT", nullable = TableColumn.Nullability.NOT_NULL)
        private Long id;

        @EncryptedField(search = {EncryptedSearchMode.EXACT, EncryptedSearchMode.SUFFIX},
                suffixLengths = 4)
        @TableColumn(databaseTypeId = "VARCHAR", length = 64,
                nullable = TableColumn.Nullability.NOT_NULL, comment = "mobile")
        private String mobile;
    }

    @TableName(value = "contains_accounts", schema = "security")
    @TablePrimaryKey(name = "pk_contains_accounts", properties = "id")
    private static final class ContainsAccount {

        @TableId
        @TableColumn(databaseTypeId = "BIGINT", nullable = TableColumn.Nullability.NOT_NULL)
        private Long id;

        @EncryptedField(search = {EncryptedSearchMode.EXACT, EncryptedSearchMode.CONTAINS})
        @TableColumn(databaseTypeId = "VARCHAR", length = 128)
        private String email;
    }

    @TableName(value = "contains_protocol_collision_accounts", schema = "security")
    @TablePrimaryKey(name = "pk_contains_protocol_collision_accounts", properties = "id")
    private static final class ContainsProtocolNameCollisionAccount {

        @TableId
        @TableColumn(databaseTypeId = "BIGINT", nullable = TableColumn.Nullability.NOT_NULL)
        private Long id;

        @TableField("field_tag")
        @TableColumn(databaseTypeId = "BIGINT")
        private Long businessFieldTag;

        @TableField("token_hash")
        @TableColumn(databaseTypeId = "BIGINT")
        private Long businessTokenHash;

        @EncryptedField(search = EncryptedSearchMode.CONTAINS)
        @TableColumn(databaseTypeId = "VARCHAR", length = 128)
        private String email;
    }

    @TableName("ordinary_accounts")
    private static final class OrdinaryAccount {

        @TableId
        private Long id;

        private String name;
    }

    @TableName(value = "protected_targets", schema = "security")
    @TableUnique(id = "email", name = "uq_protected_target_email", properties = "email")
    private static final class ProtectedTarget {
        @TableId
        private Long id;
        @EncryptedField
        private String email;
    }

    @TableName(value = "protected_references", schema = "security")
    @TableForeignKey(id = "target-email", name = "fk_protected_reference_email",
            localProperties = "targetEmail", targetEntity = ProtectedTarget.class,
            targetProperties = "email")
    private static final class ProtectedReference {
        @TableId
        private Long id;
        private String targetEmail;
    }
}
