package com.flying.orm.rdb.mapping;

import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.TableCheck;
import com.flying.orm.core.annotation.TableColumn;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableForeignKey;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableIndex;
import com.flying.orm.core.annotation.TableIndexColumn;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.TablePrimaryKey;
import com.flying.orm.core.annotation.TableUnique;
import com.flying.orm.core.metadata.CheckConstraintDefinition;
import com.flying.orm.core.metadata.CheckPredicate;
import com.flying.orm.core.metadata.ForeignKeyDefinition;
import com.flying.orm.core.metadata.IndexDefinition;
import com.flying.orm.core.metadata.IndexKeyPart;
import com.flying.orm.core.metadata.PrimaryKeyDefinition;
import com.flying.orm.core.metadata.ReferentialAction;
import com.flying.orm.core.metadata.RelationIdentity;
import com.flying.orm.core.metadata.RelationalTableDefinition;
import com.flying.orm.core.metadata.UniqueConstraintDefinition;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class EntityAnnotationConflictFailClosedTest {

    private static final String ACCOUNT_NUMBER_ID = "account-number";
    private static final String ACCOUNT_STATUS_ID = "account-status";
    private static final String ACCOUNT_TENANT_ID = "account-tenant";
    private static final String ACCOUNT_ENABLED_ID = "account-enabled";

    @Test
    void rejectsColumnMetadataOnTableFieldExcludedProperty() {
        assertThrows(MappingException.class,
                     () -> EntitySchemaDescriptor.builder(ExcludedColumnEntity.class).build());
    }

    @Test
    void rejectsColumnMetadataOnTransientProperty() {
        assertThrows(MappingException.class,
                     () -> EntitySchemaDescriptor.builder(TransientColumnEntity.class).build());
    }

    @Test
    void rejectsPrimaryKeyPropertiesThatDoNotMatchTableIds() {
        assertThrows(MappingException.class,
                     () -> EntitySchemaDescriptor.builder(MismatchedPrimaryKeyEntity.class).build());
    }

    @Test
    void rejectsConstraintThatReferencesUnknownProperty() {
        assertThrows(MappingException.class,
                     () -> EntitySchemaDescriptor.builder(UnknownConstraintPropertyEntity.class).build());
    }

    @Test
    void rejectsDifferentStructureForTheSameStableId() {
        UniqueConstraintDefinition conflicting = UniqueConstraintDefinition.of(
                "uk_accounts_account_number", "tenant_key");

        assertThrows(MappingException.class,
                     () -> EntitySchemaDescriptor.builder(MergedAccount.class)
                                                   .unique(ACCOUNT_NUMBER_ID, conflicting)
                                                   .build());
    }

    @Test
    void deduplicatesIdenticalAnnotatedAndProgrammaticConstraints() {
        EntitySchemaDescriptor<MergedAccount> descriptor = EntitySchemaDescriptor.builder(MergedAccount.class)
                .primaryKey(accountPrimaryKey())
                .unique(ACCOUNT_NUMBER_ID, accountNumberUnique())
                .index(ACCOUNT_STATUS_ID, accountStatusIndex())
                .foreignKey(ACCOUNT_TENANT_ID, accountTenantForeignKey())
                .check(ACCOUNT_ENABLED_ID, accountEnabledCheck())
                .build();

        RelationalTableDefinition table = descriptor.table();
        assertEquals(accountPrimaryKey(), table.primaryKey().orElseThrow());
        assertEquals(1, table.uniqueConstraints().size());
        assertEquals(1, table.indexes().size());
        assertEquals(1, table.foreignKeys().size());
        assertEquals(1, table.checks().size());
    }

    @Test
    void rejectsConflictingStableIdAcrossEntityInheritance() {
        assertThrows(MappingException.class,
                     () -> EntitySchemaDescriptor.builder(ConflictingInheritedUnique.class).build());
    }

    @Test
    void rejectsDatabaseGenerationForNonDatabasePrimaryKeyStrategies() {
        for (Class<?> entityType : List.of(InputIdEntity.class,
                                           AssignedIdEntity.class,
                                           AssignedUuidEntity.class,
                                           PlainInputIdEntity.class)) {
            assertThrows(MappingException.class,
                         () -> EntitySchemaDescriptor.builder(entityType).build(),
                         entityType::getSimpleName);
        }
    }

    private static PrimaryKeyDefinition accountPrimaryKey() {
        return PrimaryKeyDefinition.of("pk_accounts", "account_id");
    }

    private static UniqueConstraintDefinition accountNumberUnique() {
        return UniqueConstraintDefinition.of("uk_accounts_account_number", "account_number");
    }

    private static IndexDefinition accountStatusIndex() {
        return IndexDefinition.builder("ix_accounts_status")
                              .addKey(IndexKeyPart.desc("status_code"))
                              .build();
    }

    private static ForeignKeyDefinition accountTenantForeignKey() {
        return ForeignKeyDefinition.builder("fk_accounts_tenant")
                                   .addColumn("tenant_key")
                                   .reference(RelationIdentity.of(null, "identity", "tenants"))
                                   .addReferenceColumn("tenant_key")
                                   .onUpdate(ReferentialAction.RESTRICT)
                                   .onDelete(ReferentialAction.CASCADE)
                                   .build();
    }

    private static CheckConstraintDefinition accountEnabledCheck() {
        return CheckConstraintDefinition.of(
                "ck_accounts_enabled",
                CheckPredicate.compare(
                        "enabled_flag",
                        CheckPredicate.ComparisonOperator.EQUAL,
                        true));
    }

    @TableName("excluded_column_entities")
    private static final class ExcludedColumnEntity {

        @TableId
        private Long id;

        @TableField(exist = false)
        @TableColumn(length = 64)
        private String displayName;
    }

    @TableName("transient_column_entities")
    private static final class TransientColumnEntity {

        @TableId
        private Long id;

        @TableColumn(length = 64)
        private transient String displayName;
    }

    @TableName("mismatched_primary_keys")
    @TablePrimaryKey(name = "pk_mismatched_primary_keys", properties = "tenantId")
    private static final class MismatchedPrimaryKeyEntity {

        @TableId
        private Long tenantId;

        @TableId
        private Long entityId;
    }

    @TableName("unknown_constraint_properties")
    @TableUnique(
            id = "unknown-property",
            name = "uk_unknown_constraint_property",
            properties = "missingProperty")
    private static final class UnknownConstraintPropertyEntity {

        @TableId
        private Long id;

        private String knownProperty;
    }

    @TableName(value = "accounts", schema = "app")
    @TablePrimaryKey(name = "pk_accounts", properties = "id")
    @TableUnique(
            id = ACCOUNT_NUMBER_ID,
            name = "uk_accounts_account_number",
            properties = "accountNumber")
    @TableIndex(
            id = ACCOUNT_STATUS_ID,
            name = "ix_accounts_status",
            columns = @TableIndexColumn(
                    property = "status",
                    direction = TableIndexColumn.Direction.DESC))
    @TableForeignKey(
            id = ACCOUNT_TENANT_ID,
            name = "fk_accounts_tenant",
            localProperties = "tenantId",
            targetEntity = Tenant.class,
            targetProperties = "id",
            onUpdate = ReferentialAction.RESTRICT,
            onDelete = ReferentialAction.CASCADE)
    @TableCheck(
            id = ACCOUNT_ENABLED_ID,
            name = "ck_accounts_enabled",
            property = "enabled",
            operator = TableCheck.Operator.EQUAL,
            literalValues = "true")
    private static final class MergedAccount {

        @TableId
        @TableField("account_id")
        private Long id;

        @TableField("account_number")
        private String accountNumber;

        @TableField("status_code")
        private String status;

        @TableField("tenant_key")
        private Long tenantId;

        @TableField("enabled_flag")
        private Boolean enabled;
    }

    @TableName(value = "tenants", schema = "identity")
    @TablePrimaryKey(name = "pk_tenants", properties = "id")
    private static final class Tenant {

        @TableId
        @TableField("tenant_key")
        private Long id;
    }

    @TableUnique(
            id = "inherited-code",
            name = "uk_inherited_base_code",
            properties = "baseCode")
    private static class InheritedUniqueBase {

        @TableId
        private Long id;

        private String baseCode;
    }

    @TableName("conflicting_inherited_uniques")
    @TableUnique(
            id = "inherited-code",
            name = "uk_inherited_child_code",
            properties = "childCode")
    private static final class ConflictingInheritedUnique extends InheritedUniqueBase {

        private String childCode;
    }

    @TableName("input_id_entities")
    private static final class InputIdEntity {

        @TableId(type = IdType.INPUT)
        @TableColumn(generation = TableColumn.Generation.IDENTITY)
        private Long id;
    }

    @TableName("assigned_id_entities")
    private static final class AssignedIdEntity {

        @TableId(type = IdType.ASSIGN_ID)
        @TableColumn(generation = TableColumn.Generation.IDENTITY)
        private Long id;
    }

    @TableName("assigned_uuid_entities")
    private static final class AssignedUuidEntity {

        @TableId(type = IdType.ASSIGN_UUID)
        @TableColumn(generation = TableColumn.Generation.IDENTITY)
        private String id;
    }

    @TableName("plain_input_id_entities")
    private static final class PlainInputIdEntity {

        @TableId
        @TableColumn(generation = TableColumn.Generation.IDENTITY)
        private Long id;
    }
}
