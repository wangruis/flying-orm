package com.flying.orm.rdb.mapping;

import com.flying.orm.rdb.internal.mapping.EntityMetadataResolver;
import com.flying.orm.rdb.internal.mapping.EntityValues;

import com.flying.orm.core.annotation.EnumValue;
import com.flying.orm.core.annotation.EncryptedField;
import com.flying.orm.core.annotation.FieldStrategy;
import com.flying.orm.core.annotation.FieldFill;
import com.flying.orm.core.annotation.IdType;
import com.flying.orm.core.annotation.KeySequence;
import com.flying.orm.core.annotation.MaskedField;
import com.flying.orm.core.annotation.TableField;
import com.flying.orm.core.annotation.TableId;
import com.flying.orm.core.annotation.TableLogic;
import com.flying.orm.core.annotation.TableName;
import com.flying.orm.core.annotation.Version;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDateTime;
import java.time.OffsetTime;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 实体约定模型要让业务 service 只继承就能用，表名和字段名优先从标准 JPA 注解读取。
 *
 * @author wangr
 * @date 2026-07-30
 * @version v1.0
 */
class EntityMetadataResolverTest {

    /** 实体字段查找失败不能把运行时提供的任意字段名写进异常消息。 */
    @Test
    void missingEntityFieldFailureDoesNotExposeLookupInput() {
        EntityMetadata<?> metadata = EntityMetadataResolver.createUncached(UserEntity.class);

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> metadata.field("secret-runtime-field"));

        assertEquals("entity field does not exist", error.getMessage());
    }

    @Test
    void readsFlyingTableColumnIdAndVersion() {
        EntityMetadata<?> metadata = EntityMetadataResolver.createUncached(UserEntity.class);

        assertEquals("sys_user", metadata.table());
        assertEquals("user_entity", metadata.formId());
        assertEquals("id", metadata.idField().orElseThrow().name());
        assertEquals("version", metadata.versionField().orElseThrow().name());
        assertEquals("deleted", metadata.logicDeleteField().orElseThrow().name());
        assertEquals(0, metadata.logicDeleteField().orElseThrow().logicNotDeletedValue());
        assertEquals(1, metadata.logicDeleteField().orElseThrow().logicDeletedValue());
        assertEquals("user_name", metadata.field("name").columnName());
        assertEquals("BIGINT", metadata.field("id").dataType());
        assertEquals("VARCHAR", metadata.field("name").dataType());
        assertEquals("BOOLEAN", metadata.field("enabled").dataType());
        assertEquals("DECIMAL", metadata.field("balance").dataType());
        assertEquals("TIMESTAMP", metadata.field("createdAt").dataType());
    }

    /** 实体 OffsetTime 必须进入跨方言 OFFSET_TIME codec，不能退化成普通 VARCHAR。 */
    @Test
    void infersOffsetTimeAsTheOffsetPreservingLogicalType() {
        EntityMetadata<?> metadata = EntityMetadataResolver.createUncached(OffsetTimeEntity.class);

        assertEquals("OFFSET_TIME", metadata.field("meetingTime").dataType());
    }

    /** BigInteger 是精确数值，不能退化成可能接收不到数值参数的 VARCHAR。 */
    @Test
    void infersBigIntegerAsAnExactDecimalType() {
        EntityMetadata<?> metadata = EntityMetadataResolver.createUncached(BigIntegerEntity.class);

        assertEquals("DECIMAL", metadata.field("amount").dataType());
    }

    @Test
    void fallsBackToClassAndFieldNamingConventions() {
        EntityMetadata<?> metadata = EntityMetadataResolver.createUncached(AccountLog.class);

        assertEquals("account_log", metadata.table());
        assertEquals("account_log", metadata.formId());
        assertEquals("created_at", metadata.field("createdAt").columnName());
    }

    @Test
    void readsClassLevelLogicDeleteDeclaration() {
        EntityMetadata<?> metadata = EntityMetadataResolver.createUncached(ClassLevelLogicEntity.class);

        assertEquals("removed", metadata.logicDeleteField().orElseThrow().name());
        assertEquals("N", metadata.logicDeleteField().orElseThrow().logicNotDeletedValue());
        assertEquals("Y", metadata.logicDeleteField().orElseThrow().logicDeletedValue());
    }

    @Test
    void convertsEntityMetadataToDynamicForm() {
        DynamicForm form = EntityMetadataResolver.createUncached(UserEntity.class).toDynamicForm();

        assertEquals("user_entity", form.id());
        assertEquals("sys_user", form.table());
        assertTrue(form.field("id").primaryKey());
        assertEquals("VARCHAR", form.field("user_name").dataType());
        assertEquals("deleted", form.logicDelete().orElseThrow().fieldName());
        assertEquals(0, form.logicDelete().orElseThrow().notDeletedValue());
        assertEquals(1, form.logicDelete().orElseThrow().deletedValue());
    }

    /** 实体注解与 DynamicForm 显式声明必须编译成同一份保护元数据。 */
    @Test
    void compilesExplicitFieldProtectionAnnotationsIntoDynamicForm() {
        DynamicForm form = EntityMetadataResolver.createUncached(ProtectedCustomer.class).toDynamicForm();

        assertEquals("digits", form.protections().encrypted("contact_value").orElseThrow().normalizer());
        assertEquals(List.of(4), form.protections().encrypted("contact_value").orElseThrow().suffixLengths());
        assertEquals(SensitiveDisplayMode.FULL,
                     form.protections().masked("contact_value").orElseThrow().display());
        assertFalse(form.protections().protectedField("nickname"));
    }

    /** 加密列不能承担需要数据库原子比较和递增的实体版本职责。 */
    @Test
    void rejectsEncryptedOptimisticLockVersionAtMetadataCompilation() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> EntityMetadataResolver.createUncached(EncryptedVersionEntity.class));

        assertEquals("entity version field must not be encrypted", error.getMessage());
    }

    /** 脱敏后的版本值不能再作为下一次乐观锁比较使用，必须在元数据编译阶段拒绝这种矛盾声明。 */
    @Test
    void rejectsMaskedOptimisticLockVersionAtMetadataCompilation() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> EntityMetadataResolver.createUncached(MaskedVersionEntity.class));

        assertEquals("entity version field must not be masked", error.getMessage());
    }

    @Test
    void readsFlyingGenerationEnumAndJavaTypeSemantics() {
        EntityMetadata<?> metadata = EntityMetadataResolver.createUncached(GeneratedDocument.class);

        assertEquals("SEQUENCE", metadata.field("id").generation().strategy().name());
        assertEquals("document_id_seq", metadata.field("id").generation().sequenceName());
        assertEquals("VARCHAR", metadata.field("state").dataType());
        assertEquals(EntityEnumStorage.NAME, metadata.field("state").enumStorage());
        assertEquals("VARCHAR", metadata.field("content").dataType());
        assertEquals("BINARY", metadata.field("payload").dataType());
        assertTrue(metadata.fields().stream().noneMatch(field -> field.name().equals("displayName")));
    }

    @Test
    void omitsDatabaseGeneratedPrimaryKeysFromInsertValues() {
        EntityValues<IdentityDevice> identityValues = EntityValues.createUncached(IdentityDevice.class);
        Map<String, Object> identityRow = identityValues.readForInsert(new IdentityDevice(null, "thermometer"));

        assertEquals(Map.of("device_name", "thermometer"), identityRow);

        EntityValues<GeneratedDocument> sequenceValues = EntityValues.createUncached(GeneratedDocument.class);
        assertFalse(sequenceValues.readForInsert(new GeneratedDocument()).containsKey("id"));
    }

    @Test
    void appliesExplicitInsertAndUpdateValueStrategies() {
        EntityValues<StrategyEntity> values = EntityValues.createUncached(StrategyEntity.class);
        StrategyEntity entity = new StrategyEntity(null, "", "kept");

        assertEquals(List.of("always_value"), List.copyOf(values.readForInsert(entity).keySet()));
        assertEquals(List.of("always_value"), List.copyOf(values.readForUpdate(entity).keySet()));
    }

    @Test
    void fillsOnlyTheDeclaredWriteOperations() {
        EntityMetadata<FillEntity> metadata = EntityMetadataResolver.createUncached(FillEntity.class);
        EntityValues<FillEntity> configured = EntityValues.createUncached(
                FillEntity.class, metadata,
                (entity, field, operation, current) -> operation.name() + "-" + field.name());

        assertEquals("INSERT-createdAt", configured.readForInsert(new FillEntity()).get("created_at"));
        assertEquals("UPDATE-updatedAt", configured.readForUpdate(new FillEntity()).get("updated_at"));
        assertThrows(MappingException.class,
                     () -> EntityValues.createUncached(FillEntity.class).readForInsert(new FillEntity()));
    }

    /** 字段填充扩展包装的 JVM 致命错误必须保持原对象，不能降级成普通映射失败。 */
    @Test
    void preservesNestedVirtualMachineErrorFromEntityFieldFiller() {
        OutOfMemoryError fatal = new OutOfMemoryError("field filler fatal");
        EntityMetadata<FillEntity> metadata = EntityMetadataResolver.createUncached(FillEntity.class);
        EntityValues<FillEntity> values = EntityValues.createUncached(
                FillEntity.class, metadata,
                (entity, field, operation, current) -> {
                    throw new IllegalStateException("filler wrapper", fatal);
                });

        Throwable observed = assertThrows(Throwable.class,
                                          () -> values.readForInsert(new FillEntity()));

        assertSame(fatal, observed);
    }

    @Test
    void inheritsJavaParentFieldsAndReadsFieldStrategies() {
        EntityMetadata<ManagedDocument> metadata = EntityMetadataResolver.createUncached(ManagedDocument.class);

        // 父类字段排在子类字段前，单行和批量参数都能长期保持同一顺序。
        assertEquals(List.of("createdBy", "id", "title", "computedLabel"),
                     metadata.fields().stream().map(EntityFieldMetadata::name).toList());
        EntityFieldMetadata createdBy = metadata.field("createdBy");
        assertFalse(createdBy.insertable());
        assertFalse(createdBy.updatable());
        assertFalse(createdBy.selectable());
        assertEquals(FieldStrategy.NEVER, createdBy.insertStrategy());
        assertEquals(FieldStrategy.NEVER, createdBy.updateStrategy());

        EntityValues<ManagedDocument> values = EntityValues.createUncached(ManagedDocument.class);
        ManagedDocument document = new ManagedDocument("system", 7L, "guide", "GUIDE");
        assertEquals(List.of("id", "title"), List.copyOf(values.readForInsert(document).keySet()));
        assertEquals(List.of("title"), List.copyOf(values.readForUpdate(document).keySet()));
        assertEquals(List.of("id", "title"), List.copyOf(values.readForUpsert(document).keySet()));
    }

    @Test
    void generatesFailClosedTenantDefinitionAndRejectsAmbiguousDeclarations() {
        DynamicForm form = EntityMetadataResolver.createUncached(TenantEntity.class).toDynamicForm();

        assertEquals("tenant_id", form.tenant().orElseThrow().fieldName());
        assertEquals(TenantStrategy.AUTO, form.tenant().orElseThrow().strategy());
        assertThrows(MappingException.class,
                     () -> EntityMetadataResolver.createUncached(DuplicateTenantEntity.class));
        assertThrows(MappingException.class,
                     () -> EntityMetadataResolver.createUncached(MissingTenantEntity.class));
    }

    @Test
    void fingerprintsSharedTablesByPhysicalStructure() {
        EntityMetadata<?> first = EntityMetadataResolver.createUncached(SharedTableEntity.class);
        EntityMetadata<?> identical = EntityMetadataResolver.createUncached(IdenticalSharedTableEntity.class);
        EntityMetadata<?> reordered = EntityMetadataResolver.createUncached(ReorderedSharedTableEntity.class);
        EntityMetadata<?> divergent = EntityMetadataResolver.createUncached(DivergentSharedTableEntity.class);

        assertEquals(first.structureFingerprint(), identical.structureFingerprint());
        assertEquals(first.structureFingerprint(), reordered.structureFingerprint());
        assertFalse(first.structureFingerprint().equals(divergent.structureFingerprint()));
    }

    @Test
    void readsFlyingAnnotationsWithoutJakartaFallback() {
        EntityMetadata<?> metadata = EntityMetadataResolver.createUncached(FlyingDevice.class);

        assertEquals("iot.device", metadata.table());
        assertEquals("device_id", metadata.idField().orElseThrow().columnName());
        assertEquals(IdType.AUTO, metadata.idField().orElseThrow().idType());
        assertEquals("IDENTITY", metadata.idField().orElseThrow().generation().strategy().name());
        assertEquals("revision", metadata.versionField().orElseThrow().columnName());
        assertEquals("deleted", metadata.logicDeleteField().orElseThrow().columnName());
        assertEquals(false, metadata.logicDeleteField().orElseThrow().logicNotDeletedValue());
        assertEquals(true, metadata.logicDeleteField().orElseThrow().logicDeletedValue());
        assertEquals("status_code", metadata.field("status").columnName());
        assertEquals("INTEGER", metadata.field("status").dataType());
        assertEquals("code", metadata.field("status").enumValueMember());
        assertEquals(EntityEnumStorage.NONE, metadata.field("status").enumStorage());
        assertTrue(metadata.fields().stream().noneMatch(field -> field.name().equals("displayOnly")));
    }

    @Test
    void keepsNoneAsTheSafeDefaultAndLetsKeySequenceOverrideLegacyGeneration() {
        EntityMetadata<?> inputId = EntityMetadataResolver.createUncached(FlyingInputId.class);
        assertEquals(IdType.NONE, inputId.idField().orElseThrow().idType());
        assertEquals("NONE", inputId.idField().orElseThrow().generation().strategy().name());

        EntityMetadata<?> sequenceId = EntityMetadataResolver.createUncached(FlyingSequenceId.class);
        assertEquals(IdType.NONE, sequenceId.idField().orElseThrow().idType());
        assertEquals("SEQUENCE", sequenceId.idField().orElseThrow().generation().strategy().name());
        assertEquals("flying_device_seq", sequenceId.idField().orElseThrow().generation().sequenceName());
    }

    @TableName("sys_user")
    private static final class UserEntity {

        @TableId
        private Long id;

        @TableField("user_name")
        private String name;

        private Boolean enabled;

        private BigDecimal balance;

        private LocalDateTime createdAt;

        @Version
        private Long version;

        @FlyingLogicDelete(notDeletedValue = "0", deletedValue = "1")
        private Integer deleted;
    }

    @TableName(value = "device", schema = "iot")
    private static final class FlyingDevice {

        @TableId(value = "device_id", type = IdType.AUTO)
        private Long id;

        @Version
        @TableField("revision")
        private Long revision;

        @TableLogic(value = "false", delval = "true")
        private Boolean deleted;

        @TableField("status_code")
        private DeviceStatus status;

        @TableField(exist = false)
        private String displayOnly;
    }

    @TableName("flying_input")
    private static final class FlyingInputId {

        @TableId
        private Long id;
    }

    @TableName("flying_sequence")
    @KeySequence("flying_device_seq")
    private static final class FlyingSequenceId {

        @TableId
        private Long id;
    }

    @TableName("shared_table")
    private static final class SharedTableEntity {
        @TableId
        private Long id;

        @TableField
        private String code;
    }

    @TableName("shared_table")
    private static final class IdenticalSharedTableEntity {
        @TableId
        private Long id;

        @TableField
        private String code;
    }

    @TableName("shared_table")
    private static final class ReorderedSharedTableEntity {
        @TableField
        private String code;

        @TableId
        private Long id;
    }

    @TableName("shared_table")
    private static final class DivergentSharedTableEntity {
        @TableId
        private Long id;

        private String description;
    }

    private static final class AccountLog {

        private Long id;

        private LocalDateTime createdAt;
    }

    private static final class OffsetTimeEntity {

        private OffsetTime meetingTime;
    }

    private static final class BigIntegerEntity {

        private BigInteger amount;
    }

    @TableName("tenant_entity")
    private static final class TenantEntity {
        private Long id;
        @FlyingTenant
        @TableField("tenant_id")
        private String tenantId;
    }

    @TableName("duplicate_tenant_entity")
    private static final class DuplicateTenantEntity {
        @FlyingTenant
        private String tenantId;
        @FlyingTenant
        private String organizationId;
    }

    @TableName("missing_tenant_entity")
    @FlyingTenant(field = "tenantId")
    private static final class MissingTenantEntity {
        private Long id;
    }

    @FlyingLogicDelete(field = "removed", notDeletedValue = "N", deletedValue = "Y")
    private static final class ClassLevelLogicEntity {

        private Long id;

        private String removed;
    }

    @TableName("document")
    @KeySequence("document_id_seq")
    private static final class GeneratedDocument {

        @TableId
        private Long id;

        private DocumentState state;

        private String content;

        private byte[] payload;

        private transient String displayName;
    }

    @TableName("device")
    private static final class IdentityDevice {

        @TableId(value = "device_id", type = IdType.AUTO)
        private Long deviceId;

        @TableField("device_name")
        private String name;

        private IdentityDevice(Long deviceId, String name) {
            this.deviceId = deviceId;
            this.name = name;
        }
    }

    private static final class StrategyEntity {
        @TableField(value = "nullable_value", insertStrategy = FieldStrategy.NOT_NULL,
                updateStrategy = FieldStrategy.NOT_NULL)
        private String nullable;

        @TableField(value = "empty_value", insertStrategy = FieldStrategy.NOT_EMPTY,
                updateStrategy = FieldStrategy.NOT_EMPTY)
        private String empty;

        @TableField(value = "always_value", insertStrategy = FieldStrategy.ALWAYS,
                updateStrategy = FieldStrategy.ALWAYS)
        private String always;

        private StrategyEntity(String nullable, String empty, String always) {
            this.nullable = nullable;
            this.empty = empty;
            this.always = always;
        }
    }

    private static final class FillEntity {
        @TableField(value = "created_at", fill = FieldFill.INSERT)
        private String createdAt;

        @TableField(value = "updated_at", fill = FieldFill.UPDATE)
        private String updatedAt;
    }

    private enum DocumentState {
        DRAFT,
        PUBLISHED
    }

    private enum DeviceStatus {
        ACTIVE(1),
        INACTIVE(0);

        @EnumValue
        private final int code;

        DeviceStatus(int code) {
            this.code = code;
        }
    }

    private static class ManagedFields {

        @TableField(value = "created_by", select = false,
                insertStrategy = FieldStrategy.NEVER, updateStrategy = FieldStrategy.NEVER)
        private String createdBy;

        private ManagedFields(String createdBy) {
            this.createdBy = createdBy;
        }
    }

    @TableName("managed_document")
    private static final class ManagedDocument extends ManagedFields {

        @TableId
        private Long id;

        private String title;

        @TableField(value = "computed_label",
                insertStrategy = FieldStrategy.NEVER, updateStrategy = FieldStrategy.NEVER)
        private String computedLabel;

        private ManagedDocument(String createdBy, Long id, String title, String computedLabel) {
            super(createdBy);
            this.id = id;
            this.title = title;
            this.computedLabel = computedLabel;
        }
    }

    @TableName("protected_customer")
    private static final class ProtectedCustomer {

        @TableId
        private Long id;

        @TableField("contact_value")
        @EncryptedField(search = {EncryptedSearchMode.EXACT, EncryptedSearchMode.SUFFIX},
                normalizer = "digits", suffixLengths = 4, maxNormalizedLength = 64)
        @MaskedField(policy = "partial", prefix = 3, suffix = 4, display = SensitiveDisplayMode.FULL)
        private String contactValue;

        private String nickname;
    }

    private static final class EncryptedVersionEntity {

        @TableId
        private Long id;

        @Version
        @EncryptedField
        private Long version;
    }

    private static final class MaskedVersionEntity {

        @TableId
        private Long id;

        @Version
        @MaskedField
        private String version;
    }
}
