package com.flying.orm.core.form;

import com.flying.orm.core.metadata.IndexMetadata;
import com.flying.orm.core.metadata.TableMetadata;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.ReadOnlyBufferException;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

/**
 * 验证动态表单定义可以稳定映射到表元数据，并支持表结构变更计算。
 *
 * @author wangr
 * @date 2026-07-21
 * @version v1.0
 */
class DynamicFormTest {

    /**
     * 字段保护声明属于表单结构，同一组声明不能因为构建顺序或哈希碰撞而改变缓存与 Schema 指纹。
     */
    @Test
    void protectionFingerprintIsIndependentOfDeclarationOrder() {
        EncryptedFieldDefinition definition = EncryptedFieldDefinition.builder().build();
        DynamicForm first = DynamicForm.builder("first", "protected_values")
                                       .addField(DynamicField.of("an", "VARCHAR"))
                                       .addField(DynamicField.of("c0", "VARCHAR"))
                                       .encrypted("an", definition)
                                       .encrypted("c0", definition)
                                       .build();
        DynamicForm reversed = DynamicForm.builder("reversed", "protected_values")
                                          .addField(DynamicField.of("an", "VARCHAR"))
                                          .addField(DynamicField.of("c0", "VARCHAR"))
                                          .encrypted("c0", definition)
                                          .encrypted("an", definition)
                                          .build();

        assertEquals(first.structureFingerprint(), reversed.structureFingerprint());
    }

    /** 保护转换只接受具有稳定文本编码的业务字段，配置错误必须在表单发布时失败。 */
    @Test
    void rejectsNonTextEncryptedAndMaskedFieldsAtFormBoundary() {
        IllegalArgumentException encrypted = assertThrows(
                IllegalArgumentException.class,
                () -> DynamicForm.builder("encrypted", "protected_values")
                                 .addField(DynamicField.of("amount", "DECIMAL"))
                                 .encrypted("amount", EncryptedFieldDefinition.builder().build())
                                 .build());
        IllegalArgumentException masked = assertThrows(
                IllegalArgumentException.class,
                () -> DynamicForm.builder("masked", "protected_values")
                                 .addField(DynamicField.of("amount", "DECIMAL"))
                                 .masked("amount", MaskedFieldDefinition.builder("partial").build())
                                 .build());

        assertEquals("protected field must use a textual data type", encrypted.getMessage());
        assertEquals("protected field must use a textual data type", masked.getMessage());
    }

    /** SQL 文本数组不是单个文本值，不能通过子串匹配混入字段保护运行时。 */
    @Test
    void rejectsTextArrayEncryptedAndMaskedFieldsAtFormBoundary() {
        IllegalArgumentException encrypted = assertThrows(
                IllegalArgumentException.class,
                () -> DynamicForm.builder("encrypted-array", "protected_values")
                                 .addField(DynamicField.of("tags", "VARCHAR[]"))
                                 .encrypted("tags", EncryptedFieldDefinition.builder().build())
                                 .build());
        IllegalArgumentException masked = assertThrows(
                IllegalArgumentException.class,
                () -> DynamicForm.builder("masked-array", "protected_values")
                                 .addField(DynamicField.of("tags", "VARCHAR[]"))
                                 .masked("tags", MaskedFieldDefinition.builder("partial").build())
                                 .build());

        assertEquals("protected field must use a textual data type", encrypted.getMessage());
        assertEquals("protected field must use a textual data type", masked.getMessage());
    }

    /** ORM 必须直接比较主键，不能让合法发布的表单在第一次按主键查询时才因密文普通等值条件失败。 */
    @Test
    void rejectsEncryptedPrimaryKeyAtFormBoundary() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> DynamicForm.builder("encrypted-primary-key", "protected_values")
                                 .addField(DynamicField.primaryKey("id", "VARCHAR"))
                                 .encrypted("id", EncryptedFieldDefinition.builder().build())
                                 .build());

        assertEquals("encrypted field must not be an ORM control field", error.getMessage());
    }

    /** 租户 Scope 由 ORM 强制生成普通等值条件，因此加密租户字段必须在表单发布时被拒绝。 */
    @Test
    void rejectsEncryptedTenantFieldAtFormBoundary() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> DynamicForm.builder("encrypted-tenant", "protected_values")
                                 .addField(DynamicField.of("tenant_id", "VARCHAR"))
                                 .tenant("tenant_id", TenantStrategy.AUTO)
                                 .encrypted("tenant_id", EncryptedFieldDefinition.builder().build())
                                 .build());

        assertEquals("encrypted field must not be an ORM control field", error.getMessage());
    }

    /** 逻辑删除保护条件由 ORM 自动追加，禁止把其控制列配置成只能经盲索引搜索的密文字段。 */
    @Test
    void rejectsEncryptedLogicDeleteFieldAtFormBoundary() {
        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> DynamicForm.builder("encrypted-logic-delete", "protected_values")
                                 .addField(DynamicField.of("deleted", "VARCHAR"))
                                 .logicDelete("deleted", "N", "Y")
                                 .encrypted("deleted", EncryptedFieldDefinition.builder().build())
                                 .build());

        assertEquals("encrypted field must not be an ORM control field", error.getMessage());
    }

    @Test
    void structuralFingerprintIgnoresBusinessIdButTracksEverySqlRelevantFieldChange() {
        DynamicForm first = DynamicForm.builder("users-v1", "Users")
                                       .addField(DynamicField.primaryKey("id", "BIGINT"))
                                       .addField(DynamicField.of("name", "VARCHAR"))
                                       .build();
        DynamicForm equivalent = DynamicForm.builder("users-v2", "Users")
                                            .addField(DynamicField.primaryKey("id", "BIGINT"))
                                            .addField(DynamicField.of("name", "VARCHAR"))
                                            .build();
        DynamicForm reordered = DynamicForm.builder("users-v3", "Users")
                                           .addField(DynamicField.of("name", "VARCHAR"))
                                           .addField(DynamicField.primaryKey("id", "BIGINT"))
                                           .build();
        DynamicForm changedType = DynamicForm.builder("users-v4", "Users")
                                             .addField(DynamicField.primaryKey("id", "BIGINT"))
                                             .addField(DynamicField.of("name", "JSON"))
                                             .build();

        assertEquals(first.structureFingerprint(), equivalent.structureFingerprint());
        assertNotEquals(first.structureFingerprint(), reordered.structureFingerprint());
        assertNotEquals(first.structureFingerprint(), changedType.structureFingerprint());
    }

    @Test
    void physicalTableCaseChangesStructureIdentityAndCannotBeDiffed() {
        DynamicForm quoted = DynamicForm.builder("quoted", "Users")
                                        .addField(DynamicField.of("id", "BIGINT"))
                                        .build();
        DynamicForm unquoted = DynamicForm.builder("unquoted", "users")
                                          .addField(DynamicField.of("id", "BIGINT"))
                                          .build();

        assertNotEquals(quoted.structureFingerprint(), unquoted.structureFingerprint());
        assertThrows(IllegalArgumentException.class, () -> quoted.diffTo(unquoted));
    }

    /**
     * 引号标识符的物理表名大小写可区分，自动索引名也必须保留这份结构身份，避免同一 schema 的 DDL 冲突。
     */
    @Test
    void uniqueIndexNamesRetainPhysicalTableCaseIdentity() {
        String quotedTableIndex = uniqueIndexName("Users", "name");
        String lowerCaseTableIndex = uniqueIndexName("users", "name");

        assertNotEquals(quotedTableIndex, lowerCaseTableIndex);
        assertEquals("uk_users_name", lowerCaseTableIndex);
        assertTrue(quotedTableIndex.length() <= 30);
        assertTrue(quotedTableIndex.matches("uk_.+_[0-9a-f]{24}"));
    }

    /**
     * 验证动态表单按字段名规范化查找，并能发布为只读表元数据。
     */
    @Test
    void publishesTableMetadataFromDynamicFields() {
        DynamicForm form = DynamicForm.builder("userForm", "Users")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("name", "VARCHAR")
                                                            .withNullable(false)
                                                            .withUnique(true))
                                      .build();

        Optional<DynamicField> field = form.findField(" NAME ");
        TableMetadata table = form.toTableMetadata();

        assertTrue(field.isPresent());
        assertSame(field.get(), form.field("name"));
        assertEquals("Users", table.name());
        assertEquals(List.of("id"), table.primaryKeyColumns().stream().map(column -> column.name()).toList());
        assertTrue(!table.column("id").nullable());
        assertTrue(!table.column("name").nullable());
        IndexMetadata uniqueIndex = table.indexes().getFirst();
        assertEquals(List.of("name"), uniqueIndex.columns());
        assertTrue(uniqueIndex.unique());
        assertThrows(UnsupportedOperationException.class,
                     () -> form.fields().add(DynamicField.of("age", "INTEGER")));
    }

    /** 字段查找失败不能把来自请求的原始字段名或表单标识写进异常消息。 */
    @Test
    void missingFieldFailureDoesNotExposeLookupInput() {
        DynamicForm form = DynamicForm.builder("secret-form-id", "users")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .build();

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> form.field("secret-request-field"));

        assertEquals("dynamic field does not exist in form", error.getMessage());
    }

    /** 表单构建校验失败不能回显表单标识或字段名。 */
    @Test
    void formValidationFailuresDoNotExposeNames() {
        String secretForm = "secret-form-id";
        String secretField = "secret-field-name";

        IllegalArgumentException duplicate = assertThrows(
                IllegalArgumentException.class,
                () -> DynamicForm.builder(secretForm, "users")
                                 .addField(DynamicField.of(secretField, "VARCHAR"))
                                 .addField(DynamicField.of(" " + secretField + " ", "VARCHAR"))
                                 .build());
        IllegalArgumentException logicDelete = assertThrows(
                IllegalArgumentException.class,
                () -> DynamicForm.builder(secretForm, "users")
                                 .addField(DynamicField.primaryKey("id", "BIGINT"))
                                 .logicDelete(secretField)
                                 .build());
        IllegalArgumentException tenant = assertThrows(
                IllegalArgumentException.class,
                () -> DynamicForm.builder(secretForm, "users")
                                 .addField(DynamicField.primaryKey("id", "BIGINT"))
                                 .tenant(secretField, TenantStrategy.MANUAL)
                                 .build());

        for (IllegalArgumentException failure : List.of(duplicate, logicDelete, tenant)) {
            assertFalse(failure.getMessage().contains(secretForm));
            assertFalse(failure.getMessage().contains(secretField));
        }
    }

    @Test
    void longUniqueIndexNamesUseDeterministicNinetySixBitSha256Suffix() {
        String sharedPrefix = "customer_dimension_assignment_with_a_very_long_shared_physical_prefix_";
        String first = uniqueIndexName(sharedPrefix + "alpha", "external_business_identifier_alpha");
        String repeated = uniqueIndexName(sharedPrefix + "alpha", "external_business_identifier_alpha");
        String second = uniqueIndexName(sharedPrefix + "beta", "external_business_identifier_beta");

        assertEquals(first, repeated);
        assertNotEquals(first, second);
        // Oracle 12.1 的标识符上限为 30 bytes；自动名只包含 ASCII，字符数即字节数。
        assertTrue(first.length() <= 30);
        assertTrue(second.length() <= 30);
        assertTrue(first.matches("uk_.+_[0-9a-f]{24}"));
        assertTrue(second.matches("uk_.+_[0-9a-f]{24}"));
    }

    /** 不同列名即使清洗后相同，也必须生成不同的自动唯一索引名。 */
    @Test
    void uniqueIndexNamesRemainDistinctAfterIdentifierSanitization() {
        DynamicForm form = DynamicForm.builder("collision", "orders")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("a-b", "VARCHAR").withUnique(true))
                                      .addField(DynamicField.of("a_b", "VARCHAR").withUnique(true))
                                      .build();

        List<IndexMetadata> indexes = form.toTableMetadata().indexes();

        assertEquals(2, indexes.size());
        assertNotEquals(indexes.get(0).name(), indexes.get(1).name());
        assertTrue(indexes.stream().allMatch(index -> index.name().length() <= 30));
    }

    private static String uniqueIndexName(String table, String column) {
        DynamicForm form = DynamicForm.builder("longUnique", table)
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of(column, "VARCHAR").withUnique(true))
                                      .build();
        return form.toTableMetadata().indexes().getFirst().name();
    }

    @Test
    void keepsExplicitLogicDeleteDefinition() {
        DynamicForm form = DynamicForm.builder("userForm", "Users")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("removed", "VARCHAR"))
                                      .logicDelete("removed", "N", "Y")
                                      .build();

        LogicDeleteDefinition logicDelete = form.logicDelete().orElseThrow();

        assertEquals("removed", logicDelete.fieldName());
        assertEquals("N", logicDelete.notDeletedValue());
        assertEquals("Y", logicDelete.deletedValue());
    }

    /** 验证发布后的逻辑删除数组值不会被调用方或访问器改写。 */
    @Test
    void snapshotsArrayLogicDeleteValues() {
        byte[] notDeleted = {0};
        byte[] deleted = {1};
        DynamicForm form = DynamicForm.builder("userForm", "Users")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("removed", "BINARY"))
                                      .logicDelete("removed", notDeleted, deleted)
                                      .build();

        notDeleted[0] = 8;
        deleted[0] = 9;
        LogicDeleteDefinition definition = form.logicDelete().orElseThrow();

        assertArrayEquals(new byte[]{0}, (byte[]) definition.notDeletedValue());
        assertArrayEquals(new byte[]{1}, (byte[]) definition.deletedValue());
        ((byte[]) definition.notDeletedValue())[0] = 7;
        assertArrayEquals(new byte[]{0}, (byte[]) form.logicDelete().orElseThrow().notDeletedValue());
    }

    /** 逻辑删除值只复制数组可达图，并在构造和访问边界保持共享身份、自环及普通对象身份。 */
    @Test
    void snapshotsNestedLogicDeleteArrayGraphAtBothBoundaries() {
        Object marker = new Object();
        byte[] shared = {0};
        Object[] cycle = new Object[1];
        cycle[0] = cycle;
        Object[] source = {shared, shared, cycle, marker};
        LogicDeleteDefinition definition = LogicDeleteDefinition.of("removed", source, new byte[]{1});

        shared[0] = 9;
        cycle[0] = new Object();
        source[3] = new Object();
        Object[] first = (Object[]) definition.notDeletedValue();

        assertNotSame(source, first);
        assertSame(first[0], first[1]);
        assertArrayEquals(new byte[]{0}, (byte[]) first[0]);
        assertSame(first[2], ((Object[]) first[2])[0]);
        assertSame(marker, first[3]);

        ((byte[]) first[0])[0] = 8;
        ((Object[]) first[2])[0] = null;
        first[3] = null;
        Object[] second = (Object[]) definition.notDeletedValue();

        assertNotSame(first, second);
        assertNotSame(first[0], second[0]);
        assertSame(second[0], second[1]);
        assertArrayEquals(new byte[]{0}, (byte[]) second[0]);
        assertSame(second[2], ((Object[]) second[2])[0]);
        assertSame(marker, second[3]);
    }

    /** 二进制逻辑删除值必须在表单发布和访问边界保持冻结。 */
    @Test
    void snapshotsByteBufferLogicDeleteValuesAtBothBoundaries() {
        ByteBuffer notDeleted = ByteBuffer.wrap(new byte[]{0});
        ByteBuffer deleted = ByteBuffer.wrap(new byte[]{1});
        LogicDeleteDefinition definition = LogicDeleteDefinition.of("removed", notDeleted, deleted);

        notDeleted.put(0, (byte) 8);
        deleted.put(0, (byte) 9);
        ByteBuffer first = (ByteBuffer) definition.notDeletedValue();
        assertEquals(0, first.get(0));
        assertEquals(1, ((ByteBuffer) definition.deletedValue()).get(0));
        assertThrows(ReadOnlyBufferException.class, () -> first.put(0, (byte) 7));

        first.position(1);
        ByteBuffer second = (ByteBuffer) definition.notDeletedValue();
        assertEquals(0, second.position());
        assertEquals(0, second.get(0));
    }

    @Test
    void rejectsLogicDeleteFieldMissingFromForm() {
        DynamicForm.Builder builder = DynamicForm.builder("userForm", "Users")
                                                 .addField(DynamicField.primaryKey("id", "BIGINT"))
                                                 .logicDelete("deleted");

        assertThrows(IllegalArgumentException.class, builder::build);
    }

    @Test
    void keepsTenantDefinitionAndLeavesRegularFormsTenantFree() {
        DynamicForm regularForm = DynamicForm.builder("noticeForm", "notices")
                                             .addField(DynamicField.primaryKey("id", "BIGINT"))
                                             .build();
        DynamicForm tenantForm = DynamicForm.builder("userForm", "users")
                                            .addField(DynamicField.primaryKey("id", "BIGINT"))
                                            .addField(DynamicField.of("tenant_id", "BIGINT"))
                                            .tenant("tenant_id", TenantStrategy.AUTO)
                                            .build();

        assertEquals(TenantStrategy.NONE, regularForm.tenantStrategy());
        assertTrue(regularForm.tenant().isEmpty());

        TenantDefinition tenant = tenantForm.tenant().orElseThrow();
        assertEquals(TenantStrategy.AUTO, tenant.strategy());
        assertEquals("tenant_id", tenant.fieldName());
    }

    @Test
    void rejectsTenantFieldMissingFromForm() {
        DynamicForm.Builder builder = DynamicForm.builder("userForm", "users")
                                                 .addField(DynamicField.primaryKey("id", "BIGINT"))
                                                 .tenant("tenant_id", TenantStrategy.MANUAL);

        assertThrows(IllegalArgumentException.class, builder::build);
    }

    /**
     * 验证表单版本变更可以被计算为新增、删除和字段定义变更。
     */
    @Test
    void diffsDynamicFormVersionsByNormalizedFieldName() {
        DynamicForm source = DynamicForm.builder("userForm", "Users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("name", "VARCHAR"))
                                        .addField(DynamicField.of("age", "INTEGER"))
                                        .build();
        DynamicForm target = DynamicForm.builder("userForm", "Users")
                                        .addField(DynamicField.primaryKey("id", "BIGINT"))
                                        .addField(DynamicField.of("name", "TEXT"))
                                        .addField(DynamicField.of("email", "VARCHAR"))
                                        .build();

        DynamicFormChangeSet changes = source.diffTo(target);

        assertEquals(List.of("email"), changes.addedFields().stream().map(DynamicField::name).toList());
        assertEquals(List.of("age"), changes.removedFields().stream().map(DynamicField::name).toList());
        assertEquals(List.of("name"), changes.changedFields().stream().map(FieldChange::target).map(DynamicField::name).toList());
    }

    /** 公开变更集构造器必须拒绝伪造的跨表或与源/目标定义不一致的破坏性 DDL 描述。 */
    @Test
    void rejectsForgedDynamicFormChangeSets() {
        DynamicField id = DynamicField.primaryKey("id", "BIGINT");
        DynamicField name = DynamicField.of("name", "VARCHAR");
        DynamicForm source = DynamicForm.builder("source", "users").addField(id).addField(name).build();
        DynamicForm target = DynamicForm.builder("target", "users").addField(id).build();
        DynamicForm otherTable = DynamicForm.builder("other", "admins").addField(id).build();

        assertThrows(IllegalArgumentException.class,
                     () -> new DynamicFormChangeSet(source, otherTable, List.of(), List.of(name), List.of()));
        assertThrows(IllegalArgumentException.class,
                     () -> new DynamicFormChangeSet(source, target, List.of(), List.of(id), List.of()));
        assertThrows(IllegalArgumentException.class,
                     () -> new FieldChange(name, DynamicField.of("email", "TEXT")));
        assertThrows(IllegalArgumentException.class,
                     () -> new FieldChange(name, DynamicField.of("Name", "TEXT")));
        assertThrows(IllegalArgumentException.class, () -> new FieldChange(name, name));
    }
}
