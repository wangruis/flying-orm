package com.flying.orm.rdb.protection;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.scope.DataScope;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证租户密钥派生身份只接受有稳定数据库等价语义的编码结果。
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
class ProtectedFieldValuesTest {

    /**
     * PreparedWrite 对 ORM 生成的密文和盲索引负责快照，但不能改变普通二进制业务值的低层交接语义。
     */
    @Test
    void snapshotsGeneratedProtectionArraysWithoutCopyingCallerBinaryValues() {
        byte[] payload = {7, 8, 9};
        DynamicForm form = DynamicForm.builder("protected_snapshot", "protected_snapshot")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("secret", "VARCHAR"))
                                      .addField(DynamicField.of("payload", "BLOB"))
                                      .encrypted("secret", EncryptedFieldDefinition.builder().build())
                                      .build();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", new byte[32])) {
            ProtectedFieldRuntime.PreparedWrite write = ProtectedFieldRuntime.create(keys).prepareWrite(
                    form,
                    Map.of("id", 1L, "secret", "sensitive", "payload", payload),
                    DataScope.none(),
                    ValueCodecRegistry.standard());
            String exactColumn = ProtectedFormLayout.exactColumn(form, "secret");
            byte[] exposedCiphertext = (byte[]) write.values().get("secret");
            byte[] exposedExactToken = (byte[]) write.values().get(exactColumn);
            byte[] expectedCiphertext = exposedCiphertext.clone();
            byte[] expectedExactToken = exposedExactToken.clone();

            exposedCiphertext[0] ^= 1;
            exposedExactToken[0] ^= 1;
            Map<String, Object> secondRead = write.values();

            assertArrayEquals(expectedCiphertext, (byte[]) secondRead.get("secret"));
            assertArrayEquals(expectedExactToken, (byte[]) secondRead.get(exactColumn));
            assertNotSame(exposedCiphertext, secondRead.get("secret"));
            assertNotSame(exposedExactToken, secondRead.get(exactColumn));
            assertSame(payload, secondRead.get("payload"));
        }
    }

    /** 数值标度和具体 Java 数值类型不能改变同一数据库租户值的派生身份。 */
    @Test
    void canonicalizesEquivalentNumericTenantValues() {
        DynamicForm form = tenantForm();

        String decimal = ProtectedFieldValues.tenantIdentity(
                form, DataScope.tenant("tenant_id", new BigDecimal("1.00")), ValueCodecRegistry.standard());
        String integer = ProtectedFieldValues.tenantIdentity(
                form, DataScope.tenant("tenant_id", 1L), ValueCodecRegistry.standard());

        assertEquals(decimal, integer);
    }

    /** 极端负 scale 不能把很小的租户数值放大为无界派生文本。 */
    @Test
    void keepsNumericTenantIdentityBoundedForLargeNegativeScale() {
        BigDecimal tenant = new BigDecimal(BigInteger.ONE, -1024 * 1024);

        String identity = ProtectedFieldValues.tenantIdentity(
                tenantForm(), DataScope.tenant("tenant_id", tenant), ValueCodecRegistry.standard());

        assertTrue(identity.length() < 64);
        assertEquals(identity, ProtectedFieldValues.tenantIdentity(
                tenantForm(), DataScope.tenant("tenant_id", new BigDecimal("1E+1048576")),
                ValueCodecRegistry.standard()));
    }

    /** 任意对象数组没有稳定数据库文本语义，不能退回 Object.toString 参与密钥派生。 */
    @Test
    void rejectsNonBinaryArrayTenantIdentity() {
        DynamicForm form = tenantForm();

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> ProtectedFieldValues.tenantIdentity(
                        form, DataScope.tenant("tenant_id", new Object[]{"tenant-a"}),
                        ValueCodecRegistry.standard()));

        assertEquals("tenant value does not have a stable protected identity", error.getMessage());
    }

    @Test
    void sanitizesCodecFailureBeforeEncryptedPlaintextLeavesTheProtectionBoundary() {
        String plaintext = "sensitive-value-that-must-not-escape";
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(failingStringCodec());
        DynamicForm form = DynamicForm.builder("account", "account")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("secret", "VARCHAR"))
                                      .encrypted("secret", EncryptedFieldDefinition.builder().build())
                                      .build();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", new byte[32])) {
            ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(keys);

            IllegalArgumentException error = assertThrows(
                    IllegalArgumentException.class,
                    () -> runtime.prepareWrite(form, Map.of("secret", plaintext), DataScope.none(), codecs));

            assertEquals("encrypted field value cannot be encoded", error.getMessage());
            assertNull(error.getCause());
        }
    }

    @Test
    void sanitizesCodecFailureBeforeTenantIdentityLeavesTheProtectionBoundary() {
        String tenant = "sensitive-tenant-that-must-not-escape";
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(failingStringCodec());

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class,
                () -> ProtectedFieldValues.tenantIdentity(
                        tenantForm(), DataScope.tenant("tenant_id", tenant), codecs));

        assertEquals("tenant value does not have a stable protected identity", error.getMessage());
        assertNull(error.getCause());
    }

    /** codec 的普通 Error 也必须在明文边界转换为固定分类且不保留 cause。 */
    @Test
    void sanitizesOrdinaryCodecErrorAtProtectedValueBoundaries() {
        String plaintext = "ordinary-error-secret";
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(errorStringCodec());
        DynamicForm form = DynamicForm.builder("account", "account")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("secret", "VARCHAR"))
                                      .encrypted("secret", EncryptedFieldDefinition.builder().build())
                                      .build();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", new byte[32])) {
            IllegalArgumentException valueError = assertThrows(
                    IllegalArgumentException.class,
                    () -> ProtectedFieldRuntime.create(keys).prepareWrite(
                            form, Map.of("secret", plaintext), DataScope.none(), codecs));
            IllegalArgumentException tenantError = assertThrows(
                    IllegalArgumentException.class,
                    () -> ProtectedFieldValues.tenantIdentity(
                            tenantForm(), DataScope.tenant("tenant_id", plaintext), codecs));

            assertEquals("encrypted field value cannot be encoded", valueError.getMessage());
            assertEquals("tenant value does not have a stable protected identity", tenantError.getMessage());
            assertNull(valueError.getCause());
            assertNull(tenantError.getCause());
        }
    }

    /** codec 包装的 VM 错误在明文和租户派生边界都必须恢复为同一致命错误。 */
    @Test
    void preservesNestedVirtualMachineErrorAtProtectedValueBoundaries() {
        OutOfMemoryError fatal = new OutOfMemoryError("nested codec fatal");
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(wrappedFatalStringCodec(fatal));
        DynamicForm form = DynamicForm.builder("account", "account")
                                      .addField(DynamicField.primaryKey("id", "BIGINT"))
                                      .addField(DynamicField.of("secret", "VARCHAR"))
                                      .encrypted("secret", EncryptedFieldDefinition.builder().build())
                                      .build();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", new byte[32])) {
            OutOfMemoryError valueError = assertThrows(
                    OutOfMemoryError.class,
                    () -> ProtectedFieldRuntime.create(keys).prepareWrite(
                            form, Map.of("secret", "secret"), DataScope.none(), codecs));
            OutOfMemoryError tenantError = assertThrows(
                    OutOfMemoryError.class,
                    () -> ProtectedFieldValues.tenantIdentity(
                            tenantForm(), DataScope.tenant("tenant_id", "secret"), codecs));

            assertSame(fatal, valueError);
            assertSame(fatal, tenantError);
        }
    }

    private static ValueCodec failingStringCodec() {
        return new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == String.class;
            }

            @Override
            public Object write(Object value) {
                throw new IllegalStateException("codec leaked " + value);
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                throw new UnsupportedOperationException("not used");
            }
        };
    }

    private static ValueCodec errorStringCodec() {
        return new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == String.class;
            }

            @Override
            public Object write(Object value) {
                throw new AssertionError("codec leaked " + value);
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                throw new UnsupportedOperationException("not used");
            }
        };
    }

    private static ValueCodec wrappedFatalStringCodec(OutOfMemoryError fatal) {
        return new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == String.class;
            }

            @Override
            public Object write(Object value) {
                throw new IllegalStateException("codec wrapper", fatal);
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                throw new UnsupportedOperationException("not used");
            }
        };
    }

    private static DynamicForm tenantForm() {
        return DynamicForm.builder("tenant_account", "tenant_account")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("tenant_id", "DECIMAL"))
                .tenant("tenant_id", TenantStrategy.MANUAL)
                .build();
    }
}
