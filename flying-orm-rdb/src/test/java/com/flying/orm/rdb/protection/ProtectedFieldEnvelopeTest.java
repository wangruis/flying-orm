package com.flying.orm.rdb.protection;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.scope.DataScope;
import org.junit.jupiter.api.Test;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 密文版本探测与完整解析必须共享同一套信封校验。 */
class ProtectedFieldEnvelopeTest {

    @Test
    void keepsContainsTokenBytesCompatibleWithIndividualDerivation() throws Exception {
        byte[] masterKey = new byte[32];
        Arrays.fill(masterKey, (byte) 7);
        ProtectedFieldContext context = new ProtectedFieldContext("orders", "note", "tenant-a");
        EncryptedFieldDefinition definition = EncryptedFieldDefinition.builder()
                                                                      .searchModes(EncryptedSearchMode.CONTAINS)
                                                                      .build();
        ProtectedSearchTokenService tokens = new ProtectedSearchTokenService(
                ProtectedFieldKeyRing.single("v1", masterKey), ProtectedValueNormalizerRegistry.standard());

        List<byte[]> actual = tokens.currentContainsTokens("abcdef", definition, context);
        List<String> trigrams = List.of("abc", "bcd", "cde", "def");

        assertEquals(trigrams.size(), actual.size());
        for (int index = 0; index < trigrams.size(); index++) {
            assertArrayEquals(legacyContainsToken(masterKey, trigrams.get(index), context), actual.get(index));
        }
    }

    @Test
    void keepsEmptyContainsTokenGroupFreeOfTokens() {
        ProtectedSearchTokenService tokens = new ProtectedSearchTokenService(
                ProtectedFieldKeyRing.single("v1", new byte[32]), ProtectedValueNormalizerRegistry.standard());

        assertEquals(List.of(), tokens.currentContainsTokens(
                "ab",
                EncryptedFieldDefinition.builder().searchModes(EncryptedSearchMode.CONTAINS).build(),
                new ProtectedFieldContext("orders", "note", "tenant-a")));
    }

    @Test
    void preservesErrorsFromProtectedValueCodecs() {
        AssertionError fatal = new AssertionError("codec error");
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == String.class;
            }

            @Override
            public Object write(Object value) {
                throw fatal;
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                return value;
            }
        });

        assertSame(fatal, assertThrows(
                AssertionError.class, () -> ProtectedFieldValues.encodedText(codecs, "secret")));
    }

    @Test
    void isolatesRuntimeFailuresFromProtectedValueCodecs() {
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == String.class;
            }

            @Override
            public Object write(Object value) {
                throw new IllegalStateException("secret codec failure");
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                return value;
            }
        });

        IllegalArgumentException error = assertThrows(
                IllegalArgumentException.class, () -> ProtectedFieldValues.encodedText(codecs, "secret"));

        assertEquals("encrypted field value cannot be encoded", error.getMessage());
        assertNull(error.getCause());
    }

    @Test
    void clearsTemporarySearchMasterKeyWhenTokenPreparationFails() throws Exception {
        byte[] temporaryMasterKey = new byte[32];
        Arrays.fill(temporaryMasterKey, (byte) 7);
        ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", new byte[32]);
        ProtectedSearchTokenService tokens = new ProtectedSearchTokenService(
                keys, ProtectedValueNormalizerRegistry.standard());
        Method token = ProtectedSearchTokenService.class.getDeclaredMethod(
                "token", byte[].class, String.class, String.class, ProtectedFieldContext.class);
        token.setAccessible(true);

        InvocationTargetException failure = assertThrows(
                InvocationTargetException.class,
                () -> token.invoke(tokens, temporaryMasterKey, "exact", "value", null));

        assertTrue(failure.getCause() instanceof NullPointerException);
        assertArrayEquals(new byte[32], temporaryMasterKey);
    }

    @Test
    void readsVersionWithoutChangingFullParseSemantics() {
        byte[] nonce = new byte[ProtectedFieldEnvelope.NONCE_LENGTH];
        byte[] ciphertext = new byte[32];
        Arrays.fill(ciphertext, (byte) 7);
        byte[] envelope = ProtectedFieldEnvelope.encode("v2", nonce, ciphertext);

        assertEquals("v2", ProtectedFieldEnvelope.keyVersion(envelope));
        ProtectedFieldEnvelope.Parsed parsed = ProtectedFieldEnvelope.parse(envelope);
        assertEquals("v2", parsed.keyVersion());
        assertArrayEquals(nonce, parsed.nonce());
        assertArrayEquals(ciphertext, parsed.ciphertext());
    }

    @Test
    void versionProbeRejectsTruncatedCiphertext() {
        byte[] envelope = ProtectedFieldEnvelope.encode(
                "v2", new byte[ProtectedFieldEnvelope.NONCE_LENGTH], new byte[32]);

        assertThrows(ProtectedFieldException.class,
                     () -> ProtectedFieldEnvelope.keyVersion(Arrays.copyOf(envelope, envelope.length - 1)));
    }

    @Test
    void plaintextMigrationRewritesAnAuthenticatedCiphertextThatNoLongerVerifies() {
        ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", new byte[32]);
        DynamicForm form = DynamicForm.builder("users", "users")
                                      .addField(DynamicField.of("phone", "VARCHAR"))
                                      .encrypted("phone", EncryptedFieldDefinition.builder().build())
                                      .build();
        ValueCodecRegistry codecs = ValueCodecRegistry.standard();
        String tenant = ProtectedFieldValues.tenantIdentity(form, DataScope.none(), codecs);
        byte[] ciphertext = new ProtectedFieldCipher(keys).encrypt(
                "already protected", ProtectedFieldValues.context(form, form.field("phone"), tenant));
        ciphertext[ciphertext.length - 1] ^= 1;

        Map<String, Object> pending = ProtectedFieldReprotection.create(keys)
                .valuesNeedingPlaintextMigration(
                        form, Map.of("phone", "trusted legacy"), Map.of("phone", ciphertext));

        assertEquals(Map.of("phone", "trusted legacy"), pending);
    }

    @Test
    void equivalentAbsoluteTenantValuesShareOneProtectedIdentity() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("tenant_at", "TIMESTAMPTZ"))
                                      .tenant("tenant_at", TenantStrategy.AUTO)
                                      .build();
        ValueCodecRegistry codecs = ValueCodecRegistry.standard();
        Instant instant = Instant.parse("2026-08-23T00:00:00Z");

        String fromInstant = ProtectedFieldValues.tenantIdentity(
                form, DataScope.tenant("tenant_at", instant), codecs);
        String fromEquivalentOffset = ProtectedFieldValues.tenantIdentity(
                form,
                DataScope.tenant("tenant_at", OffsetDateTime.parse("2026-08-23T08:00:00+08:00")),
                codecs);
        String fromDifferentInstant = ProtectedFieldValues.tenantIdentity(
                form,
                DataScope.tenant("tenant_at", OffsetDateTime.parse("2026-08-23T08:00:01+08:00")),
                codecs);

        assertEquals("time:java.time.Instant:2026-08-23T00:00:00Z", fromInstant);
        assertEquals(fromInstant, fromEquivalentOffset);
        assertNotEquals(fromInstant, fromDifferentInstant);
    }

    @Test
    void equivalentLocalTemporalTenantCarriersShareProtectedIdentities() {
        ValueCodecRegistry codecs = ValueCodecRegistry.standard();
        LocalDate date = LocalDate.parse("2026-08-23");
        LocalTime time = LocalTime.parse("10:11:12");
        LocalDateTime dateTime = LocalDateTime.parse("2026-08-23T10:11:12.123456789");

        String localDate = tenantIdentity("DATE", date, codecs);
        String localTime = tenantIdentity("TIME", time, codecs);
        String localDateTime = tenantIdentity("TIMESTAMP(9)", dateTime, codecs);

        assertEquals("time:java.time.LocalDate:2026-08-23", localDate);
        assertEquals(localDate, assertDoesNotThrow(
                () -> tenantIdentity("DATE", java.sql.Date.valueOf(date), codecs)));
        assertEquals("time:java.time.LocalTime:10:11:12", localTime);
        assertEquals(localTime, assertDoesNotThrow(
                () -> tenantIdentity("TIME", java.sql.Time.valueOf(time), codecs)));
        assertEquals("time:java.time.LocalDateTime:2026-08-23T10:11:12.123456789", localDateTime);
        assertEquals(localDateTime, assertDoesNotThrow(
                () -> tenantIdentity("TIMESTAMP(9)", java.sql.Timestamp.valueOf(dateTime), codecs)));
    }

    @Test
    void equivalentOffsetTimeTenantCarriersShareOneProtectedIdentity() {
        ValueCodecRegistry codecs = ValueCodecRegistry.standard();
        OffsetTime time = OffsetTime.parse("10:11:12.123456789+08:00");

        String fromOffsetTime = tenantIdentity("OFFSET_TIME(9)", time, codecs);
        String fromText = tenantIdentity("OFFSET_TIME(9)", time.toString(), codecs);

        assertEquals("time:java.time.OffsetTime:10:11:12.123456789+08:00", fromOffsetTime);
        assertEquals(fromOffsetTime, fromText);
    }

    @Test
    void preservesCustomTemporalWriteIdentityAfterCarrierNormalization() {
        ValueCodec millisecondTimestamp = new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == LocalDateTime.class;
            }

            @Override
            public Object write(Object value) {
                LocalDateTime dateTime = (LocalDateTime) value;
                return dateTime.withNano(dateTime.getNano() / 1_000_000 * 1_000_000);
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                if (value instanceof java.sql.Timestamp timestamp) {
                    return timestamp.toLocalDateTime();
                }
                return LocalDateTime.parse(value.toString());
            }
        };
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(millisecondTimestamp);
        LocalDateTime source = LocalDateTime.parse("2026-08-23T10:11:12.123456789");
        String expected = "time:java.time.LocalDateTime:2026-08-23T10:11:12.123";

        assertEquals(expected, tenantIdentity("TIMESTAMP(3)", source, codecs));
        assertEquals(expected, tenantIdentity("TIMESTAMP(3)", java.sql.Timestamp.valueOf(source), codecs));
    }

    @Test
    void rejectsTemporalTenantValuesThatExceedTheDeclaredStoragePrecision() {
        ValueCodecRegistry codecs = ValueCodecRegistry.standard();

        assertThrows(IllegalArgumentException.class, () -> tenantIdentity(
                "TIMESTAMP(3)", LocalDateTime.parse("2026-08-23T10:11:12.123456789"), codecs));
        assertThrows(IllegalArgumentException.class, () -> tenantIdentity(
                "OFFSET_TIME(3)", OffsetTime.parse("10:11:12.123456789+08:00"), codecs));
        assertThrows(IllegalArgumentException.class, () -> tenantIdentity(
                DynamicField.of("tenant_time", "TIMESTAMP").withPrecision(3, null),
                LocalDateTime.parse("2026-08-23T10:11:12.123456789"), codecs));
    }

    @Test
    void rejectsFractionalTemporalTenantValuesWhenStoragePrecisionIsImplicit() {
        ValueCodecRegistry codecs = ValueCodecRegistry.standard();

        assertThrows(IllegalArgumentException.class, () -> tenantIdentity(
                "TIMESTAMPTZ", Instant.parse("2026-08-23T10:11:12.123456789Z"), codecs));
        assertThrows(IllegalArgumentException.class, () -> tenantIdentity(
                "TIMESTAMP", LocalDateTime.parse("2026-08-23T10:11:12.123456789"), codecs));
        assertThrows(IllegalArgumentException.class, () -> tenantIdentity(
                "TIME", LocalTime.parse("10:11:12.123456789"), codecs));
        assertThrows(IllegalArgumentException.class, () -> tenantIdentity(
                "SQLSERVER_SMALLDATETIME", LocalDateTime.parse("2026-08-23T10:11:12"), codecs));
        assertDoesNotThrow(() -> tenantIdentity(
                "TIMESTAMPTZ", Instant.parse("2026-08-23T10:11:12Z"), codecs));
        assertDoesNotThrow(() -> tenantIdentity(
                "SQLSERVER_SMALLDATETIME", LocalDateTime.parse("2026-08-23T10:11:00"), codecs));
    }

    @Test
    void validatesFixedPrecisionTemporalTenantStorageIndependentlyOfDeclaredPrecision() {
        ValueCodecRegistry codecs = ValueCodecRegistry.standard();

        assertThrows(IllegalArgumentException.class, () -> tenantIdentity(
                DynamicField.of("tenant_time", "ORACLE_DATE").withPrecision(9, null),
                LocalDateTime.parse("2026-08-23T10:11:12.000000001"), codecs));
        assertThrows(IllegalArgumentException.class, () -> tenantIdentity(
                DynamicField.of("tenant_time", "SQLSERVER_SMALLDATETIME").withPrecision(9, null),
                LocalDateTime.parse("2026-08-23T10:11:00.000000001"), codecs));
        assertThrows(IllegalArgumentException.class, () -> tenantIdentity(
                DynamicField.of("tenant_time", "SQLSERVER_DATETIME").withPrecision(9, null),
                LocalDateTime.parse("2026-08-23T10:11:12.004"), codecs));
        assertDoesNotThrow(() -> tenantIdentity(
                "SQLSERVER_DATETIME", LocalDateTime.parse("2026-08-23T10:11:12.003"), codecs));
    }

    @Test
    void rejectsTextEncodedTemporalTenantValuesThatExceedStoragePrecision() {
        ValueCodec textTimestamp = new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == LocalDateTime.class;
            }

            @Override
            public Object write(Object value) {
                return value.toString();
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                return LocalDateTime.parse(value.toString());
            }
        };

        assertThrows(IllegalArgumentException.class, () -> tenantIdentity(
                "TIMESTAMP(3)", LocalDateTime.parse("2026-08-23T10:11:12.123456789"),
                ValueCodecRegistry.standard().withFirst(textTimestamp)));
    }

    @Test
    void rejectsConflictingInlineAndExplicitTemporalTenantPrecision() {
        assertThrows(IllegalArgumentException.class, () -> tenantIdentity(
                DynamicField.of("tenant_time", "TIMESTAMP(3)").withPrecision(6, null),
                LocalDateTime.parse("2026-08-23T10:11:12.123"), ValueCodecRegistry.standard()));
    }

    private static String tenantIdentity(String dataType, Object value, ValueCodecRegistry codecs) {
        return tenantIdentity(DynamicField.of("tenant_time", dataType), value, codecs);
    }

    private static byte[] legacyContainsToken(byte[] masterKey,
                                               String trigram,
                                               ProtectedFieldContext context) throws Exception {
        byte[] tokenKey = HkdfSha256.derive(masterKey,
                                            "flying-orm/protected-search/v1".getBytes(StandardCharsets.US_ASCII),
                                            context.derivationInfo("contains"),
                                            32);
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(tokenKey, "HmacSHA256"));
            return mac.doFinal(trigram.getBytes(StandardCharsets.UTF_8));
        } finally {
            Arrays.fill(tokenKey, (byte) 0);
        }
    }

    private static String tenantIdentity(DynamicField tenantField, Object value, ValueCodecRegistry codecs) {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(tenantField)
                                      .tenant("tenant_time", TenantStrategy.AUTO)
                                      .build();
        return ProtectedFieldValues.tenantIdentity(
                form, DataScope.tenant("tenant_time", value), codecs);
    }
}
