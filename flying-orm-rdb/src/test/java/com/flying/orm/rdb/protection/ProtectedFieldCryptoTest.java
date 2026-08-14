package com.flying.orm.rdb.protection;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证字段密码协议的密钥治理、用途隔离和认证失败边界。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
class ProtectedFieldCryptoTest {

    private static final ProtectedFieldContext CUSTOMER_CONTACT =
            new ProtectedFieldContext("customer", "contact_value", "tenant-a");

    @Test
    void validatesAndOwnsTheVersionedMasterKeyRing() {
        byte[] source = key(1);
        ProtectedFieldKeyRing ring = ProtectedFieldKeyRing.single("v1", source);
        source[0] = 99;
        ProtectedFieldCipher cipher = new ProtectedFieldCipher(ring);

        byte[] encrypted = cipher.encrypt("contact-value", CUSTOMER_CONTACT);

        assertEquals("contact-value", cipher.decrypt(encrypted, CUSTOMER_CONTACT));
        assertThrows(IllegalArgumentException.class,
                     () -> ProtectedFieldKeyRing.single("v1", new byte[31]));
        assertThrows(IllegalArgumentException.class,
                     () -> ProtectedFieldKeyRing.single("unsafe version", key(2)));
        assertThrows(IllegalArgumentException.class,
                     () -> ProtectedFieldKeyRing.builder()
                             .current("v5", key(5))
                             .readable("v1", key(1))
                             .readable("v2", key(2))
                             .readable("v3", key(3))
                             .readable("v4", key(4))
                             .build());

        ring.close();
        assertThrows(IllegalStateException.class, () -> cipher.encrypt("after-close", CUSTOMER_CONTACT));
    }

    /** 构建失败后内部密钥副本已经清理，构建器必须被消费而不能继续持有或复用失败配置。 */
    @Test
    void consumesKeyRingBuilderWhenValidationFails() {
        ProtectedFieldKeyRing.Builder builder = ProtectedFieldKeyRing.builder()
                                                                      .current("v5", key(5))
                                                                      .readable("v1", key(1))
                                                                      .readable("v2", key(2))
                                                                      .readable("v3", key(3))
                                                                      .readable("v4", key(4));

        assertThrows(IllegalArgumentException.class, builder::build);
        assertEquals("protected field key ring builder is already used",
                     assertThrows(IllegalStateException.class, builder::build).getMessage());
        assertEquals("protected field key ring builder is already used",
                     assertThrows(IllegalStateException.class,
                                  () -> builder.readable("v6", key(6))).getMessage());
    }

    @Test
    void usesRandomAuthenticatedEncryptionAndRejectsContextOrCiphertextTampering() {
        try (ProtectedFieldKeyRing ring = ProtectedFieldKeyRing.single("v1", key(1))) {
            ProtectedFieldCipher cipher = new ProtectedFieldCipher(ring);

            byte[] first = cipher.encrypt("same-value", CUSTOMER_CONTACT);
            byte[] second = cipher.encrypt("same-value", CUSTOMER_CONTACT);
            byte[] tampered = first.clone();
            tampered[tampered.length - 1] ^= 1;

            assertFalse(Arrays.equals(first, second));
            assertEquals("same-value", cipher.decrypt(first, CUSTOMER_CONTACT));
            assertProtectedFailure(() -> cipher.decrypt(first,
                    new ProtectedFieldContext("customer", "other_field", "tenant-a")));
            assertProtectedFailure(() -> cipher.decrypt(first,
                    new ProtectedFieldContext("customer", "contact_value", "tenant-b")));
            assertProtectedFailure(() -> cipher.decrypt(tampered, CUSTOMER_CONTACT));
        }
    }

    @Test
    void readsOldCiphertextDuringRotationAndWritesOnlyWithTheCurrentVersion() {
        byte[] legacy;
        try (ProtectedFieldKeyRing oldRing = ProtectedFieldKeyRing.single("v1", key(1))) {
            legacy = new ProtectedFieldCipher(oldRing).encrypt("legacy", CUSTOMER_CONTACT);
        }

        try (ProtectedFieldKeyRing rotating = ProtectedFieldKeyRing.builder()
                .current("v2", key(2))
                .readable("v1", key(1))
                .build()) {
            ProtectedFieldCipher cipher = new ProtectedFieldCipher(rotating);
            byte[] current = cipher.encrypt("current", CUSTOMER_CONTACT);

            assertEquals("legacy", cipher.decrypt(legacy, CUSTOMER_CONTACT));
            assertEquals("current", cipher.decrypt(current, CUSTOMER_CONTACT));
            assertEquals("v2", ProtectedFieldEnvelope.keyVersion(current));
            assertNotEquals(ProtectedFieldEnvelope.keyVersion(legacy), ProtectedFieldEnvelope.keyVersion(current));
            assertEquals(List.of("v2", "v1"), rotating.versionsInSearchOrder());
        }
    }

    @Test
    void derivesHkdfSha256UsingTheRfc5869Vector() {
        byte[] ikm = new byte[22];
        Arrays.fill(ikm, (byte) 0x0b);
        byte[] salt = hex("000102030405060708090a0b0c");
        byte[] info = hex("f0f1f2f3f4f5f6f7f8f9");

        byte[] output = HkdfSha256.derive(ikm, salt, info, 42);

        assertArrayEquals(hex("3cb25f25faacd57a90434f64d0362f2a"
                + "2d2d0a90cf1a5a4c5db02d56ecc4c5bf"
                + "34007208d5b887185865"), output);
    }

    private static void assertProtectedFailure(ThrowingOperation operation) {
        ProtectedFieldException error = assertThrows(ProtectedFieldException.class, operation::run);
        assertEquals("protected field value cannot be decrypted", error.getMessage());
    }

    private static byte[] key(int seed) {
        byte[] value = new byte[32];
        Arrays.fill(value, (byte) seed);
        return value;
    }

    private static byte[] hex(String value) {
        return java.util.HexFormat.of().parseHex(value);
    }

    @FunctionalInterface
    private interface ThrowingOperation {
        void run();
    }
}
