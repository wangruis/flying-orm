package com.flying.orm.rdb.protection;

import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.protection.MaskedFieldDefinition;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ProtectedFieldDeveloperLimitsTest {

    private static final ProtectedFieldContext CONTEXT =
            new ProtectedFieldContext("orders", "note", "tenant-a");

    @Test
    void customMaskMayReturnDeveloperChosenDisplayLength() {
        String display = "[This value is hidden by the application's disclosure policy]";
        MaskingPolicyRegistry masks = MaskingPolicyRegistry.standard()
                .with("notice", (value, definition) -> display)
                .with("invalid", (value, definition) -> null);

        assertEquals(display, masks.mask("secret", MaskedFieldDefinition.builder("notice").build()));
        assertThrows(IllegalArgumentException.class,
                () -> masks.mask("secret", MaskedFieldDefinition.builder("invalid").build()));
    }

    @Test
    void encryptsAndAuthenticatesMoreThanOneMiBOfUtf8Plaintext() {
        String plaintext = "界".repeat(349_526);
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(1))) {
            ProtectedFieldCipher cipher = new ProtectedFieldCipher(keys);
            byte[] envelope = cipher.encrypt(plaintext, CONTEXT);

            assertEquals(plaintext, cipher.decrypt(envelope, CONTEXT));
            assertDoesNotThrow(() -> cipher.verify(envelope, CONTEXT));
            assertThrows(ProtectedFieldException.class, () -> cipher.decrypt(
                    envelope, new ProtectedFieldContext("orders", "note", "tenant-b")));
            envelope[envelope.length - 1] ^= 1;
            assertThrows(ProtectedFieldException.class, () -> cipher.verify(envelope, CONTEXT));
        }
    }

    @Test
    void parsesLargeEnvelopesAndStillRejectsTruncation() {
        byte[] ciphertext = new byte[1_048_593];
        byte[] envelope = ProtectedFieldEnvelope.encode("v1", new byte[12], ciphertext);

        assertEquals("v1", ProtectedFieldEnvelope.keyVersion(envelope));
        assertArrayEquals(ciphertext, ProtectedFieldEnvelope.parse(envelope).ciphertext());
        assertThrows(ProtectedFieldException.class, () -> ProtectedFieldEnvelope.parse(
                Arrays.copyOf(envelope, envelope.length - 1)));
    }

    @Test
    void longContextComponentsRemainAuthenticatedAndUnambiguous() {
        String form = "f".repeat(513);
        String field = "d".repeat(513);
        String tenant = "t".repeat(513);
        ProtectedFieldContext context = new ProtectedFieldContext(form, field, tenant);
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(1))) {
            ProtectedFieldCipher cipher = new ProtectedFieldCipher(keys);
            byte[] envelope = cipher.encrypt("secret", context);

            assertEquals("secret", cipher.decrypt(envelope, context));
            assertThrows(ProtectedFieldException.class, () -> cipher.decrypt(
                    envelope, new ProtectedFieldContext(form, field + "t", tenant.substring(1))));
        }
    }

    @Test
    void allDeveloperConfiguredReadableKeysParticipateInDecryptionAndSearch() {
        ProtectedFieldKeyRing.Builder builder = ProtectedFieldKeyRing.builder().current("v6", key(6));
        for (int version = 1; version < 6; version++) {
            builder.readable("v" + version, key(version));
        }
        try (ProtectedFieldKeyRing keys = builder.build()) {
            ProtectedFieldCipher cipher = new ProtectedFieldCipher(keys);
            ProtectedSearchTokenService tokens = tokens(keys);
            EncryptedFieldDefinition definition = EncryptedFieldDefinition.builder().build();
            List<byte[]> queryTokens = tokens.exactQueryTokens("secret", definition, CONTEXT);

            assertEquals(6, keys.readableVersions().size());
            assertEquals(6, queryTokens.size());
            for (int version = 1; version <= 6; version++) {
                try (ProtectedFieldKeyRing oldKeys = ProtectedFieldKeyRing.single("v" + version, key(version))) {
                    assertEquals("secret", cipher.decrypt(
                            new ProtectedFieldCipher(oldKeys).encrypt("secret", CONTEXT), CONTEXT));
                    int queryIndex = version == 6 ? 0 : version;
                    assertArrayEquals(tokens(oldKeys).currentExactToken("secret", definition, CONTEXT),
                            queryTokens.get(queryIndex));
                }
            }
        }
    }

    @Test
    void developerNormalizedLimitFlowsThroughExactTokenGeneration() {
        String value = "x".repeat(65_537);
        EncryptedFieldDefinition definition = EncryptedFieldDefinition.builder()
                .maxNormalizedLength(value.length()).build();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(1))) {
            ProtectedSearchTokenService tokens = tokens(keys);
            assertArrayEquals(tokens.currentExactToken(value, definition, CONTEXT),
                    tokens.exactQueryTokens(value, definition, CONTEXT).getFirst());
            assertThrows(IllegalArgumentException.class,
                    () -> tokens.currentExactToken(value + "x", definition, CONTEXT));
        }
    }

    @Test
    void developerContainsLimitAllowsMoreThanFourThousandDistinctTrigrams() {
        int[] codePoints = IntStream.range(0x4e00, 0x4e00 + 4_100).toArray();
        String value = new String(codePoints, 0, codePoints.length);
        EncryptedFieldDefinition definition = EncryptedFieldDefinition.builder()
                .searchModes(EncryptedSearchMode.CONTAINS).maxNormalizedLength(4_100).build();
        try (ProtectedFieldKeyRing keys = ProtectedFieldKeyRing.single("v1", key(1))) {
            ProtectedSearchTokenService tokens = tokens(keys);
            List<byte[]> written = tokens.currentContainsTokens(value, definition, CONTEXT);
            ProtectedSearchTokenService.ContainsQuery query = tokens.containsQuery(value, definition, CONTEXT);

            assertEquals(4_098, written.size());
            assertEquals(written.size(), query.distinctTokenCount());
            for (int index = 0; index < written.size(); index++) {
                assertArrayEquals(written.get(index), query.groups().getFirst().tokens().get(index));
            }
            assertThrows(IllegalArgumentException.class,
                    () -> tokens.currentContainsTokens(value + "x", definition, CONTEXT));
        }
    }

    private static ProtectedSearchTokenService tokens(ProtectedFieldKeyRing keys) {
        return new ProtectedSearchTokenService(keys, ProtectedValueNormalizerRegistry.standard());
    }

    private static byte[] key(int value) {
        byte[] key = new byte[32];
        Arrays.fill(key, (byte) value);
        return key;
    }
}
