package com.flying.orm.rdb.protection;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Arrays;
import java.util.Objects;

/**
 * 使用版本化主密钥环执行字段 AES-256-GCM 加解密。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
final class ProtectedFieldCipher {

    private static final byte[] DERIVATION_SALT =
            "flying-orm/protected-field/v1".getBytes(StandardCharsets.US_ASCII);
    private static final int MAX_PLAINTEXT_BYTES = 1_048_576;

    private final ProtectedFieldKeyRing keys;
    private final SecureRandom random;

    ProtectedFieldCipher(ProtectedFieldKeyRing keys) {
        this(keys, new SecureRandom());
    }

    ProtectedFieldCipher(ProtectedFieldKeyRing keys, SecureRandom random) {
        this.keys = Objects.requireNonNull(keys, "protected field key ring must not be null");
        this.random = Objects.requireNonNull(random, "secure random must not be null");
    }

    byte[] encrypt(String plaintext, ProtectedFieldContext context) {
        byte[] value = Objects.requireNonNull(plaintext, "protected field plaintext must not be null")
                              .getBytes(StandardCharsets.UTF_8);
        try {
            if (value.length > MAX_PLAINTEXT_BYTES) {
                throw new IllegalArgumentException("protected field plaintext is too long");
            }
            String version = keys.currentVersion();
            byte[] nonce = new byte[ProtectedFieldEnvelope.NONCE_LENGTH];
            random.nextBytes(nonce);
            byte[] ciphertext = crypt(Cipher.ENCRYPT_MODE, value, nonce, version, context);
            return ProtectedFieldEnvelope.encode(version, nonce, ciphertext);
        } finally {
            // 加密、随机数或信封编码任一阶段失败，都不能让明文字节继续留在临时数组中。
            Arrays.fill(value, (byte) 0);
        }
    }

    String decrypt(byte[] envelope, ProtectedFieldContext context) {
        ProtectedFieldEnvelope.Parsed parsed = ProtectedFieldEnvelope.parse(envelope);
        byte[] plaintext = crypt(Cipher.DECRYPT_MODE,
                                 parsed.ciphertext(),
                                 parsed.nonce(),
                                 parsed.keyVersion(),
                                 context);
        try {
            return new String(plaintext, StandardCharsets.UTF_8);
        } finally {
            Arrays.fill(plaintext, (byte) 0);
        }
    }

    private byte[] crypt(int mode,
                         byte[] value,
                         byte[] nonce,
                         String version,
                         ProtectedFieldContext context) {
        byte[] masterKey = keys.masterKey(version);
        byte[] fieldKey = null;
        try {
            fieldKey = HkdfSha256.derive(masterKey,
                                         DERIVATION_SALT,
                                         context.derivationInfo("encryption"),
                                         32);
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            cipher.init(mode, new SecretKeySpec(fieldKey, "AES"), new GCMParameterSpec(128, nonce));
            cipher.updateAAD(context.aad());
            return cipher.doFinal(value);
        } catch (GeneralSecurityException error) {
            throw new ProtectedFieldException(error);
        } finally {
            Arrays.fill(masterKey, (byte) 0);
            if (fieldKey != null) {
                Arrays.fill(fieldKey, (byte) 0);
            }
        }
    }
}
