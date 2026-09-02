package com.flying.orm.rdb.protection;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.security.GeneralSecurityException;
import java.util.Arrays;

/** 使用 JCA 实现 RFC 5869 HKDF-SHA-256。 */
final class HkdfSha256 {

    private static final int HASH_LENGTH = 32;

    private HkdfSha256() {
    }

    static byte[] derive(byte[] inputKey, byte[] salt, byte[] info, int length) {
        if (length < 1 || length > 255 * HASH_LENGTH) {
            throw new IllegalArgumentException("HKDF output length is out of range");
        }
        byte[] effectiveSalt = salt == null || salt.length == 0 ? new byte[HASH_LENGTH] : salt.clone();
        byte[] pseudoRandomKey = hmac(effectiveSalt, inputKey);
        Arrays.fill(effectiveSalt, (byte) 0);
        try {
            return expand(pseudoRandomKey, info == null ? new byte[0] : info, length);
        } finally {
            Arrays.fill(pseudoRandomKey, (byte) 0);
        }
    }

    private static byte[] expand(byte[] key, byte[] info, int length) {
        byte[] output = new byte[length];
        byte[] previous = new byte[0];
        int offset = 0;
        for (int block = 1; offset < length; block++) {
            byte[] input = new byte[previous.length + info.length + 1];
            System.arraycopy(previous, 0, input, 0, previous.length);
            System.arraycopy(info, 0, input, previous.length, info.length);
            input[input.length - 1] = (byte) block;
            Arrays.fill(previous, (byte) 0);
            previous = hmac(key, input);
            Arrays.fill(input, (byte) 0);
            int copied = Math.min(previous.length, length - offset);
            System.arraycopy(previous, 0, output, offset, copied);
            offset += copied;
        }
        Arrays.fill(previous, (byte) 0);
        return output;
    }

    private static byte[] hmac(byte[] key, byte[] value) {
        try {
            Mac mac = Mac.getInstance("HmacSHA256");
            mac.init(new SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(value);
        } catch (GeneralSecurityException error) {
            throw new IllegalStateException("HmacSHA256 is required by Java 21", error);
        }
    }
}
