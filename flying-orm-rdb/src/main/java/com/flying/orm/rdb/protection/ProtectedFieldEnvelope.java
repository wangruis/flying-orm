package com.flying.orm.rdb.protection;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/** 解析和编码有界字段密文信封。 */
final class ProtectedFieldEnvelope {

    static final int MAGIC = 0x464f5031;
    static final int NONCE_LENGTH = 12;
    private static final int MAX_CIPHERTEXT_LENGTH = 1_048_592;

    private ProtectedFieldEnvelope() {
    }

    static byte[] encode(String keyVersion, byte[] nonce, byte[] ciphertext) {
        byte[] version = keyVersion.getBytes(StandardCharsets.US_ASCII);
        ByteBuffer target = ByteBuffer.allocate(9 + version.length + nonce.length + ciphertext.length);
        target.putInt(MAGIC).put((byte) version.length).put(version).put(nonce)
              .putInt(ciphertext.length).put(ciphertext);
        return target.array();
    }

    static Parsed parse(byte[] envelope) {
        try {
            Header header = header(envelope);
            byte[] ciphertext = new byte[header.ciphertextLength()];
            header.source().get(ciphertext);
            return new Parsed(header.keyVersion(), header.nonce(), ciphertext);
        } catch (ProtectedFieldException error) {
            throw error;
        } catch (RuntimeException error) {
            throw new ProtectedFieldException(error);
        }
    }

    static String keyVersion(byte[] envelope) {
        try {
            return header(envelope).keyVersion();
        } catch (ProtectedFieldException error) {
            throw error;
        } catch (RuntimeException error) {
            throw new ProtectedFieldException(error);
        }
    }

    /** 校验完整信封形状，但只复制固定大小的版本和 nonce；版本探测不复制大密文。 */
    private static Header header(byte[] envelope) {
        if (envelope == null || envelope.length < 9 + NONCE_LENGTH + 16
                || envelope.length > MAX_CIPHERTEXT_LENGTH + 64) {
            throw new ProtectedFieldException();
        }
        ByteBuffer source = ByteBuffer.wrap(envelope);
        if (source.getInt() != MAGIC) {
            throw new ProtectedFieldException();
        }
        int versionLength = Byte.toUnsignedInt(source.get());
        if (versionLength < 1 || versionLength > 16
                || source.remaining() < versionLength + NONCE_LENGTH + 4 + 16) {
            throw new ProtectedFieldException();
        }
        byte[] version = new byte[versionLength];
        source.get(version);
        byte[] nonce = new byte[NONCE_LENGTH];
        source.get(nonce);
        int ciphertextLength = source.getInt();
        if (ciphertextLength < 16 || ciphertextLength > MAX_CIPHERTEXT_LENGTH
                || source.remaining() != ciphertextLength) {
            throw new ProtectedFieldException();
        }
        return new Header(new String(version, StandardCharsets.US_ASCII), nonce, ciphertextLength, source);
    }

    record Parsed(String keyVersion, byte[] nonce, byte[] ciphertext) {
    }

    private record Header(String keyVersion, byte[] nonce, int ciphertextLength, ByteBuffer source) {
    }
}
