package com.flying.orm.core.internal.hash;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.HexFormat;
import java.util.Objects;

/**
 * 把有标签的值编码为边界明确的稳定摘要输入。
 *
 * <p>每段都使用 UTF-8 标签、显式长度和值；空值使用负长度，与文本 {@code "null"} 不同。
 * 实例是一次性的且不保证线程安全，调用 {@link #finishHex()} 后不可复用。</p>
 *
 * @author wangr
 * @date 2026-08-24
 * @version v3.0
 */
public final class StableEncoder {

    private static final byte[] EMPTY = new byte[0];

    private final MessageDigest digest;
    // 复用小缓冲区合并常用段头；较长标签仍分段写入，不限制输入长度。
    private final byte[] numberBuffer = new byte[32];
    private boolean finished;

    StableEncoder(MessageDigest digest) {
        this.digest = Objects.requireNonNull(digest, "message digest must not be null");
    }

    /** 写入只表示结构边界、不携带值的标记。 */
    public StableEncoder marker(String tag) {
        return bytes(tag, EMPTY);
    }

    /** 写入非空 UTF-8 文本。 */
    public StableEncoder text(String tag, String value) {
        String safeValue = Objects.requireNonNull(value, "stable text must not be null");
        if (safeValue.length() <= Long.BYTES) {
            for (int index = 0; index < safeValue.length(); index++) {
                if (safeValue.charAt(index) > 127) {
                    return bytes(tag, safeValue.getBytes(StandardCharsets.UTF_8));
                }
            }
            ensureOpen();
            beginSegment(tag, safeValue.length());
            for (int index = 0; index < safeValue.length(); index++) {
                numberBuffer[index] = (byte) safeValue.charAt(index);
            }
            digest.update(numberBuffer, 0, safeValue.length());
            return this;
        }
        return bytes(tag, safeValue.getBytes(StandardCharsets.UTF_8));
    }

    /** 写入可空 UTF-8 文本；空值拥有独立编码。 */
    public StableEncoder nullableText(String tag, String value) {
        ensureOpen();
        if (value == null) {
            beginSegment(tag, -1);
            return this;
        }
        return text(tag, value);
    }

    /** 写入布尔值。 */
    public StableEncoder bool(String tag, boolean value) {
        ensureOpen();
        beginSegment(tag, 1);
        digest.update((byte) (value ? 1 : 0));
        return this;
    }

    /** 写入稳定的 64 位有符号整数；类型含义由标签明确表达。 */
    public StableEncoder integer(String tag, long value) {
        ensureOpen();
        beginSegment(tag, Long.BYTES);
        writeLong(value);
        return this;
    }

    /** 写入可空整数；空值与任何数字均不碰撞。 */
    public StableEncoder nullableInteger(String tag, Integer value) {
        if (value == null) {
            ensureOpen();
            beginSegment(tag, -1);
            return this;
        }
        return integer(tag, value);
    }

    /** 写入字节数组。调用返回前已消费数组内容，不保存引用。 */
    public StableEncoder bytes(String tag, byte[] value) {
        ensureOpen();
        byte[] safeValue = Objects.requireNonNull(value, "stable bytes must not be null");
        beginSegment(tag, safeValue.length);
        digest.update(safeValue);
        return this;
    }

    /** 完成摘要并返回小写十六进制；编码器随后失效。 */
    public String finishHex() {
        ensureOpen();
        finished = true;
        return HexFormat.of().formatHex(digest.digest());
    }

    private void beginSegment(String tag, int valueLength) {
        String safeTag = Objects.requireNonNull(tag, "stable tag must not be null");
        if (safeTag.isEmpty()) {
            throw new IllegalArgumentException("stable tag must not be empty");
        }
        if (!writeHeader(safeTag, valueLength)) {
            writeTag(safeTag);
            writeInt(valueLength);
        }
    }

    private boolean writeHeader(String tag, int valueLength) {
        if (tag.length() > numberBuffer.length - 2 * Integer.BYTES) return false;
        for (int index = 0; index < tag.length(); index++) {
            char character = tag.charAt(index);
            if (character > 127) return false;
            numberBuffer[Integer.BYTES + index] = (byte) character;
        }
        putInt(0, tag.length());
        putInt(Integer.BYTES + tag.length(), valueLength);
        digest.update(numberBuffer, 0, 2 * Integer.BYTES + tag.length());
        return true;
    }

    /** 固定 ASCII 标签复用数字缓冲区；其他文本仍由 JDK 保持完整 UTF-8 替换语义。 */
    private void writeTag(String tag) {
        for (int index = 0; index < tag.length(); index++) {
            if (tag.charAt(index) > 127) {
                byte[] bytes = tag.getBytes(StandardCharsets.UTF_8);
                writeInt(bytes.length);
                digest.update(bytes);
                return;
            }
        }
        writeInt(tag.length());
        for (int offset = 0; offset < tag.length();) {
            int length = Math.min(numberBuffer.length, tag.length() - offset);
            for (int index = 0; index < length; index++) {
                numberBuffer[index] = (byte) tag.charAt(offset + index);
            }
            digest.update(numberBuffer, 0, length);
            offset += length;
        }
    }

    private void writeInt(int value) {
        putInt(0, value);
        digest.update(numberBuffer, 0, Integer.BYTES);
    }

    private void putInt(int offset, int value) {
        numberBuffer[offset] = (byte) (value >>> 24);
        numberBuffer[offset + 1] = (byte) (value >>> 16);
        numberBuffer[offset + 2] = (byte) (value >>> 8);
        numberBuffer[offset + 3] = (byte) value;
    }

    private void writeLong(long value) {
        for (int index = Long.BYTES - 1; index >= 0; index--) {
            numberBuffer[index] = (byte) value;
            value >>>= Byte.SIZE;
        }
        digest.update(numberBuffer, 0, Long.BYTES);
    }

    private void ensureOpen() {
        if (finished) {
            throw new IllegalStateException("stable digest is already finished");
        }
    }
}
