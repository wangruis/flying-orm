package com.flying.orm.rdb.execution;

import java.nio.ByteBuffer;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.RandomAccess;
import java.util.function.IntFunction;

/**
 * 持有保护字段搜索令牌的内部不可变存储。
 *
 * <p>公开列表访问始终返回数组副本，包内执行链只获取独立只读视图，避免重复复制工作计划已经拥有的令牌。</p>
 *
 * @author wangr
 * @date 2026-08-30
 * @version v1.0
 */
final class ProtectedTokenStorage extends AbstractList<byte[]> implements RandomAccess {

    private static final long COLLECTION_OVERHEAD = 24L;
    private static final long REFERENCE_BYTES = 8L;
    private static final long TOKEN_OVERHEAD = 40L;

    private final ByteBuffer[] values;

    private ProtectedTokenStorage(ByteBuffer[] values) {
        this.values = values;
    }

    static ProtectedTokenStorage snapshot(List<byte[]> values) {
        List<byte[]> safeValues = Objects.requireNonNull(values, "protected tokens must not be null");
        if (safeValues instanceof ProtectedTokenStorage storage) {
            return storage;
        }
        ByteBuffer[] owned = new ByteBuffer[safeValues.size()];
        for (int index = 0; index < safeValues.size(); index++) {
            byte[] token = Objects.requireNonNull(
                    safeValues.get(index), "protected token must not be null");
            owned[index] = ByteBuffer.wrap(token.clone()).asReadOnlyBuffer();
        }
        return new ProtectedTokenStorage(owned);
    }

    static ProtectedTokenStorage owned(int tokenCount, IntFunction<ByteBuffer> tokenAt) {
        if (tokenCount < 0) {
            throw new IllegalArgumentException("protected token count must not be negative");
        }
        IntFunction<ByteBuffer> safeTokenAt = Objects.requireNonNull(
                tokenAt, "protected token access must not be null");
        ByteBuffer[] owned = new ByteBuffer[tokenCount];
        for (int index = 0; index < tokenCount; index++) {
            ByteBuffer token = Objects.requireNonNull(
                    safeTokenAt.apply(index), "protected token must not be null");
            owned[index] = token.slice().asReadOnlyBuffer();
        }
        return new ProtectedTokenStorage(owned);
    }

    @Override
    public byte[] get(int index) {
        return copy(index);
    }

    @Override
    public int size() {
        return values.length;
    }

    ByteBuffer readOnlyBuffer(int index) {
        return values[index].asReadOnlyBuffer();
    }

    List<byte[]> publicCopy() {
        List<byte[]> copy = new ArrayList<>(values.length);
        for (int index = 0; index < values.length; index++) {
            copy.add(copy(index));
        }
        return List.copyOf(copy);
    }

    long estimatedBytes() {
        long total = saturatedAdd(COLLECTION_OVERHEAD, values.length * REFERENCE_BYTES);
        for (ByteBuffer value : values) {
            total = saturatedAdd(total, TOKEN_OVERHEAD + value.capacity());
        }
        return total;
    }

    @Override
    public boolean equals(Object other) {
        if (this == other) {
            return true;
        }
        if (!(other instanceof List<?> list) || list.size() != values.length) {
            return false;
        }
        for (int index = 0; index < values.length; index++) {
            Object value = list.get(index);
            if (!(value instanceof byte[] bytes) || !contentEquals(values[index], bytes)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        int hash = 1;
        for (ByteBuffer value : values) {
            int tokenHash = 1;
            ByteBuffer readable = value.asReadOnlyBuffer();
            while (readable.hasRemaining()) {
                tokenHash = 31 * tokenHash + readable.get();
            }
            hash = 31 * hash + tokenHash;
        }
        return hash;
    }

    private byte[] copy(int index) {
        ByteBuffer readable = readOnlyBuffer(index);
        byte[] copy = new byte[readable.remaining()];
        readable.get(copy);
        return copy;
    }

    private static boolean contentEquals(ByteBuffer value, byte[] bytes) {
        ByteBuffer readable = value.asReadOnlyBuffer();
        if (readable.remaining() != bytes.length) {
            return false;
        }
        for (byte item : bytes) {
            if (readable.get() != item) {
                return false;
            }
        }
        return true;
    }

    private static long saturatedAdd(long left, long right) {
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }
}
