package com.flying.orm.core.codec;

import java.nio.ByteBuffer;

/** 二进制读取只复制 ByteBuffer 当前可读区间，并且不移动原 buffer 的 position。 */
final class BinaryValueCodec implements ValueCodec {

    @Override
    public boolean supports(Class<?> targetType) {
        // 写入按运行时实现类查找 codec，必须接受 HeapByteBuffer 等子类；读取仍在下方只承诺精确 ByteBuffer。
        return targetType == byte[].class || targetType == Byte[].class
                || ByteBuffer.class.isAssignableFrom(targetType);
    }

    @Override
    public Object write(Object value) {
        return value instanceof Byte[] boxed ? toPrimitive(boxed) : value;
    }

    @Override
    public Object read(Object value, Class<?> targetType) {
        if (targetType == byte[].class && value instanceof ByteBuffer buffer) {
            ByteBuffer readable = buffer.duplicate();
            byte[] bytes = new byte[readable.remaining()];
            readable.get(bytes);
            return bytes;
        }
        if (targetType == byte[].class && value instanceof Byte[] boxed) {
            return toPrimitive(boxed);
        }
        if (targetType == Byte[].class && value instanceof byte[] bytes) {
            return toBoxed(bytes);
        }
        if (targetType == Byte[].class && value instanceof ByteBuffer buffer) {
            return toBoxed((byte[]) read(buffer, byte[].class));
        }
        if (targetType == ByteBuffer.class && value instanceof byte[] bytes) {
            return ByteBuffer.wrap(bytes.clone());
        }
        if (targetType == ByteBuffer.class && value instanceof Byte[] boxed) {
            return ByteBuffer.wrap(toPrimitive(boxed));
        }
        throw new IllegalArgumentException("binary value cannot be converted from " + value.getClass().getName()
                                                   + " to " + targetType.getName());
    }

    /** boxed 二进制不能包含数据库无法表达的元素级 null。 */
    private static byte[] toPrimitive(Byte[] source) {
        byte[] target = new byte[source.length];
        for (int index = 0; index < source.length; index++) {
            Byte value = source[index];
            if (value == null) {
                throw new IllegalArgumentException("boxed binary value must not contain null");
            }
            target[index] = value;
        }
        return target;
    }

    private static Byte[] toBoxed(byte[] source) {
        Byte[] target = new Byte[source.length];
        for (int index = 0; index < source.length; index++) {
            target[index] = source[index];
        }
        return target;
    }
}
