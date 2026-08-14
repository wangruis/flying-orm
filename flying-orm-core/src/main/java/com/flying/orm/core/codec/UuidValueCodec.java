package com.flying.orm.core.codec;

import java.util.UUID;

/** UUID 本身可被常见驱动直接绑定，文本读取时再解析。 */
final class UuidValueCodec implements ValueCodec {

    @Override
    public boolean supports(Class<?> targetType) {
        return targetType == UUID.class;
    }

    @Override
    public Object read(Object value, Class<?> targetType) {
        try {
            return UUID.fromString(ValueCodecTypeSupport.text(value));
        } catch (IllegalArgumentException failure) {
            ValueCodecTypeSupport.rethrowVirtualMachineError(failure);
            throw new IllegalArgumentException("value cannot be converted to UUID");
        }
    }
}
