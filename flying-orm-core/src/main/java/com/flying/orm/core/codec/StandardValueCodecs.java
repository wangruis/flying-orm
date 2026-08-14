package com.flying.orm.core.codec;

import java.util.List;

/** 内置 codec 的固定顺序表，越靠前优先级越高。 */
final class StandardValueCodecs {

    private StandardValueCodecs() {
    }

    static List<ValueCodec> create() {
        return List.of(new EnumValueCodec(),
                       new BooleanValueCodec(),
                       new NumberValueCodec(),
                       new JavaTimeValueCodec(),
                       new UuidValueCodec(),
                       new BinaryValueCodec(),
                       new TextValueCodec(),
                       new ArrayIdentityValueCodec(),
                       new IdentityValueCodec());
    }
}
