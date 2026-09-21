package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;

/** 已完成字段/方言转换的条件值；专用 codec 保证应用 codec 不会再次改写它。 */
record FormEncodedConditionValue(Object value) {

    static ValueCodecRegistry registerWith(ValueCodecRegistry codecs) {
        return codecs.withFirst(Codec.INSTANCE);
    }

    private enum Codec implements ValueCodec {
        INSTANCE;

        @Override
        public boolean supports(Class<?> targetType) {
            return targetType == FormEncodedConditionValue.class;
        }

        @Override
        public Object write(Object value) {
            return ((FormEncodedConditionValue) value).value();
        }

        @Override
        public Object read(Object value, Class<?> targetType) {
            return ((FormEncodedConditionValue) value).value();
        }
    }
}
