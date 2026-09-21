package com.flying.orm.rdb.protection;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.TermCondition;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;

class ProtectedFieldValueOwnershipTest {

    @Test
    void textCodecReceivesAnIsolatedLogicalTypeBeforeProtection() {
        StringBuilder source = new StringBuilder("secret");
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == StringBuilder.class;
            }

            @Override
            public Object write(Object value) {
                StringBuilder text = (StringBuilder) value;
                String encoded = "encoded:" + text;
                text.setLength(0);
                return encoded;
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                return value;
            }
        });
        assertEquals("encoded:secret", ProtectedFieldValues.encodedText(codecs, source));
        assertEquals("encoded:secret", ProtectedFieldValues.encodedOwnedText(codecs, source));
        assertEquals("secret", source.toString());
    }

    @Test
    void snapshotsReusableWriteValuesBeforeCallingACodec() {
        byte[] source = {1, 2, 3};
        AtomicReference<Object> received = new AtomicReference<>();

        ProtectedFieldValues.encodedText(codecs(received), source);

        assertNotSame(source, received.get());
    }

    @Test
    void doesNotResnapshotOneShotQueryValuesBeforeCallingACodec() {
        byte[] owned = {1, 2, 3};
        AtomicReference<Object> received = new AtomicReference<>();

        ProtectedFieldValues.encodedOwnedText(codecs(received), owned);

        assertSame(owned, received.get());
    }

    @Test
    void snapshotsMutableScalarReturnedByACustomProtectedTerm() {
        ByteBuffer source = ByteBuffer.wrap(new byte[]{1, 2});
        TermCondition term = TermCondition.of("secret", ProtectedConditions.EXACT, source);
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return ByteBuffer.class.isAssignableFrom(targetType);
            }

            @Override
            public Object write(Object value) {
                return Integer.toString(Byte.toUnsignedInt(((ByteBuffer) value).get()));
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                return value;
            }
        });

        assertEquals("1", ProtectedFieldValues.encodedOwnedText(codecs, term.value()));
        assertEquals("1", ProtectedFieldValues.encodedOwnedText(codecs, term.value()));
        assertEquals(0, source.position());
    }

    private static ValueCodecRegistry codecs(AtomicReference<Object> received) {
        return ValueCodecRegistry.standard().withFirst(new ValueCodec() {
            @Override
            public boolean supports(Class<?> targetType) {
                return targetType == byte[].class;
            }

            @Override
            public Object write(Object value) {
                received.set(value);
                return "encoded";
            }

            @Override
            public Object read(Object value, Class<?> targetType) {
                return value;
            }
        });
    }
}
