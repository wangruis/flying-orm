package com.flying.orm.rdb.codec;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class LargeObjectValueOwnershipTest {

    @Test
    void oracleBlobOwnsTheBorrowedByteArray() {
        byte[] input = {1, 2};
        SqlTypedValue value = assertInstanceOf(SqlTypedValue.class,
                LargeObjectValueCodec.write(input, "VARBINARY", "oracle"));
        input[0] = 9;
        assertArrayEquals(new byte[]{1, 2}, (byte[]) value.value());
        ((byte[]) value.value())[1] = 8;
        assertArrayEquals(new byte[]{9, 2}, input);
    }

    @Test
    void oracleBlobOwnsOnlyTheReadableBufferRange() {
        byte[] input = {0, 1, 2, 3};
        ByteBuffer buffer = ByteBuffer.wrap(input).position(1).limit(3);
        SqlTypedValue value = assertInstanceOf(SqlTypedValue.class,
                LargeObjectValueCodec.write(buffer, "BLOB", "oracle"));
        input[1] = 9;
        ByteBuffer owned = assertInstanceOf(ByteBuffer.class, value.value());
        byte[] actual = new byte[owned.remaining()];
        owned.get(actual);
        assertArrayEquals(new byte[]{1, 2}, actual);
        assertEquals(1, buffer.position());
        assertEquals(3, buffer.limit());
    }

    @Test
    void oracleBoxedBinaryAndTextAlreadyBecomeOwnedValues() {
        Byte[] binary = {1, 2};
        SqlTypedValue blob = (SqlTypedValue) LargeObjectValueCodec.write(binary, "BLOB", "oracle");
        binary[0] = 9;
        assertArrayEquals(new byte[]{1, 2}, (byte[]) blob.value());
        for (String type : new String[]{"CLOB", "NCLOB"}) {
            StringBuilder text = new StringBuilder("before");
            SqlTypedValue clob = (SqlTypedValue) LargeObjectValueCodec.write(text, type, "oracle");
            text.append(" after");
            assertEquals("before", clob.value());
        }
    }
}
