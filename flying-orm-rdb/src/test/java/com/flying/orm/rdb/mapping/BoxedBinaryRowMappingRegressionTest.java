package com.flying.orm.rdb.mapping;

import com.flying.orm.core.codec.ValueCodecRegistry;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class BoxedBinaryRowMappingRegressionTest {

    @Test
    void recordReadsBoxedBinaryFromReadableBufferWithoutMovingIt() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{9, 1, 2, 8});
        buffer.position(1);
        buffer.limit(3);

        BinaryRecord value = RowMapper.of(BinaryRecord.class).map(Map.of("payload", buffer));

        assertArrayEquals(new Byte[]{1, 2}, value.payload());
        assertEquals(1, buffer.position());
        assertEquals(3, buffer.limit());
    }

    @Test
    void beanReadsBoxedBinaryFromReadOnlyBuffer() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{3, 4}).asReadOnlyBuffer();

        BinaryBean value = RowMapper.of(BinaryBean.class).map(Map.of("payload", buffer));

        assertArrayEquals(new Byte[]{3, 4}, value.getPayload());
        assertEquals(0, buffer.position());
    }

    @Test
    void emptyBufferRemainsEmptyBinary() {
        BinaryRecord value = RowMapper.of(BinaryRecord.class)
                .map(Map.of("payload", ByteBuffer.allocate(0)));

        assertArrayEquals(new Byte[0], value.payload());
    }

    @Test
    void primitiveBytesStillMapToBoxedBinary() {
        BinaryRecord value = RowMapper.of(BinaryRecord.class)
                .map(Map.of("payload", new byte[]{-1, 0, 127}));

        assertArrayEquals(new Byte[]{-1, 0, 127}, value.payload());
    }

    @Test
    void nullBinaryRemainsNull() {
        BinaryRecord value = RowMapper.of(BinaryRecord.class)
                .map(Collections.singletonMap("payload", null));

        assertNull(value.payload());
    }

    @Test
    void primitiveBinaryAndTextArraysKeepScalarMapping() {
        ScalarRecord value = RowMapper.of(ScalarRecord.class)
                .map(Map.of("payload", ByteBuffer.wrap(new byte[]{5}), "text", "ab"));

        assertArrayEquals(new byte[]{5}, value.payload());
        assertArrayEquals(new char[]{'a', 'b'}, value.text());
    }

    @Test
    void existingBinaryCodecAlreadySupportsTheDriverCarrier() {
        assertArrayEquals(new Byte[]{6, 7}, ValueCodecRegistry.standard()
                .read(ByteBuffer.wrap(new byte[]{6, 7}), Byte[].class));
    }

    record BinaryRecord(Byte[] payload) { }

    record ScalarRecord(byte[] payload, char[] text) { }

    static class BinaryBean {
        private Byte[] payload;

        public Byte[] getPayload() {
            return payload;
        }

        public void setPayload(Byte[] payload) {
            this.payload = payload;
        }
    }
}
