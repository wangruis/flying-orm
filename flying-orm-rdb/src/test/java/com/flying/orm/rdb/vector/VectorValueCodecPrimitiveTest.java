package com.flying.orm.rdb.vector;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class VectorValueCodecPrimitiveTest {

    @Test
    void floatArrayWriteAndReadPreserveExactValuesAndOwnTheirResults() {
        float negativeZero = Float.intBitsToFloat(0x8000_0000);
        float smallestSubnormal = Float.intBitsToFloat(0x0000_0001);
        float[] source = {negativeZero, smallestSubnormal, 1.25F};

        float[] written = VectorValueCodec.write(source, 3);
        float[] read = VectorValueCodec.read(source, 3);

        assertNotSame(source, written);
        assertNotSame(source, read);
        assertRawBitsEqual(source, written);
        assertRawBitsEqual(source, read);
        source[0] = 9F;
        source[1] = 8F;
        assertEquals(0x8000_0000, Float.floatToRawIntBits(written[0]));
        assertEquals(0x0000_0001, Float.floatToRawIntBits(read[1]));
    }

    @Test
    void floatArrayKeepsFiniteAndDimensionGuards() {
        assertThrows(IllegalArgumentException.class, () -> VectorValueCodec.write(new float[] {Float.NaN}, 1));
        assertThrows(IllegalArgumentException.class,
                     () -> VectorValueCodec.read(new float[] {Float.POSITIVE_INFINITY}, 1));
        assertThrows(IllegalArgumentException.class, () -> VectorValueCodec.write(new float[0], null));
        assertThrows(IllegalArgumentException.class,
                     () -> VectorValueCodec.write(new float[VectorValueCodec.MAX_DIMENSIONS + 1], null));
        assertThrows(IllegalArgumentException.class, () -> VectorValueCodec.write(new float[] {1F, 2F}, 1));
    }

    @Test
    void objectArraysAndCollectionsKeepTheirExistingConversionRules() {
        assertArrayEquals(new float[] {1F, 2.5F}, VectorValueCodec.write(new Number[] {1, 2.5D}, 2));
        assertArrayEquals(new float[] {3F, 4.5F}, VectorValueCodec.write(List.of(3D, 4.5D), 2));
    }

    private static void assertRawBitsEqual(float[] expected, float[] actual) {
        assertEquals(expected.length, actual.length);
        for (int index = 0; index < expected.length; index++) {
            assertEquals(Float.floatToRawIntBits(expected[index]), Float.floatToRawIntBits(actual[index]));
        }
    }
}
