package com.flying.orm.rdb.lock;

import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OptimisticLockOptionsValueSnapshotTest {

    @Test
    void assignFreezesMutableValuesAndDoesNotExposeWritableContent() {
        ByteBuffer expected = ByteBuffer.wrap(new byte[]{1});
        ByteBuffer next = ByteBuffer.wrap(new byte[]{2});
        OptimisticLockOptions options = OptimisticLockOptions.assign("version", expected, next);

        expected.put(0, (byte) 9);
        next.put(0, (byte) 8);

        ByteBuffer publishedExpected = (ByteBuffer) options.expectedValue();
        ByteBuffer publishedNext = (ByteBuffer) options.nextValue();
        assertEquals(1, publishedExpected.get(0));
        assertEquals(2, publishedNext.get(0));
        assertTrue(publishedExpected.isReadOnly());
        assertTrue(publishedNext.isReadOnly());
    }
}
