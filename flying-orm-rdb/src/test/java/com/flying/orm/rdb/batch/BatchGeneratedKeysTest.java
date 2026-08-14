package com.flying.orm.rdb.batch;

import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 验证批量生成键契约只接受安全列名，并保持普通批量的零开销默认值。 */
class BatchGeneratedKeysTest {

    @Test
    void ordinaryBatchDoesNotRequestGeneratedKeys() {
        assertFalse(BatchGeneratedKeys.none().required());
        assertThrows(IllegalStateException.class, () -> BatchGeneratedKeys.none().columnName());
    }

    @Test
    void requiredKeysValidateIdentifierAndDeliverGlobalOffset() {
        AtomicLong deliveredOffset = new AtomicLong(-1L);
        BatchGeneratedKeys keys = BatchGeneratedKeys.required(
                "device_id", (offset, row) -> deliveredOffset.set(offset));

        keys.accept(7L, DynamicRow.copyOf(Map.of("device_id", 101L)));

        assertEquals("device_id", keys.columnName());
        assertEquals(7L, deliveredOffset.get());
        assertThrows(IllegalArgumentException.class,
                     () -> BatchGeneratedKeys.required("device_id) returning secret", (offset, row) -> { }));
    }
}
