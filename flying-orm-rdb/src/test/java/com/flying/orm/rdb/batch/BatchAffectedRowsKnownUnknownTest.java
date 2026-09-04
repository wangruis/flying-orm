package com.flying.orm.rdb.batch;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class BatchAffectedRowsKnownUnknownTest {

    @Test
    void keepsKnownZeroDistinctFromUnknown() {
        BatchAffectedRows zero = BatchAffectedRows.known(0);
        BatchAffectedRows unknown = BatchAffectedRows.unknown();

        assertTrue(zero.isKnown());
        assertEquals(0L, zero.value());
        assertFalse(unknown.isKnown());
        assertThrows(IllegalStateException.class, unknown::value);
        assertEquals(BatchAffectedRows.known(0), zero);
        assertEquals(BatchAffectedRows.unknown(), unknown);
    }

    @Test
    void rejectsNegativeKnownCounts() {
        assertThrows(IllegalArgumentException.class, () -> BatchAffectedRows.known(-1));
    }
}
