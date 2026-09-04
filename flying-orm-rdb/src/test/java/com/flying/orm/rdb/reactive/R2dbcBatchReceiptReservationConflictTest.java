package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

class R2dbcBatchReceiptReservationConflictTest {

    @Test
    void recoversTheInternalConflictMarkerFromATransactionCauseChain() {
        RdbException duplicateKey = new RdbException(RdbErrorKind.DUPLICATE_KEY,
                                                     "duplicate key",
                                                     "23505",
                                                     null,
                                                     new IllegalStateException("driver failure"));
        Throwable marker = R2dbcBatchReceiptReservationConflict.classify(duplicateKey);
        assertInstanceOf(R2dbcBatchReceiptReservationConflict.class, marker);

        RuntimeException transactionFailure = new RuntimeException("transaction failed", marker);

        assertSame(marker, R2dbcBatchReceiptReservationConflict.find(transactionFailure));
    }
}
