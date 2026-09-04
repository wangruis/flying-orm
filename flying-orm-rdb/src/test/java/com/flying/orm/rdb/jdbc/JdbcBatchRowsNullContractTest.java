package com.flying.orm.rdb.jdbc;

import com.flying.orm.rdb.execution.ProtectedBatchRows;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class JdbcBatchRowsNullContractTest {

    @Test
    void rejectsNullRowAtSubscriberBoundary() {
        JdbcBatchRows rows = new JdbcBatchRows(subscriber -> {
        }, 0, 4096L);

        assertThrows(NullPointerException.class, () -> rows.onNext(null));
    }

    @Test
    void rejectsNullFailureEvenAfterTermination() {
        JdbcBatchRows rows = new JdbcBatchRows(subscriber -> {
        }, 0, 4096L);
        rows.onComplete();

        assertThrows(NullPointerException.class, () -> rows.onError(null));
    }

    @Test
    void carriesTheOwnedRowAndItsEstimateFromTheSubscriberBoundary() throws Exception {
        byte[] payload = {1, 2};
        Object[] row = {payload};
        JdbcBatchRows rows = new JdbcBatchRows(subscriber -> subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long ignored) {
                subscriber.onNext(row);
                subscriber.onComplete();
            }

            @Override
            public void cancel() {
            }
        }), 1, 4096L);

        ProtectedBatchRows.RowView snapshot = rows.nextRowView(Duration.ZERO);

        assertSame(row, snapshot.row());
        assertSame(payload, snapshot.row()[0]);
        assertTrue(snapshot.estimatedBytes() > 0L);
    }
}
