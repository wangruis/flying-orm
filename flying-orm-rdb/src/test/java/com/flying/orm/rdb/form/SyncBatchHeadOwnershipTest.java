package com.flying.orm.rdb.form;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SyncBatchHeadOwnershipTest {

    @Test
    void stateOwnsItsDownstreamSubscriptionProtocol() {
        assertAll(
                () -> assertTrue(Subscription.class.isAssignableFrom(SyncBatchHeadState.class)),
                () -> assertThrows(ClassNotFoundException.class, () -> Class.forName(
                        "com.flying.orm.rdb.form.SyncBatchHeadSubscription")));
    }
}
