package com.flying.orm.rdb.form;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SyncBatchHeadStateTest {

    @Test
    void doesNotSignalCompletionAfterSynchronousCancellation() {
        SyncBatchHeadState<String> state = new SyncBatchHeadState<>();
        state.onComplete();
        AtomicInteger completions = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();

        state.subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.cancel();
            }

            @Override
            public void onNext(String value) {
            }

            @Override
            public void onError(Throwable error) {
                failures.incrementAndGet();
            }

            @Override
            public void onComplete() {
                completions.incrementAndGet();
            }
        });

        assertEquals(0, completions.get());
        assertEquals(0, failures.get());
    }

    @Test
    void doesNotSignalCompletionWhenFirstRowCallbackCancels() {
        SyncBatchHeadState<String> state = new SyncBatchHeadState<>();
        state.onNext("first");
        state.onComplete();
        AtomicInteger rows = new AtomicInteger();
        AtomicInteger completions = new AtomicInteger();

        state.subscribe(new Subscriber<>() {
            private Subscription subscription;

            @Override
            public void onSubscribe(Subscription candidate) {
                subscription = candidate;
                candidate.request(1);
            }

            @Override
            public void onNext(String value) {
                rows.incrementAndGet();
                subscription.cancel();
            }

            @Override
            public void onError(Throwable error) {
            }

            @Override
            public void onComplete() {
                completions.incrementAndGet();
            }
        });

        assertEquals(1, rows.get());
        assertEquals(0, completions.get());
    }

    @Test
    void ignoresInvalidDemandAfterCancellation() {
        SyncBatchHeadState<String> state = new SyncBatchHeadState<>();
        AtomicInteger failures = new AtomicInteger();

        state.subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                subscription.cancel();
                subscription.request(0);
            }

            @Override
            public void onNext(String value) {
            }

            @Override
            public void onError(Throwable error) {
                failures.incrementAndGet();
            }

            @Override
            public void onComplete() {
            }
        });

        assertEquals(0, failures.get());
    }

    @Test
    void rejectsNullRowEvenAfterClosure() {
        SyncBatchHeadState<String> state = new SyncBatchHeadState<>();
        state.close();

        assertThrows(NullPointerException.class, () -> state.onNext(null));
    }

    @Test
    void rejectsNullFailureEvenAfterTermination() {
        SyncBatchHeadState<String> state = new SyncBatchHeadState<>();
        state.close();

        assertThrows(NullPointerException.class, () -> state.onError(null));
    }
}
