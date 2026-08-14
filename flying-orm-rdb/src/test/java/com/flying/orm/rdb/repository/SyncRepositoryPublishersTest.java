package com.flying.orm.rdb.repository;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 验证同步 Repository 集合批量入口的订阅边界。 */
class SyncRepositoryPublishersTest {

    /** 故障 Iterable 不得从 subscribe 直接抛出并绕过 onError。 */
    @Test
    void reportsIteratorFailureAfterSubscriptionInsteadOfThrowingFromSubscribe() {
        AtomicBoolean subscribed = new AtomicBoolean();
        AtomicReference<Throwable> error = new AtomicReference<>();

        assertDoesNotThrow(() -> SyncRepositoryPublishers.fromIterable(() -> {
            throw new IllegalStateException("iterator unavailable");
        }).subscribe(new Subscriber<Object>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                subscribed.set(true);
                subscription.request(1);
            }

            @Override
            public void onNext(Object value) {
                throw new AssertionError("iterator failure must not emit a row");
            }

            @Override
            public void onError(Throwable failure) {
                error.set(failure);
            }

            @Override
            public void onComplete() {
                throw new AssertionError("iterator failure must not complete");
            }
        }));

        assertTrue(subscribed.get());
        assertInstanceOf(IllegalStateException.class, error.get());
    }
    /** Repository 集合批量的普通 Error 必须封闭本地订阅，后续 demand 不能再次读取损坏迭代器。 */
    @Test
    void terminatesIterableSubscriptionWhenIteratorThrowsError() {
        AtomicInteger nextCalls = new AtomicInteger();
        AtomicReference<Subscription> downstream = new AtomicReference<>();
        AssertionError failure = new AssertionError("iterator error");

        SyncRepositoryPublishers.fromIterable(() -> new java.util.Iterator<>() {
            @Override public boolean hasNext() { return true; }

            @Override
            public Object next() {
                nextCalls.incrementAndGet();
                throw failure;
            }
        }).subscribe(new Subscriber<>() {
            @Override public void onSubscribe(Subscription subscription) { downstream.set(subscription); }
            @Override public void onNext(Object value) { }
            @Override public void onError(Throwable error) { }
            @Override public void onComplete() { }
        });

        Throwable observed = null;
        try {
            downstream.get().request(1);
        } catch (Throwable error) {
            observed = error;
        }

        assertSame(failure, observed);
        assertDoesNotThrow(() -> downstream.get().request(1));
        assertEquals(1, nextCalls.get());
    }
}
