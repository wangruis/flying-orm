package com.flying.orm.rdb.form;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 验证同步集合批量入口仍遵守 Reactive Streams 订阅信号顺序。 */
class BatchPublishersTest {

    /** 重复的上游订阅必须立即取消，不能把第二个订阅继续暴露给下游。 */
    @Test
    void cancelsDuplicateIndexedMapperSubscription() {
        AtomicInteger downstreamSubscriptions = new AtomicInteger();
        AtomicInteger duplicateCancellations = new AtomicInteger();
        Subscription first = new Subscription() {
            @Override public void request(long count) { }
            @Override public void cancel() { }
        };
        Subscription duplicate = new Subscription() {
            @Override public void request(long count) { }
            @Override public void cancel() { duplicateCancellations.incrementAndGet(); }
        };

        BatchPublishers.mapIndexed(subscriber -> {
            subscriber.onSubscribe(first);
            subscriber.onSubscribe(duplicate);
        }, (String value, long index) -> value).subscribe(new Subscriber<>() {
            @Override public void onSubscribe(Subscription subscription) {
                downstreamSubscriptions.incrementAndGet();
            }
            @Override public void onNext(String value) { }
            @Override public void onError(Throwable error) { }
            @Override public void onComplete() { }
        });

        assertEquals(1, downstreamSubscriptions.get());
        assertEquals(1, duplicateCancellations.get());
    }

    /** Iterable 在取得迭代器时失败，也必须先建立订阅再向下游发出错误。 */
    @Test
    void reportsIteratorFailureAfterSubscriptionInsteadOfThrowingFromSubscribe() {
        AtomicBoolean subscribed = new AtomicBoolean();
        AtomicReference<Throwable> error = new AtomicReference<>();

        assertDoesNotThrow(() -> BatchPublishers.fromIterable(() -> {
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

    /** mapper 的 VM 失败仍须取消已建立的上游订阅，不能只依赖驱动或 Reactor 的后续清理。 */
    /** 普通 Error 与 VME 一样必须终止 mapper 订阅并取消上游，不能在下一次 demand 时继续映射。 */
    @Test
    void cancelsSourceAndStopsFurtherDemandWhenIndexedMapperThrowsError() {
        AtomicBoolean cancelled = new AtomicBoolean();
        AtomicInteger mappings = new AtomicInteger();
        AtomicReference<Subscription> downstream = new AtomicReference<>();
        AssertionError failure = new AssertionError("mapper error");

        BatchPublishers.mapIndexed(subscriber -> subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long count) {
                if (count > 0L && !cancelled.get()) {
                    subscriber.onNext("row");
                }
            }

            @Override
            public void cancel() {
                cancelled.set(true);
            }
        }), (String value, long index) -> {
            mappings.incrementAndGet();
            throw failure;
        }).subscribe(recordingSubscriber(downstream));

        Throwable observed = null;
        try {
            downstream.get().request(1);
        } catch (Throwable error) {
            observed = error;
        }

        assertSame(failure, observed);
        assertTrue(cancelled.get());
        assertEquals(1, mappings.get());
        assertDoesNotThrow(() -> downstream.get().request(1));
        assertEquals(1, mappings.get());
    }

    /** 本地 Iterable 抛普通 Error 后也必须封闭订阅，避免后续 demand 反复触发同一个损坏迭代器。 */
    @Test
    void terminatesIterableSubscriptionWhenIteratorThrowsError() {
        AtomicInteger nextCalls = new AtomicInteger();
        AtomicReference<Subscription> downstream = new AtomicReference<>();
        AssertionError failure = new AssertionError("iterator error");

        BatchPublishers.fromIterable(() -> new java.util.Iterator<>() {
            @Override public boolean hasNext() { return true; }

            @Override
            public Object next() {
                nextCalls.incrementAndGet();
                throw failure;
            }
        }).subscribe(recordingSubscriber(downstream));

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

    @Test
    void cancelsSourceWhenIndexedMapperThrowsVirtualMachineError() {
        AtomicInteger cancellations = new AtomicInteger();
        AtomicReference<Subscription> downstream = new AtomicReference<>();
        OutOfMemoryError fatal = new OutOfMemoryError("mapper fatal");

        BatchPublishers.mapIndexed(subscriber -> subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long count) {
                if (count > 0L) {
                    subscriber.onNext("row");
                }
            }

            @Override
            public void cancel() {
                cancellations.incrementAndGet();
            }
        }), (String value, long index) -> {
            throw fatal;
        }).subscribe(new Subscriber<Object>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                downstream.set(subscription);
            }

            @Override public void onNext(Object value) { }
            @Override public void onError(Throwable error) { }
            @Override public void onComplete() { }
        });

        AtomicReference<OutOfMemoryError> observed = new AtomicReference<>();
        try {
            downstream.get().request(1);
        } catch (OutOfMemoryError error) {
            observed.set(error);
        }

        assertSame(fatal, observed.get());
        assertEquals(1, cancellations.get());
    }

    /** Iterable 在 VM 失败后必须终止本订阅，后续 demand 不得重新触发同一有害迭代。 */
    @Test
    void terminatesIterableSubscriptionWhenIteratorThrowsVirtualMachineError() {
        AtomicInteger nextCalls = new AtomicInteger();
        AtomicReference<Subscription> downstream = new AtomicReference<>();
        OutOfMemoryError fatal = new OutOfMemoryError("iterator fatal");

        BatchPublishers.fromIterable(() -> new java.util.Iterator<>() {
            @Override public boolean hasNext() { return true; }

            @Override
            public Object next() {
                nextCalls.incrementAndGet();
                throw fatal;
            }
        }).subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription subscription) {
                downstream.set(subscription);
            }

            @Override public void onNext(Object value) { }
            @Override public void onError(Throwable error) { }
            @Override public void onComplete() { }
        });

        AtomicReference<OutOfMemoryError> observed = new AtomicReference<>();
        try {
            downstream.get().request(1);
        } catch (OutOfMemoryError error) {
            observed.set(error);
        }

        assertSame(fatal, observed.get());
        AtomicReference<OutOfMemoryError> secondFailure = new AtomicReference<>();
        try {
            downstream.get().request(1);
        } catch (OutOfMemoryError error) {
            secondFailure.set(error);
        }
        assertNull(secondFailure.get());
        assertEquals(1, nextCalls.get());
    }

    /** mapper 的 primary VME 不能被 cancel 的普通清理失败覆盖，且诊断边不得反向成环。 */
    @Test
    void retainsPrimaryVirtualMachineErrorWhenMapperCancellationFailsNormally() {
        OutOfMemoryError primary = new OutOfMemoryError("mapper fatal");
        IllegalStateException cleanup = new IllegalStateException("cancel failed");
        AtomicReference<Subscription> downstream = new AtomicReference<>();

        BatchPublishers.mapIndexed(subscriber -> subscriber.onSubscribe(new Subscription() {
            @Override public void request(long count) { if (count > 0L) subscriber.onNext("row"); }
            @Override public void cancel() { throw cleanup; }
        }), (String value, long index) -> { throw primary; })
                       .subscribe(recordingSubscriber(downstream));

        AtomicReference<Throwable> observed = new AtomicReference<>();
        try {
            downstream.get().request(1);
        } catch (Throwable error) {
            observed.set(error);
        }

        assertSame(primary, observed.get());
        assertTrue(reaches(primary, cleanup));
        assertFalse(reaches(cleanup, primary));
    }

    /** cancel 的 VME 必须提升为主异常，普通 mapper 失败仍以无环 suppressed 上下文保留。 */
    @Test
    void promotesCancellationVirtualMachineErrorWhenMapperFailsNormally() {
        IllegalStateException primary = new IllegalStateException("mapper failed");
        OutOfMemoryError cleanup = new OutOfMemoryError("cancel fatal");
        AtomicReference<Subscription> downstream = new AtomicReference<>();

        BatchPublishers.mapIndexed(subscriber -> subscriber.onSubscribe(new Subscription() {
            @Override public void request(long count) { if (count > 0L) subscriber.onNext("row"); }
            @Override public void cancel() { throw cleanup; }
        }), (String value, long index) -> { throw primary; })
                       .subscribe(recordingSubscriber(downstream));

        AtomicReference<Throwable> observed = new AtomicReference<>();
        try {
            downstream.get().request(1);
        } catch (Throwable error) {
            observed.set(error);
        }

        assertSame(cleanup, observed.get());
        assertTrue(reaches(cleanup, primary));
        assertFalse(reaches(primary, cleanup));
    }

    /** 两个普通失败时不能让 cancel 覆盖 mapper primary，仍需按既有 Reactive Streams 语义通知下游。 */
    /** mapper 用普通异常包装 VME 时仍必须提升原致命错误，不能降级为下游普通 onError。 */
    @Test
    void promotesVirtualMachineErrorNestedInMapperFailure() {
        OutOfMemoryError fatal = new OutOfMemoryError("mapper nested fatal");
        IllegalStateException wrapper = new IllegalStateException("mapper wrapper", fatal);
        AtomicReference<Subscription> downstream = new AtomicReference<>();
        AtomicReference<Throwable> downstreamFailure = new AtomicReference<>();
        AtomicBoolean cancelled = new AtomicBoolean();

        BatchPublishers.mapIndexed(subscriber -> subscriber.onSubscribe(new Subscription() {
            @Override public void request(long count) { if (count > 0L) subscriber.onNext("row"); }
            @Override public void cancel() { cancelled.set(true); }
        }), (String value, long index) -> { throw wrapper; })
                       .subscribe(new Subscriber<>() {
                           @Override public void onSubscribe(Subscription value) { downstream.set(value); }
                           @Override public void onNext(Object value) { }
                           @Override public void onError(Throwable error) { downstreamFailure.set(error); }
                           @Override public void onComplete() { }
                       });

        Throwable observed = null;
        try {
            downstream.get().request(1);
        } catch (Throwable error) {
            observed = error;
        }

        assertSame(fatal, observed);
        assertTrue(cancelled.get());
        assertNull(downstreamFailure.get());
    }

    /** cancel 用普通异常包装 VME 时必须提升原致命错误，并以无环方式保留 mapper 主失败。 */
    @Test
    void promotesVirtualMachineErrorNestedInCancellationFailure() {
        IllegalStateException primary = new IllegalStateException("mapper failed");
        OutOfMemoryError fatal = new OutOfMemoryError("cancel nested fatal");
        IllegalStateException cleanup = new IllegalStateException("cancel wrapper", fatal);
        AtomicReference<Subscription> downstream = new AtomicReference<>();
        AtomicReference<Throwable> downstreamFailure = new AtomicReference<>();

        BatchPublishers.mapIndexed(subscriber -> subscriber.onSubscribe(new Subscription() {
            @Override public void request(long count) { if (count > 0L) subscriber.onNext("row"); }
            @Override public void cancel() { throw cleanup; }
        }), (String value, long index) -> { throw primary; })
                       .subscribe(new Subscriber<>() {
                           @Override public void onSubscribe(Subscription value) { downstream.set(value); }
                           @Override public void onNext(Object value) { }
                           @Override public void onError(Throwable error) { downstreamFailure.set(error); }
                           @Override public void onComplete() { }
                       });

        Throwable observed = null;
        try {
            downstream.get().request(1);
        } catch (Throwable error) {
            observed = error;
        }

        assertSame(fatal, observed);
        assertTrue(reaches(fatal, primary));
        assertFalse(reaches(primary, fatal));
        assertNull(downstreamFailure.get());
    }

    @Test
    void retainsMapperRuntimeWhenCancellationAlsoFailsNormally() {
        IllegalStateException primary = new IllegalStateException("mapper failed");
        IllegalStateException cleanup = new IllegalStateException("cancel failed");
        AtomicReference<Subscription> downstream = new AtomicReference<>();
        AtomicReference<Throwable> downstreamFailure = new AtomicReference<>();

        BatchPublishers.mapIndexed(subscriber -> subscriber.onSubscribe(new Subscription() {
            @Override public void request(long count) { if (count > 0L) subscriber.onNext("row"); }
            @Override public void cancel() { throw cleanup; }
        }), (String value, long index) -> { throw primary; })
                       .subscribe(new Subscriber<>() {
                           @Override public void onSubscribe(Subscription value) { downstream.set(value); }
                           @Override public void onNext(Object value) { }
                           @Override public void onError(Throwable error) { downstreamFailure.set(error); }
                           @Override public void onComplete() { }
                       });

        assertDoesNotThrow(() -> downstream.get().request(1));

        assertSame(primary, downstreamFailure.get());
        assertTrue(reaches(primary, cleanup));
        assertFalse(reaches(cleanup, primary));
    }

    private static Subscriber<Object> recordingSubscriber(AtomicReference<Subscription> subscription) {
        return new Subscriber<>() {
            @Override public void onSubscribe(Subscription value) { subscription.set(value); }
            @Override public void onNext(Object value) { }
            @Override public void onError(Throwable error) { }
            @Override public void onComplete() { }
        };
    }

    private static boolean reaches(Throwable root, Throwable expected) {
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.add(root);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (current == expected) {
                return true;
            }
            if (current.getCause() != null) {
                pending.addLast(current.getCause());
            }
            Collections.addAll(pending, current.getSuppressed());
        }
        return false;
    }
}
