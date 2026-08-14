package com.flying.orm.rdb.form;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 验证同步批量首行桥接只向下游发送一次终止信号。 */
class SyncBatchHeadTest {

    @Test
    void doesNotRepeatCompletionAfterAdditionalDemand() throws Exception {
        RecordingSubscriber<String> subscriber = new RecordingSubscriber<>();

        try (SyncBatchHead<String> head = SyncBatchHead.open(Flux.just("one"), Duration.ofSeconds(1))) {
            head.subscribe(subscriber);
            subscriber.request(1);
            subscriber.request(1);
        }

        assertEquals(List.of("one"), subscriber.values);
        assertEquals(1, subscriber.completions.get());
        assertEquals(0, subscriber.errors.get());
    }

    @Test
    void completesEmptySourceWithoutWaitingForDemand() throws Exception {
        RecordingSubscriber<String> subscriber = new RecordingSubscriber<>();

        try (SyncBatchHead<String> head = SyncBatchHead.open(Flux.empty(), Duration.ofSeconds(1))) {
            head.subscribe(subscriber);
        }

        assertEquals(List.of(), subscriber.values);
        assertEquals(1, subscriber.completions.get());
        assertEquals(0, subscriber.errors.get());
    }

    @Test
    void reportsInvalidDemandOnlyOnce() throws Exception {
        RecordingSubscriber<String> subscriber = new RecordingSubscriber<>();

        try (SyncBatchHead<String> head = SyncBatchHead.open(Flux.just("one"), Duration.ofSeconds(1))) {
            head.subscribe(subscriber);
            subscriber.request(0);
            subscriber.request(-1);
        }

        assertEquals(List.of(), subscriber.values);
        assertEquals(0, subscriber.completions.get());
        assertEquals(1, subscriber.errors.get());
    }

    @Test
    void cancellationReleasesTheSingleUpstreamSubscription() throws Exception {
        AtomicInteger cancellations = new AtomicInteger();
        Publisher<String> source = subscriber -> subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long count) {
                if (count > 0L) {
                    subscriber.onNext("one");
                }
            }

            @Override
            public void cancel() {
                cancellations.incrementAndGet();
            }
        });
        RecordingSubscriber<String> subscriber = new RecordingSubscriber<>();

        try (SyncBatchHead<String> head = SyncBatchHead.open(source, Duration.ofSeconds(1))) {
            head.subscribe(subscriber);
            subscriber.cancel();
        }

        assertEquals(1, cancellations.get());
    }

    @Test
    void acceptsDurationBeyondNanosecondRange() throws Exception {
        try (SyncBatchHead<String> head = SyncBatchHead.open(
                Flux.just("one"), Duration.ofSeconds(Long.MAX_VALUE))) {
            assertEquals("one", head.first());
        }
    }

    /** VM 级上游失败不能被同步首行桥接降级为普通规划错误，且仍要取消已建立的订阅。 */
    @Test
    void propagatesVirtualMachineErrorFromSourceAndCancelsUpstream() {
        AtomicInteger cancellations = new AtomicInteger();
        OutOfMemoryError fatal = new OutOfMemoryError("source fatal");
        Publisher<String> source = subscriber -> subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long count) {
                if (count > 0L) {
                    subscriber.onError(fatal);
                }
            }

            @Override
            public void cancel() {
                cancellations.incrementAndGet();
            }
        });

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                                                  () -> SyncBatchHead.open(source, Duration.ofSeconds(1)));

        assertSame(fatal, observed);
        assertEquals(1, cancellations.get());
    }

    /** 普通 Error 也属于上游失败本身，首行桥接不得改写其身份，并仍要释放唯一上游订阅。 */
    @Test
    void propagatesErrorFromSourceAndCancelsUpstream() {
        AtomicInteger cancellations = new AtomicInteger();
        AssertionError failure = new AssertionError("source error");
        Publisher<String> source = subscriber -> subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long count) {
                if (count > 0L) {
                    subscriber.onError(failure);
                }
            }

            @Override
            public void cancel() {
                cancellations.incrementAndGet();
            }
        });

        AssertionError observed = assertThrows(AssertionError.class,
                                                () -> SyncBatchHead.open(source, Duration.ofSeconds(1)));

        assertSame(failure, observed);
        assertEquals(1, cancellations.get());
    }

    /** 上游用普通异常包装 VME 时，首行桥接仍须恢复原致命错误并取消唯一订阅。 */
    @Test
    void propagatesVirtualMachineErrorNestedInSourceFailure() {
        AtomicInteger cancellations = new AtomicInteger();
        OutOfMemoryError fatal = new OutOfMemoryError("source nested fatal");
        IllegalStateException wrapper = new IllegalStateException("source wrapper", fatal);
        Publisher<String> source = subscriber -> subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long count) {
                if (count > 0L) {
                    subscriber.onError(wrapper);
                }
            }

            @Override
            public void cancel() {
                cancellations.incrementAndGet();
            }
        });

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                                                  () -> SyncBatchHead.open(source, Duration.ofSeconds(1)));

        assertSame(fatal, observed);
        assertEquals(1, cancellations.get());
    }

    /** 取消清理用普通异常包装 VME 时，清理致命错误须优先于普通源失败。 */
    @Test
    void promotesVirtualMachineErrorNestedInSourceCancellationFailure() {
        IllegalStateException primary = new IllegalStateException("source failed");
        OutOfMemoryError fatal = new OutOfMemoryError("cancel nested fatal");
        IllegalStateException cleanup = new IllegalStateException("cancel wrapper", fatal);
        Publisher<String> source = subscriber -> subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long count) {
                if (count > 0L) {
                    subscriber.onError(primary);
                }
            }

            @Override
            public void cancel() {
                throw cleanup;
            }
        });

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                                                  () -> SyncBatchHead.open(source, Duration.ofSeconds(1)));

        assertSame(fatal, observed);
    }

    private static final class RecordingSubscriber<T> implements Subscriber<T> {
        private final AtomicReference<Subscription> subscription = new AtomicReference<>();
        private final List<T> values = new ArrayList<>();
        private final AtomicInteger completions = new AtomicInteger();
        private final AtomicInteger errors = new AtomicInteger();

        @Override
        public void onSubscribe(Subscription candidate) {
            subscription.set(candidate);
        }

        @Override
        public void onNext(T value) {
            values.add(value);
        }

        @Override
        public void onError(Throwable error) {
            errors.incrementAndGet();
        }

        @Override
        public void onComplete() {
            completions.incrementAndGet();
        }

        private void request(long count) {
            subscription.get().request(count);
        }

        private void cancel() {
            subscription.get().cancel();
        }
    }
}
