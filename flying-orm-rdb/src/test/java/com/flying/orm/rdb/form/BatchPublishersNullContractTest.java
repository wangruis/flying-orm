package com.flying.orm.rdb.form;

import com.flying.orm.rdb.internal.batch.BatchPublishers;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;

import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class BatchPublishersNullContractTest {
    @Test
    void mappedIterableCompletesWhenDemandMatchesItsSize() {
        RecordingSubscriber<Integer> subscriber = new RecordingSubscriber<>();
        BatchPublishers.mapIndexed(Flux.fromIterable(List.of(1, 2)),
                (value, index) -> value + (int) index).subscribe(subscriber);
        subscriber.subscription.request(2);
        assertEquals(List.of(1, 3), subscriber.values);
        assertTrue(subscriber.completed);
    }

    @Test
    void mappedIterableHonorsDemandCancellationAndReplay() {
        AtomicInteger iterators = new AtomicInteger();
        Publisher<Integer> source = mappedIterable(() -> {
            iterators.incrementAndGet();
            return List.of(1, 2, 3).iterator();
        });
        RecordingSubscriber<Integer> first = new RecordingSubscriber<>();
        source.subscribe(first);
        assertEquals(1, iterators.get());
        assertTrue(first.values.isEmpty());
        first.subscription.request(1);
        assertEquals(List.of(1), first.values);
        first.subscription.cancel();
        first.subscription.request(2);
        assertEquals(List.of(1), first.values);
        RecordingSubscriber<Integer> replay = new RecordingSubscriber<>();
        source.subscribe(replay);
        replay.subscription.request(Long.MAX_VALUE);
        assertEquals(List.of(1, 2, 3), replay.values);
        assertTrue(replay.completed);
        assertEquals(2, iterators.get());
    }

    @Test
    void iterablePreservesEmptyAndFailedInputSignals() {
        RecordingSubscriber<Object> empty = new RecordingSubscriber<>();
        mappedIterable(List.of()).subscribe(empty);
        empty.subscription.request(1);
        assertTrue(empty.completed);
        RecordingSubscriber<Integer> invalid = new RecordingSubscriber<>();
        mappedIterable(Arrays.asList(1, null)).subscribe(invalid);
        invalid.subscription.request(Long.MAX_VALUE);
        assertEquals(List.of(1), invalid.values);
        assertInstanceOf(NullPointerException.class, invalid.error);
        RecordingSubscriber<Object> failed = new RecordingSubscriber<>();
        mappedIterable(() -> {
            throw new IllegalStateException("iterator failed");
        }).subscribe(failed);
        failed.subscription.request(1);
        assertEquals("iterator failed", failed.error.getMessage());
    }

    private static <T> Publisher<T> mappedIterable(Iterable<T> rows) {
        return BatchPublishers.mapIndexed(Flux.fromIterable(rows), (row, index) -> row);
    }

    @Test
    void rejectsNullSourceValueAtSubscriberBoundary() {
        CapturingPublisher<String> source = new CapturingPublisher<>();
        BatchPublishers.mapIndexed(source, (value, index) -> value).subscribe(new NoopSubscriber<>());

        assertThrows(NullPointerException.class, () -> source.subscriber.onNext(null));
    }

    @Test
    void rejectsNullSourceFailureEvenAfterTermination() {
        CapturingPublisher<String> source = new CapturingPublisher<>();
        BatchPublishers.mapIndexed(source, (value, index) -> value).subscribe(new NoopSubscriber<>());
        source.subscriber.onComplete();

        assertThrows(NullPointerException.class, () -> source.subscriber.onError(null));
    }

    private static final class CapturingPublisher<T> implements Publisher<T> {
        private Subscriber<? super T> subscriber;

        @Override
        public void subscribe(Subscriber<? super T> candidate) {
            subscriber = candidate;
            candidate.onSubscribe(new NoopSubscription());
        }
    }

    private static final class RecordingSubscriber<T> implements Subscriber<T> {
        private final List<T> values = new ArrayList<>();
        private Subscription subscription;
        private Throwable error;
        private boolean completed;

        public void onSubscribe(Subscription value) { subscription = value; }
        public void onNext(T value) { values.add(value); }
        public void onError(Throwable value) { error = value; }
        public void onComplete() { completed = true; }
    }

    private static final class NoopSubscriber<T> implements Subscriber<T> {
        @Override
        public void onSubscribe(Subscription subscription) {
        }

        @Override
        public void onNext(T value) {
        }

        @Override
        public void onError(Throwable error) {
        }

        @Override
        public void onComplete() {
        }
    }

    private static final class NoopSubscription implements Subscription {
        @Override
        public void request(long count) {
        }

        @Override
        public void cancel() {
        }
    }
}
