package com.flying.orm.rdb.form;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import static org.junit.jupiter.api.Assertions.assertThrows;

class BatchPublishersNullContractTest {

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
