package com.flying.orm.rdb.repository;

import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.protection.FieldProtectionRegistry;
import com.flying.orm.rdb.id.IdGenerator;
import com.flying.orm.rdb.internal.mapping.EntityValues;
import com.flying.orm.rdb.lifecycle.EntityLifecyclePhase;
import com.flying.orm.rdb.mapping.EntityMetadata;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.time.Duration;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertThrows;

class SyncRepositoryBatchRowsNullContractTest {

    @Test
    void rejectsNullEntityAtSubscriberBoundary() {
        CapturingPublisher<String> source = new CapturingPublisher<>();
        rows(source).subscribe(new NoopSubscriber<>());

        assertThrows(NullPointerException.class, () -> source.subscriber.onNext(null));
    }

    @Test
    void rejectsNullFailureEvenAfterTermination() {
        CapturingPublisher<String> source = new CapturingPublisher<>();
        rows(source).subscribe(new NoopSubscriber<>());
        source.subscriber.onComplete();

        assertThrows(NullPointerException.class, () -> source.subscriber.onError(null));
    }

    private static SyncRepositoryBatchRows<String, String> rows(Publisher<String> source) {
        EntityMetadata<String> metadata = EntityMetadata.create(
                String.class, "contract", "contract", List.of(), null, TenantStrategy.NONE,
                FieldProtectionRegistry.builder().build());
        SyncRepositoryLifecycleSupport<String> lifecycle = new SyncRepositoryLifecycleSupport<>(
                metadata, null, new SyncRepositoryAwaiter(Duration.ofSeconds(1)));
        SyncRepositoryBatchLifecycle<String> retention = new SyncRepositoryBatchLifecycle<>(
                lifecycle, EntityLifecyclePhase.POST_PERSIST,
                EntityValues.createUncached(String.class, metadata),
                RepositoryEntityIdSupport.create(metadata, IdGenerator.none()), false, 1024L);
        return new SyncRepositoryBatchRows<>(source, value -> value, EntityLifecyclePhase.PRE_PERSIST,
                                             lifecycle, retention);
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
