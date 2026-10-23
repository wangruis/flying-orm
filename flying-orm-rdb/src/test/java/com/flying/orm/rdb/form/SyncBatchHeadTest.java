package com.flying.orm.rdb.form;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscription;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SyncBatchHeadTest {
    @Test
    void previewsOneRowThenUsesOnlyTheConsumersDemand() throws Exception {
        AtomicInteger subscriptions = new AtomicInteger();
        AtomicInteger emitted = new AtomicInteger();
        AtomicInteger cancelled = new AtomicInteger();
        Flux<Integer> source = Flux.range(1, 10)
                .doOnSubscribe(ignored -> subscriptions.incrementAndGet())
                .doOnNext(ignored -> emitted.incrementAndGet())
                .doOnCancel(cancelled::incrementAndGet);
        try (SyncBatchHead<Integer> head = SyncBatchHead.open(source)) {
            assertFalse(head.isEmpty());
            assertEquals(1, head.first());
            assertEquals(1, emitted.get());
            List<Integer> rows = new ArrayList<>();
            BaseSubscriber<Integer> consumer = new BaseSubscriber<>() {
                protected void hookOnSubscribe(Subscription subscription) { }
                protected void hookOnNext(Integer value) { rows.add(value); }
            };
            Flux.from(head.rows()).subscribe(consumer);
            consumer.request(1);
            assertEquals(List.of(1), rows);
            assertEquals(1, emitted.get());
            consumer.request(1);
            assertEquals(List.of(1, 2), rows);
            assertEquals(2, emitted.get());
            consumer.cancel();
        }
        assertEquals(1, subscriptions.get());
        assertEquals(1, cancelled.get());
    }

    @Test
    void closingBeforeExecutionCancelsTheSingleSource() throws Exception {
        AtomicInteger cancelled = new AtomicInteger();
        SyncBatchHead<Integer> head = SyncBatchHead.open(
                Flux.just(1).concatWith(Flux.never()).doOnCancel(cancelled::incrementAndGet));
        assertEquals(1, head.first());
        head.close();
        head.close();
        assertEquals(1, cancelled.get());
    }

    @Test
    void emptyAndFailedSourcesAreResolvedBeforePlanning() throws Exception {
        try (SyncBatchHead<Integer> head = SyncBatchHead.open(Flux.empty())) {
            assertTrue(head.isEmpty());
            assertThrows(IllegalStateException.class, head::first);
        }
        IllegalStateException failure = new IllegalStateException("upstream failed");
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> SyncBatchHead.open(Flux.error(failure))));
    }

    @Test
    void interruptedPreviewCancelsTheSource() throws Exception {
        CountDownLatch subscribed = new CountDownLatch(1);
        AtomicInteger cancelled = new AtomicInteger();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread worker = Thread.ofPlatform().start(() -> {
            try (SyncBatchHead<Integer> ignored = SyncBatchHead.open(Flux.<Integer>never()
                    .doOnSubscribe(subscription -> subscribed.countDown())
                    .doOnCancel(cancelled::incrementAndGet))) {
                failure.set(new AssertionError("preview should wait for a signal"));
            } catch (Throwable error) {
                failure.set(error);
            }
        });
        try {
            assertTrue(subscribed.await(5, TimeUnit.SECONDS));
            worker.interrupt();
            worker.join(5000);
            assertFalse(worker.isAlive());
            assertInstanceOf(InterruptedException.class, failure.get());
            assertEquals(1, cancelled.get());
        } finally {
            worker.interrupt();
            worker.join(5000);
        }
    }
}
