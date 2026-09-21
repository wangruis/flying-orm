package com.flying.orm.rdb.form;

import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SyncBatchHeadTerminalSerializationTest {

    @Test
    void asynchronousCompletionWaitsForCachedFirstCallbackToReturn() throws Exception {
        assertSerializedFirstReplay(false, null);
    }

    @Test
    void asynchronousFailureWaitsForCachedFirstCallbackToReturn() throws Exception {
        assertSerializedFirstReplay(false, new IllegalStateException("source failed after first row"));
    }

    @Test
    void mappedFirstRowIsPresentWhenAsynchronousCompletionArrives() throws Exception {
        assertSerializedFirstReplay(true, null);
    }

    @Test
    void mappedFirstRowPrecedesTheOriginalAsynchronousFailure() throws Exception {
        assertSerializedFirstReplay(true, new IllegalStateException("source failed after first row"));
    }

    private static void assertSerializedFirstReplay(boolean mapped, Throwable expectedFailure)
            throws Exception {
        SyncBatchHeadState<String> state = new SyncBatchHeadState<>();
        // The original upstream onNext has returned before its asynchronous terminal signal.
        state.onNext("first");
        CountDownLatch firstEntered = new CountDownLatch(1);
        CountDownLatch releaseFirst = new CountDownLatch(1);
        ConcurrentLinkedQueue<String> events = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<String> rows = new ConcurrentLinkedQueue<>();
        AtomicReference<List<String>> rowsAtTerminal = new AtomicReference<>();
        AtomicReference<Subscription> subscription = new AtomicReference<>();
        AtomicReference<Throwable> observedFailure = new AtomicReference<>();
        AtomicInteger completions = new AtomicInteger();
        AtomicInteger failures = new AtomicInteger();

        Publisher<String> source = state::subscribe;
        Publisher<String> result = mapped
                ? BatchPublishers.mapIndexed(source, (row, index) -> {
                    firstEntered.countDown();
                    awaitRelease(releaseFirst);
                    return row + ":" + index;
                })
                : source;
        result.subscribe(new Subscriber<>() {
            @Override
            public void onSubscribe(Subscription candidate) {
                // JDBC also subscribes first and requests its first row after subscribe returns.
                subscription.set(candidate);
            }

            @Override
            public void onNext(String row) {
                if (!mapped) {
                    events.add("first-enter");
                    firstEntered.countDown();
                    awaitRelease(releaseFirst);
                }
                rows.add(row);
                events.add("next:" + row);
            }

            @Override
            public void onError(Throwable failure) {
                observedFailure.set(failure);
                failures.incrementAndGet();
                rowsAtTerminal.set(List.copyOf(rows));
                events.add("error");
            }

            @Override
            public void onComplete() {
                completions.incrementAndGet();
                rowsAtTerminal.set(List.copyOf(rows));
                events.add("complete");
            }
        });

        ExecutorService executor = Executors.newFixedThreadPool(2);
        try {
            Future<?> replay = executor.submit(() -> subscription.get().request(1));
            assertTrue(firstEntered.await(10, TimeUnit.SECONDS), "cached first row did not enter its callback");
            Future<?> terminal = executor.submit(() -> {
                if (expectedFailure == null) {
                    state.onComplete();
                } else {
                    state.onError(expectedFailure);
                }
            });
            try {
                // Registration must not block on user mapping/callback code. Delivery waits for it.
                terminal.get(10, TimeUnit.SECONDS);
            } finally {
                releaseFirst.countDown();
            }
            replay.get(10, TimeUnit.SECONDS);
            subscription.get().request(1);

            String expectedRow = mapped ? "first:0" : "first";
            String expectedTerminal = expectedFailure == null ? "complete" : "error";
            List<String> expectedEvents = mapped
                    ? List.of("next:" + expectedRow, expectedTerminal)
                    : List.of("first-enter", "next:" + expectedRow, expectedTerminal);
            assertAll(
                    () -> assertEquals(expectedEvents, List.copyOf(events),
                            "terminal must not overlap or overtake the cached first callback"),
                    () -> assertEquals(List.of(expectedRow), List.copyOf(rows), "first row must be delivered once"),
                    () -> assertEquals(List.of(expectedRow), rowsAtTerminal.get(),
                            "terminal must observe the first row, never an empty successful result"),
                    () -> assertEquals(expectedFailure == null ? 1 : 0, completions.get()),
                    () -> assertEquals(expectedFailure == null ? 0 : 1, failures.get()),
                    () -> assertSame(expectedFailure, observedFailure.get()));
        } finally {
            releaseFirst.countDown();
            state.close();
            executor.shutdownNow();
            assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS), "signal workers did not terminate");
        }
    }

    private static void awaitRelease(CountDownLatch release) {
        try {
            assertTrue(release.await(10, TimeUnit.SECONDS), "first callback was not released");
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new AssertionError("first callback was interrupted", exception);
        }
    }
}
