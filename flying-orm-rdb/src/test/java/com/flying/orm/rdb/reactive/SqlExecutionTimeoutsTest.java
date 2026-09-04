package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.execution.SqlExecutionTimeoutException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqlExecutionTimeoutsTest {

    @Test
    void doesNotCreateTimeoutFailureWhenSourceCompletesBeforeDeadline() {
        AtomicInteger failuresCreated = new AtomicInteger();

        Integer value = SqlExecutionTimeouts.total(
                Flux.just(7),
                Duration.ofSeconds(1),
                Duration.ofSeconds(1),
                () -> {
                    failuresCreated.incrementAndGet();
                    return timeoutFailure();
                }).blockLast();

        assertEquals(7, value);
        assertEquals(0, failuresCreated.get());
    }

    @Test
    void createsOneTimeoutFailureOnlyWhenDeadlineWins() {
        AtomicInteger failuresCreated = new AtomicInteger();

        RuntimeException failure = assertThrows(RuntimeException.class, () ->
                SqlExecutionTimeouts.total(
                        Flux.never(),
                        Duration.ofMillis(10),
                        Duration.ofSeconds(2),
                        () -> {
                            failuresCreated.incrementAndGet();
                            return timeoutFailure();
                        }).blockLast());

        assertInstanceOf(SqlExecutionTimeoutException.class, failure);
        assertEquals(1, failuresCreated.get());
    }

    private static SqlExecutionTimeoutException timeoutFailure() {
        return new SqlExecutionTimeoutException(
                Duration.ofSeconds(2), new TimeoutException("deadline elapsed"));
    }
}
