package com.flying.orm.rdb.observation;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

class BatchExecutionObserverIsolationTest {

    @Test
    void safeDispatcherIsIdempotentAndRecognizesTheLogObserver() {
        BatchExecutionObserver throwing = ignored -> {
            throw new IllegalStateException("metrics unavailable");
        };
        BatchExecutionObserver safe = BatchExecutionObservers.safe(throwing);
        SqlExecutionLogObserver log = SqlExecutionLogObserver.create(
                SqlExecutionLogOptions.defaults(), ignored -> {
                    // Real log observer already owns sink isolation.
                });

        assertSame(safe, BatchExecutionObservers.safe(safe));
        assertSame(log, BatchExecutionObservers.safe(log));
        assertDoesNotThrow(() -> safe.onExecution(null));
    }

    @Test
    void publicObserverApiDoesNotExposeForgeableIsolationTypes() {
        Set<String> nestedTypes = Arrays.stream(BatchExecutionObserver.class.getDeclaredClasses())
                .map(Class::getSimpleName)
                .collect(Collectors.toSet());

        assertEquals(Set.of(), nestedTypes);
    }
}
