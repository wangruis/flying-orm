package com.flying.orm.rdb.observation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.TimeoutException;

import org.junit.jupiter.api.Test;

class ResourceCleanupObservationTest {

    @Test
    void publishesOnlyPrimaryAndDirectSecondarySanitizedFacts() {
        IllegalStateException primary = new IllegalStateException("driver password=secret");
        TimeoutException secondary = new TimeoutException("endpoint with credentials");
        secondary.addSuppressed(new IllegalArgumentException("nested driver detail"));
        primary.addSuppressed(secondary);

        ResourceCleanupObservation observation = new ResourceCleanupObservation(
                SqlExecutionOperation.UPDATE,
                ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                true,
                primary);

        assertEquals(ResourceCleanupObservation.FailureKind.FAILURE, observation.failureKind());
        assertEquals("resource cleanup failed", observation.error().getMessage());
        assertEquals(1, observation.error().getSuppressed().length);
        assertEquals("resource cleanup timed out", observation.error().getSuppressed()[0].getMessage());
        assertEquals(0, observation.error().getSuppressed()[0].getSuppressed().length);
    }
}
