package com.flying.orm.rdb.repository;

import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.Test;

class RepositoryFailureSupportTest {

    @Test
    void preservesPrimaryFailureAndUsesStandardSuppressionForCleanupFailure() {
        RuntimeException primary = new RuntimeException(
                "primary", new CompletionException(new OutOfMemoryError("nested fatal")));
        CompletionException cleanup = new CompletionException(new InternalError("nested cleanup fatal"));

        Throwable merged;
        try {
            merged = RepositoryFailureSupport.afterCleanup(primary, () -> { throw cleanup; });
        } catch (VirtualMachineError promoted) {
            fail("ordinary repository lifecycle must not promote nested errors", promoted);
            return;
        }

        assertSame(primary, merged);
        assertSame(cleanup, primary.getSuppressed()[0]);
        assertSame(primary, RepositoryFailureSupport.propagate(merged));
    }

    @Test
    void cancellationCleanupOnlyPropagatesDirectErrors() {
        try {
            RepositoryFailureSupport.cleanupAfterCancellation(
                    () -> { throw new CompletionException(new OutOfMemoryError("nested fatal")); });
        } catch (VirtualMachineError promoted) {
            fail("cancellation cleanup must not promote nested errors", promoted);
        }

        AssertionError direct = new AssertionError("direct fatal");
        assertSame(direct, assertThrows(AssertionError.class,
                () -> RepositoryFailureSupport.cleanupAfterCancellation(() -> { throw direct; })));
    }
}
