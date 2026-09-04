package com.flying.orm.rdb.schema;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SchemaMigrationObserversTest {

    @Test
    void safeIsIdempotent() {
        SchemaMigrationObserver safe = SchemaMigrationObservers.safe(ignored -> {
        });

        assertSame(safe, SchemaMigrationObservers.safe(safe));
    }

    @Test
    void isolatesOrdinaryFailureWithoutMiningItsCauseGraph() {
        SyntheticVirtualMachineError nestedFatal = new SyntheticVirtualMachineError();
        SchemaMigrationObserver runtimeFailure = ignored -> {
            throw new CompletionException(nestedFatal);
        };
        AssertionError directError = new AssertionError("direct observer failure");
        SchemaMigrationObserver errorFailure = ignored -> {
            throw directError;
        };

        assertDoesNotThrow(() -> SchemaMigrationObservers.safe(runtimeFailure).onMigration(null));
        assertSame(directError, assertThrows(
                AssertionError.class,
                () -> SchemaMigrationObservers.safe(errorFailure).onMigration(null)));
    }

    private static final class SyntheticVirtualMachineError extends VirtualMachineError {
    }
}
