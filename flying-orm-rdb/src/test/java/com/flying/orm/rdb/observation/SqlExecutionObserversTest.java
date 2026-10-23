package com.flying.orm.rdb.observation;

import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlExecutionObserversTest {
    @Test
    void logSinkOnlyExposesTheLevelAwareWriteContract() throws Exception {
        assertThrows(NoSuchMethodException.class, () -> SqlExecutionLogSink.class.getMethod("write", String.class));
        assertTrue(java.lang.reflect.Modifier.isAbstract(SqlExecutionLogSink.class
                .getMethod("write", SqlExecutionLogLevel.class, String.class).getModifiers()));
        org.junit.jupiter.api.Assertions.assertEquals(1, SqlExecutionLogOptions.class.getDeclaredConstructors().length);
        org.junit.jupiter.api.Assertions.assertEquals(1,
                com.flying.orm.rdb.bootstrap.FlyingOrmConfiguration.SqlLog.class.getDeclaredConstructors().length);
    }


    @Test
    void logSinkReceivesTheExecutionLevelWithoutAnAdapter() {
        java.util.List<SqlExecutionLogLevel> levels = new java.util.ArrayList<>();
        SqlExecutionLogObserver.create(SqlExecutionLogOptions.defaults(), (level, message) -> levels.add(level))
                .onExecution(successObservation());
        org.junit.jupiter.api.Assertions.assertEquals(java.util.List.of(SqlExecutionLogLevel.DEBUG), levels);
    }

    @Test
    void disabledLevelDoesNotPublishALog() {
        SqlExecutionLogSink sink = new SqlExecutionLogSink() {
            public boolean isEnabled(SqlExecutionLogLevel level) { return false; }
            public void write(SqlExecutionLogLevel level, String message) {
                throw new AssertionError("disabled level must not be written");
            }
        };
        SqlExecutionLogObserver.create(SqlExecutionLogOptions.defaults(), sink).onExecution(successObservation());
    }

    @Test
    void safeIsIdempotentAndPreservesObserverCapabilities() {
        SqlExecutionObserver observer = new SqlExecutionObserver() {
            @Override
            public boolean requiresParameterValues() {
                return true;
            }

            @Override
            public void onExecution(SqlExecutionObservation observation) {
                // Capability behavior is the contract under test.
            }
        };

        SqlExecutionObserver safe = SqlExecutionObservers.safe(observer);

        assertSame(safe, SqlExecutionObservers.safe(safe));
        assertTrue(safe.requiresParameterValues());
    }

    @Test
    void doesNotWrapTheAlreadyIsolatedLogObserverAgain() {
        SqlExecutionLogObserver observer = SqlExecutionLogObserver.create(
                SqlExecutionLogOptions.defaults(), (level, message) -> {
                    // The real log observer owns its sink/formatter isolation.
                });

        assertSame(observer, SqlExecutionObservers.safe(observer));
    }

    @Test
    void isolatesRuntimeObserverFailuresWithoutInspectingTheirCausesAndPropagatesErrors() {
        OutOfMemoryError nestedFatal = new OutOfMemoryError("nested observer failure");
        SqlExecutionObserver runtimeFailure = ignored -> {
            throw new IllegalStateException("observer failure", nestedFatal);
        };
        AssertionError directFatal = new AssertionError("direct observer failure");
        SqlExecutionObserver errorFailure = ignored -> {
            throw directFatal;
        };

        VirtualMachineError escaped = null;
        try {
            SqlExecutionObservers.safe(runtimeFailure).onExecution(null);
        } catch (VirtualMachineError error) {
            escaped = error;
        }

        assertNull(escaped);
        assertSame(directFatal, assertThrows(
                AssertionError.class, () -> SqlExecutionObservers.safe(errorFailure).onExecution(null)));
    }

    @Test
    void logSinkDoesNotMineRuntimeWrappersButStillPropagatesDirectErrors() {
        CompletionException wrapped = new CompletionException(
                new SyntheticVirtualMachineError());
        SqlExecutionLogObserver wrappedSink = SqlExecutionLogObserver.create(
                SqlExecutionLogOptions.defaults(), (level, message) -> {
                    throw wrapped;
                });

        assertDoesNotThrow(() -> wrappedSink.onExecution(successObservation()));

        AssertionError direct = new AssertionError("direct sink failure");
        SqlExecutionLogObserver directSink = SqlExecutionLogObserver.create(
                SqlExecutionLogOptions.defaults(), (level, message) -> {
                    throw direct;
                });

        assertSame(direct, assertThrows(
                AssertionError.class, () -> directSink.onExecution(successObservation())));
    }

    private static SqlExecutionObservation successObservation() {
        return new SqlExecutionObservation(
                SqlExecutionOperation.QUERY,
                SqlExecutionBackend.JDBC,
                SqlStatementType.SELECT,
                SqlExecutionStatus.SUCCESS,
                SqlFailureCategory.NONE,
                "select 1",
                0,
                0,
                1,
                1,
                null);
    }

    private static final class SyntheticVirtualMachineError extends VirtualMachineError {
        private static final long serialVersionUID = 1L;
    }
}
