package com.flying.orm.rdb.observation;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.UnaryOperator;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SqlObserverCompositionContractRegressionTest {
    private static final SqlExecutionObservation EVENT = new SqlExecutionObservation(
            SqlExecutionOperation.UPDATE, SqlExecutionBackend.JDBC, SqlStatementType.UPDATE,
            SqlExecutionStatus.SUCCESS, SqlFailureCategory.NONE,
            "UPDATE account SET active = ?", 1, 0, 1, 1, null);
    private static final List<Object> PARAMETERS = List.of(7);

    @Test
    void conditionalAndSamplingFactoriesPreserveDisabledState() {
        List<UnaryOperator<SqlExecutionObserver>> factories = List.of(
                observer -> SqlExecutionObservers.when(event -> true, observer),
                SqlExecutionObservers::errors, SqlExecutionObservers::successes,
                observer -> SqlExecutionObservers.slow(Duration.ZERO, observer),
                observer -> SqlExecutionObservers.sample(0.5, observer),
                observer -> SqlExecutionObservers.sampleEvery(1, observer));
        AtomicInteger calls = new AtomicInteger();
        SqlExecutionObserver disabled = new SqlExecutionObserver() {
            @Override public boolean enabled() { return false; }
            @Override public void onExecution(SqlExecutionObservation event) { calls.incrementAndGet(); }
            @Override public void onResourceCleanup(ResourceCleanupObservation event) { calls.incrementAndGet(); }
        };
        for (UnaryOperator<SqlExecutionObserver> factory : factories) {
            SqlExecutionObserver observer = factory.apply(disabled);
            assertFalse(observer.enabled(), "a filter must not enable a disabled observer");
            if (observer.enabled()) {
                observer.onExecution(EVENT);
                observer.onExecution(EVENT, PARAMETERS);
                observer.onResourceCleanup(cleanup());
            }
        }
        assertEquals(0, calls.get());
    }

    @Test
    void mixedDetailsPreserveBaseAndParameterCallbacks() {
        List<String> calls = new ArrayList<>();
        SqlExecutionObserver baseOnly = event -> calls.add("base");
        SqlExecutionObserver parameterOnly = new SqlExecutionObserver() {
            @Override public boolean requiresParameterValues() { return true; }
            @Override public void onExecution(SqlExecutionObservation event) { calls.add("parameter-base"); }
            @Override public void onExecution(SqlExecutionObservation event, List<Object> parameters) {
                assertSame(PARAMETERS, parameters);
                calls.add("parameters");
            }
        };
        SqlExecutionObserver observer = SqlExecutionObservers.composite(baseOnly, parameterOnly);
        assertTrue(observer.requiresParameterValues());
        observer.onExecution(EVENT, PARAMETERS);
        assertEquals(List.of("base", "parameters"), calls);
    }

    @Test
    void eachDetailCombinationReceivesExactlyOneMatchingCallbackInOrder() {
        for (int available = 0; available < 2; available++) {
            List<String> calls = new ArrayList<>();
            SqlExecutionObserver[] delegates = new SqlExecutionObserver[2];
            for (int required = 0; required < 2; required++) {
                delegates[required] = recording(required, calls);
            }
            SqlExecutionObserver observer = SqlExecutionObservers.composite(delegates);
            emit(observer, available);
            List<String> expected = new ArrayList<>();
            for (int required = 0; required < 2; required++) {
                expected.add(required + ":" + (required & available));
            }
            assertEquals(expected, calls, "available details=" + available);
        }
    }

    @Test
    void nestedSafeAndConditionalCompositionPreservesDispatch() {
        List<String> calls = new ArrayList<>();
        SqlExecutionObserver observer = SqlExecutionObservers.safe(SqlExecutionObservers.when(
                event -> true, SqlExecutionObservers.composite(
                        SqlExecutionObservers.when(event -> true, recording(0, calls)), recording(1, calls))));
        emit(observer, 1);
        assertEquals(List.of("0:0", "1:1"), calls);
    }

    @Test
    void enabledFiltersKeepTheirEventAndCleanupSemantics() {
        AtomicInteger events = new AtomicInteger();
        AtomicInteger cleanups = new AtomicInteger();
        SqlExecutionObserver delegate = new SqlExecutionObserver() {
            @Override public void onExecution(SqlExecutionObservation event) { events.incrementAndGet(); }
            @Override public void onResourceCleanup(ResourceCleanupObservation event) { cleanups.incrementAndGet(); }
        };
        SqlExecutionObserver observer = SqlExecutionObservers.errors(delegate);
        observer.onExecution(EVENT);
        observer.onResourceCleanup(cleanup());
        assertEquals(0, events.get());
        assertEquals(1, cleanups.get());
        SqlExecutionObservers.sampleEvery(2, delegate).onExecution(EVENT);
        assertEquals(0, events.get());
        SqlExecutionObservers.successes(delegate).onExecution(EVENT);
        assertEquals(1, events.get());
    }

    @Test
    void ordinaryCallbackFailuresAreIsolatedButDirectErrorsPropagate() {
        AtomicInteger calls = new AtomicInteger();
        SqlExecutionObserver broken = event -> { throw new IllegalStateException("observer failure"); };
        SqlExecutionObservers.composite(broken, event -> calls.incrementAndGet()).onExecution(EVENT);
        assertEquals(1, calls.get());
        AssertionError fatal = new AssertionError("direct error");
        SqlExecutionObserver fatalObserver = SqlExecutionObservers.when(event -> true, event -> { throw fatal; });
        assertSame(fatal, assertThrows(AssertionError.class, () -> fatalObserver.onExecution(EVENT)));
    }

    @Test
    void enabledProbeFailureKeepsFilteringDisabled() {
        SqlExecutionObserver broken = new SqlExecutionObserver() {
            @Override public boolean enabled() { throw new IllegalStateException("probe failure"); }
            @Override public void onExecution(SqlExecutionObservation event) { fail("disabled callback"); }
        };
        assertFalse(SqlExecutionObservers.when(event -> true, broken).enabled());
    }

    private static SqlExecutionObserver recording(int required, List<String> calls) {
        return new SqlExecutionObserver() {
            @Override public boolean requiresParameterValues() { return (required & 1) != 0; }
            @Override public void onExecution(SqlExecutionObservation event) { calls.add(required + ":0"); }
            @Override public void onExecution(SqlExecutionObservation event, List<Object> parameters) {
                assertSame(PARAMETERS, parameters);
                calls.add(required + ":1");
            }
        };
    }

    private static void emit(SqlExecutionObserver observer, int details) {
        switch (details) {
            case 0 -> observer.onExecution(EVENT);
            case 1 -> observer.onExecution(EVENT, PARAMETERS);
            default -> throw new AssertionError(details);
        }
    }

    private static ResourceCleanupObservation cleanup() {
        return new ResourceCleanupObservation(SqlExecutionOperation.UPDATE,
                ResourceCleanupObservation.Phase.CONNECTION_RELEASE, true, new IllegalStateException("close failed"));
    }
}
