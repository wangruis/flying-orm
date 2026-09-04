package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;

import javax.sql.DataSource;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JdbcFailureTraversalConvergenceTest {

    @Test
    void metadataCallbackIsTranslatedAfterOneWrapperRead() {
        TrackingCompletionException failure = new TrackingCompletionException(new IllegalStateException("failed"));
        JdbcAdvancedOperations advanced = JdbcSqlExecutor.create(dataSource()).advanced();

        RuntimeException thrown = assertThrows(RuntimeException.class,
                                               () -> advanced.metadata(ignored -> {
                                                   throw failure;
                                               }));

        assertSame(failure, thrown);
        assertEquals(1, failure.causeReads);
    }

    @Test
    void resourceCloseDoesNotInspectPrimaryWhenThereIsNoCleanupFailure() {
        TrackingCompletionException primary = new TrackingCompletionException(new IllegalStateException("failed"));

        JdbcResources.close(SqlExecutionOperation.QUERY, false, primary, observations());

        assertEquals(0, primary.causeReads);
        assertEquals(0, primary.getSuppressed().length);
    }

    @Test
    void directVirtualMachineErrorStillEscapesUnchanged() {
        SyntheticVirtualMachineError callbackFatal = new SyntheticVirtualMachineError();
        JdbcAdvancedOperations advanced = JdbcSqlExecutor.create(dataSource()).advanced();

        SyntheticVirtualMachineError callbackThrown = assertThrows(
                SyntheticVirtualMachineError.class,
                () -> advanced.metadata(ignored -> {
                    throw callbackFatal;
                }));

        SyntheticVirtualMachineError cleanupFatal = new SyntheticVirtualMachineError();
        SyntheticVirtualMachineError cleanupThrown = assertThrows(
                SyntheticVirtualMachineError.class,
                () -> JdbcResources.close(SqlExecutionOperation.QUERY, false, null, observations(),
                                          () -> {
                                              throw cleanupFatal;
                                          }));
        assertSame(callbackFatal, callbackThrown);
        assertSame(cleanupFatal, cleanupThrown);
    }

    @Test
    void statementCancellationDoesNotPromoteWrappedVirtualMachineError() {
        TrackingCompletionException wrapped = new TrackingCompletionException(
                new SyntheticVirtualMachineError());
        Statement statement = (Statement) Proxy.newProxyInstance(
                JdbcFailureTraversalConvergenceTest.class.getClassLoader(),
                new Class<?>[]{Statement.class},
                (proxy, method, arguments) -> {
                    if ("cancel".equals(method.getName())) {
                        throw wrapped;
                    }
                    return null;
                });

        Thread.currentThread().interrupt();
        try {
            SQLException thrown = assertThrows(
                    SQLException.class,
                    () -> JdbcStatementControl.requireNotInterrupted(statement));

            assertEquals("HY008", thrown.getSQLState());
            assertEquals(1, thrown.getSuppressed().length);
            assertSame(wrapped, thrown.getSuppressed()[0]);
            assertEquals(0, wrapped.causeReads);
        } finally {
            Thread.interrupted();
        }
    }

    @Test
    void rollbackFailureDoesNotPromoteWrappedVirtualMachineError() {
        IllegalStateException primary = new IllegalStateException("write failed");
        TrackingCompletionException rollback = new TrackingCompletionException(
                new SyntheticVirtualMachineError());

        JdbcBatchSupport.RollbackOutcome outcome = JdbcBatchSupport.RollbackOutcome.failed(primary, rollback);

        assertFalse(outcome.confirmed());
        assertNull(outcome.cleanupFatal());
        assertEquals(1, primary.getSuppressed().length);
        assertSame(rollback, primary.getSuppressed()[0]);
        assertEquals(0, rollback.causeReads);
    }

    @Test
    void batchTryWithResourcesPromotesOnlyADirectSuppressedVirtualMachineError() {
        SyntheticVirtualMachineError cleanupFatal = new SyntheticVirtualMachineError();
        IllegalStateException merged = assertThrows(IllegalStateException.class, () -> {
            try (AutoCloseable ignored = () -> {
                throw cleanupFatal;
            }) {
                throw new IllegalStateException("batch failed");
            }
        });

        SyntheticVirtualMachineError thrown = assertThrows(
                SyntheticVirtualMachineError.class,
                () -> JdbcBatchSupport.rethrowTryWithResourcesVirtualMachineError(merged));

        assertSame(cleanupFatal, thrown);

        IllegalStateException nested = new IllegalStateException("nested cleanup wrapper");
        nested.addSuppressed(new SyntheticVirtualMachineError());
        IllegalStateException primary = new IllegalStateException("batch failed");
        primary.addSuppressed(nested);
        JdbcBatchSupport.rethrowTryWithResourcesVirtualMachineError(primary);
    }

    @Test
    void independentBatchPromotesDirectCancelFatalMergedByRowsTryWithResources() {
        SyntheticVirtualMachineError cleanupFatal = new SyntheticVirtualMachineError();
        Publisher<Object[]> rows = subscriber -> subscriber.onSubscribe(new Subscription() {
            private boolean emitted;

            @Override
            public void request(long count) {
                if (!emitted) {
                    emitted = true;
                    subscriber.onNext(new Object[]{1});
                }
            }

            @Override
            public void cancel() {
                throw cleanupFatal;
            }
        });
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into sample(value_col) values (?)",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                rows,
                BatchWriteOptions.independent(1, 1));
        DataSource failingDataSource = (DataSource) Proxy.newProxyInstance(
                JdbcFailureTraversalConvergenceTest.class.getClassLoader(),
                new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> {
                    if ("getConnection".equals(method.getName())) {
                        throw new SQLException("borrow failed");
                    }
                    return null;
                });

        SyntheticVirtualMachineError thrown = assertThrows(
                SyntheticVirtualMachineError.class,
                () -> JdbcBatchWriter.create(failingDataSource).writeBatch(request));

        assertSame(cleanupFatal, thrown);
    }

    @Test
    void keepsPrimaryAndSuppressedCleanupTruth() {
        IllegalStateException primary = new IllegalStateException("primary");
        IllegalArgumentException cleanup = new IllegalArgumentException("cleanup");

        JdbcResources.close(SqlExecutionOperation.QUERY, false, primary, observations(),
                            () -> {
                                throw cleanup;
                            });

        assertEquals(1, primary.getSuppressed().length);
        assertSame(cleanup, primary.getSuppressed()[0]);
    }

    private static JdbcExecutionObservationSupport observations() {
        return JdbcExecutionObservationSupport.create(SqlExecutionObserver.noop());
    }

    private static DataSource dataSource() {
        Connection connection = (Connection) Proxy.newProxyInstance(
                JdbcFailureTraversalConvergenceTest.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> null);
        return (DataSource) Proxy.newProxyInstance(
                JdbcFailureTraversalConvergenceTest.class.getClassLoader(),
                new Class<?>[]{DataSource.class},
                (proxy, method, arguments) -> "getConnection".equals(method.getName()) ? connection : null);
    }

    private static final class TrackingCompletionException extends CompletionException {
        private int causeReads;

        private TrackingCompletionException(Throwable cause) {
            super(cause);
        }

        @Override
        public synchronized Throwable getCause() {
            causeReads++;
            return super.getCause();
        }
    }

    private static final class SyntheticVirtualMachineError extends VirtualMachineError {
        private static final long serialVersionUID = 1L;
    }
}
