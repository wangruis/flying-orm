package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
import com.flying.orm.rdb.observation.SqlTransactionSource;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import io.r2dbc.spi.Connection;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReactiveSqlObservationTest {

    @Test
    void ownsOneSubscriptionAndPublishesOnlyItsFirstTerminalSignal() {
        List<SqlExecutionObservation> events = new ArrayList<>();
        List<SqlTransactionSource> transactionSources = new ArrayList<>();
        ReactiveSqlObservation observation = ReactiveSqlObservation.start(
                new com.flying.orm.rdb.observation.SqlExecutionObserver() {
                    @Override
                    public void onExecution(SqlExecutionObservation event) {
                        events.add(event);
                    }

                    @Override
                    public void onExecution(SqlExecutionObservation event,
                                            SqlTransactionSource transactionSource) {
                        events.add(event);
                        transactionSources.add(transactionSource);
                    }
                },
                false,
                true,
                new ReactiveSqlObservation.Request(
                        SqlExecutionOperation.QUERY,
                        "select id from device",
                        0,
                        0,
                        List.of()));

        observation.incrementRows();
        observation.incrementRows();
        observation.success(SqlTransactionSource.EXTERNAL);
        observation.cancelled(SqlTransactionSource.AUTO_COMMIT);

        assertEquals(1, events.size());
        assertEquals(SqlExecutionStatus.SUCCESS, events.getFirst().status());
        assertEquals(2L, events.getFirst().rows());
        assertEquals(List.of(SqlTransactionSource.EXTERNAL), transactionSources);
    }

    @Test
    void preservesAffectedRowsWhenGeneratedKeyReadingFails() {
        List<SqlExecutionObservation> events = new ArrayList<>();
        ReactiveSqlObservation observation = ReactiveSqlObservation.start(
                events::add,
                false,
                false,
                new ReactiveSqlObservation.Request(
                        SqlExecutionOperation.UPDATE,
                        "insert into device(name) values (?)",
                        1,
                        0,
                        List.of("sensor")));

        observation.error(
                new GeneratedKeyReadException(1L, new IllegalStateException("key decoding failed")),
                SqlTransactionSource.AUTO_COMMIT);

        assertEquals(1L, events.getFirst().rows());
    }

    @Test
    void doesNotHideInvalidOrmObservationStateAsAnObserverFailure() {
        ReactiveSqlObservation observation = ReactiveSqlObservation.start(
                event -> {
                    // The observer is deliberately valid; the invalid row count belongs to the ORM event assembly.
                },
                false,
                false,
                new ReactiveSqlObservation.Request(
                        SqlExecutionOperation.UPDATE,
                        "update device set name = ?",
                        1,
                        0,
                        List.of("sensor")));

        assertThrows(IllegalArgumentException.class,
                () -> observation.success(-1L, SqlTransactionSource.AUTO_COMMIT));
    }

    @Test
    void cancelledMonoUsesTheResolvedExternalTransactionSource() {
        TransactionSourceObserver observer = new TransactionSourceObserver();
        ReactiveSqlExecutionObservationSupport support = support(observer);

        Disposable subscription = support.observeMono(
                SqlExecutionOperation.UPDATE,
                new SqlRequest("update device set active = false", List.of()),
                0,
                Mono.<Long>never())
                .subscribe();
        subscription.dispose();

        assertEquals(List.of(SqlExecutionStatus.CANCELLED), observer.statuses());
        assertEquals(List.of(SqlTransactionSource.EXTERNAL), observer.transactionSources);
    }

    @Test
    void cancelledFluxUsesTheResolvedExternalTransactionSource() {
        TransactionSourceObserver observer = new TransactionSourceObserver();
        ReactiveSqlExecutionObservationSupport support = support(observer);

        Disposable subscription = support.observeFlux(
                SqlExecutionOperation.QUERY,
                new SqlRequest("select id from device", List.of()),
                0,
                Flux.never())
                .subscribe();
        subscription.dispose();

        assertEquals(List.of(SqlExecutionStatus.CANCELLED), observer.statuses());
        assertEquals(List.of(SqlTransactionSource.EXTERNAL), observer.transactionSources);
    }

    private static ReactiveSqlExecutionObservationSupport support(TransactionSourceObserver observer) {
        Connection connection = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> {
                    if ("toString".equals(method.getName())) {
                        return "sql-observation-external-connection";
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        return ReactiveSqlExecutionObservationSupport.create(
                observer,
                BatchExecutionObserver.noop(),
                () -> Mono.just(R2dbcTransactionContext.external(connection)));
    }

    private static final class TransactionSourceObserver implements SqlExecutionObserver {
        private final List<SqlExecutionObservation> events = new ArrayList<>();
        private final List<SqlTransactionSource> transactionSources = new ArrayList<>();

        @Override
        public boolean requiresTransactionSource() {
            return true;
        }

        @Override
        public void onExecution(SqlExecutionObservation observation) {
            events.add(observation);
        }

        @Override
        public void onExecution(SqlExecutionObservation observation,
                                SqlTransactionSource transactionSource) {
            events.add(observation);
            transactionSources.add(transactionSource);
        }

        private List<SqlExecutionStatus> statuses() {
            return events.stream().map(SqlExecutionObservation::status).toList();
        }
    }
}
