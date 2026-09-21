package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.reactive.ReactiveSqlExecutionObservationSupport.ReactiveSqlObservation;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.observation.BatchExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionStatus;
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
        ReactiveSqlObservation observation = observation(events::add,
                new ReactiveSqlObservation.Request(
                        SqlExecutionOperation.QUERY,
                        "select id from device",
                        0,
                        0,
                        List.of()));

        observation.incrementRows();
        observation.incrementRows();
        observation.success();
        observation.cancelled();

        assertEquals(1, events.size());
        assertEquals(SqlExecutionStatus.SUCCESS, events.getFirst().status());
        assertEquals(2L, events.getFirst().rows());
    }

    @Test
    void preservesAffectedRowsWhenGeneratedKeyReadingFails() {
        List<SqlExecutionObservation> events = new ArrayList<>();
        ReactiveSqlObservation observation = observation(
                events::add,
                new ReactiveSqlObservation.Request(
                        SqlExecutionOperation.UPDATE,
                        "insert into device(name) values (?)",
                        1,
                        0,
                        List.of("sensor")));

        observation.error(
                new GeneratedKeyReadException(1L, new IllegalStateException("key decoding failed")));

        assertEquals(1L, events.getFirst().rows());
    }

    @Test
    void doesNotHideInvalidOrmObservationStateAsAnObserverFailure() {
        ReactiveSqlObservation observation = observation(
                event -> {
                    // The observer is deliberately valid; the invalid row count belongs to the ORM event assembly.
                },
                new ReactiveSqlObservation.Request(
                        SqlExecutionOperation.UPDATE,
                        "update device set name = ?",
                        1,
                        0,
                        List.of("sensor")));

        assertThrows(IllegalArgumentException.class,
                () -> observation.success(-1L));
    }

    @Test
    void cancelledMonoPublishesCancellation() {
        RecordingObserver observer = new RecordingObserver();
        ReactiveSqlExecutionObservationSupport support = support(observer);

        Disposable subscription = support.observeMono(
                SqlExecutionOperation.UPDATE,
                new SqlRequest("update device set active = false", List.of()),
                0,
                Mono.<Long>never())
                .subscribe();
        subscription.dispose();

        assertEquals(List.of(SqlExecutionStatus.CANCELLED), observer.statuses());
    }

    @Test
    void cancelledFluxPublishesCancellation() {
        RecordingObserver observer = new RecordingObserver();
        ReactiveSqlExecutionObservationSupport support = support(observer);

        Disposable subscription = support.observeFlux(
                SqlExecutionOperation.QUERY,
                new SqlRequest("select id from device", List.of()),
                0,
                Flux.never())
                .subscribe();
        subscription.dispose();

        assertEquals(List.of(SqlExecutionStatus.CANCELLED), observer.statuses());
    }

    private static ReactiveSqlObservation observation(
            SqlExecutionObserver observer,
            ReactiveSqlObservation.Request request) {
        return ReactiveSqlExecutionObservationSupport.create(
                observer, BatchExecutionObserver.noop()).start(request);
    }

    private static ReactiveSqlExecutionObservationSupport support(RecordingObserver observer) {
        return ReactiveSqlExecutionObservationSupport.create(observer, BatchExecutionObserver.noop());
    }

    private static final class RecordingObserver implements SqlExecutionObserver {
        private final List<SqlExecutionObservation> events = new ArrayList<>();
        public void onExecution(SqlExecutionObservation observation) { events.add(observation); }
        private List<SqlExecutionStatus> statuses() {
            return events.stream().map(SqlExecutionObservation::status).toList();
        }
    }
}
