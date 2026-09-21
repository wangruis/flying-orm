package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlStatementType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JdbcExecutionObservationSupportTest {

    @Test
    void parameterCountDoesNotResnapshotTheRequestAfterExecutionValuesWereCaptured() {
        AtomicInteger copies = new AtomicInteger();
        SqlRequest request = new SqlRequest(
                "update device set seen_at = ?", List.of(new CountingDate(1L, copies)));
        JdbcExecutionObservationSupport observations = JdbcExecutionObservationSupport.create(ignored -> { });
        copies.set(0);

        observations.success(SqlExecutionOperation.UPDATE, request, 1L, System.nanoTime());

        assertEquals(0, copies.get(),
                "observation must reuse the request-owned execution value");
    }

    @Test
    void preservesAffectedRowsWhenGeneratedKeyReadingFails() {
        List<SqlExecutionObservation> events = new ArrayList<>();
        JdbcExecutionObservationSupport observations = JdbcExecutionObservationSupport.create(events::add);

        observations.failure(
                SqlExecutionOperation.UPDATE,
                new SqlRequest("insert into device(name) values (?)", List.of("sensor")),
                0L,
                System.nanoTime(),
                new GeneratedKeyReadException(1L, new IllegalStateException("key decoding failed")));

        assertEquals(1L, events.getFirst().rows());
    }

    @Test
    void doesNotHideInvalidOrmObservationStateAsAnObserverFailure() {
        JdbcExecutionObservationSupport observations = JdbcExecutionObservationSupport.create(event -> {
            // The observer is deliberately valid; the invalid row count belongs to the ORM event assembly.
        });

        assertThrows(IllegalArgumentException.class, () -> observations.success(
                SqlExecutionOperation.UPDATE,
                new SqlRequest("update device set name = ?", List.of("sensor")),
                -1L,
                System.nanoTime()));
    }

    @Test
    void publishesTheStatementTypeAlreadyDerivedByTheExecutionLine() {
        List<SqlExecutionObservation> events = new ArrayList<>();
        JdbcExecutionObservationSupport observations = JdbcExecutionObservationSupport.create(events::add);

        observations.success(
                SqlExecutionOperation.UPDATE,
                new SqlRequest("select misleading_text", List.of()),
                SqlStatementType.UPDATE,
                1L,
                System.nanoTime());

        assertEquals(SqlStatementType.UPDATE, events.getFirst().statementType());
    }

    @Test
    void skipsStatementClassificationWhenObservationAndResultLimitsAreDisabled() {
        JdbcExecutionObservationSupport observations = JdbcExecutionObservationSupport.create(
                SqlExecutionObserver.noop());
        SqlRequest request = new SqlRequest("select value_col from sample", List.of());

        assertEquals(SqlStatementType.UNKNOWN, observations.statementType(request, false));
        assertEquals(SqlStatementType.SELECT, observations.statementType(request, true));
    }

    private static final class CountingDate extends Date {
        private final AtomicInteger copies;

        private CountingDate(long time, AtomicInteger copies) {
            super(time);
            this.copies = copies;
        }

        @Override
        public Object clone() {
            copies.incrementAndGet();
            return new CountingDate(getTime(), copies);
        }
    }
}
