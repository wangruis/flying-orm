package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.observation.SqlExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlTransactionSource;
import com.flying.orm.rdb.observation.SqlStatementType;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JdbcExecutionObservationSupportTest {

    @Test
    void preservesAffectedRowsWhenGeneratedKeyReadingFails() {
        List<SqlExecutionObservation> events = new ArrayList<>();
        JdbcExecutionObservationSupport observations = JdbcExecutionObservationSupport.create(events::add);

        observations.failure(
                SqlExecutionOperation.UPDATE,
                new SqlRequest("insert into device(name) values (?)", List.of("sensor")),
                0L,
                System.nanoTime(),
                new GeneratedKeyReadException(1L, new IllegalStateException("key decoding failed")),
                SqlTransactionSource.AUTO_COMMIT);

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
                System.nanoTime(),
                SqlTransactionSource.AUTO_COMMIT));
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
                System.nanoTime(),
                SqlTransactionSource.AUTO_COMMIT);

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
}
