package com.flying.orm.rdb.reactive;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.ConnectionAccessTestSupport;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class R2dbcBatchInsertValuesTest {
    private final FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
            SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
    private final DynamicForm form = DynamicForm.builder("items", "items")
            .addField(DynamicField.primaryKey("id", "BIGINT"))
            .addField(DynamicField.of("name", "VARCHAR")).build();

    @Test
    void zeroParameterRowsExecuteIndividuallyWithoutBindingBatches() {
        Capture capture = new Capture();
        BatchWriteRequest request = renderer.insertBatch(form, List.of(Map.of(), Map.of(), Map.of()));
        BatchExecutionEvidence evidence = execute(capture, request);
        assertEquals(3, capture.executions);
        assertEquals(0, capture.adds);
        assertEquals(3, evidence.affectedRows().value());
        assertEquals(List.of(0L, 1L, 2L), evidence.successfulOffsets().boxed().toList());
    }

    @Test
    void failedZeroParameterRowDoesNotInventSuccessfulOffsets() {
        Capture capture = new Capture();
        capture.failExecution = 2;
        BatchWriteRequest request = renderer.insertBatch(form, List.of(Map.of(), Map.of(), Map.of()));
        BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                () -> execute(capture, request));
        assertEquals(BatchExecutionState.PARTIAL, failure.evidence().state());
        assertEquals(List.of(0L), failure.evidence().successfulOffsets().boxed().toList());
        assertEquals(2, capture.executions);
        assertEquals(0, capture.adds);
    }

    @Test
    void mixedLayoutsKeepAdjacentRowsInNativeBatchesAndBindTheirOwnNullTypes() {
        Capture capture = new Capture();
        R2dbcSqlExecutor executor = R2dbcSqlExecutor.create(ConnectionAccessTestSupport.borrowed(capture.connection()),
                RdbDialect.postgresql());
        BatchExecutionEvidence evidence = com.flying.orm.rdb.form.ReactiveFormClient.create(executor, renderer)
                .writeBatch(com.flying.orm.rdb.form.spec.BatchSpec.insert(form, Flux.fromIterable(List.of(
                        row(1, "a"), row(2, "b"), Map.of("id", 3L), Map.of("id", 4L), row(5, null))))
                        .withOptions(BatchWriteOptions.of(10))).block();
        assertEquals(List.of("insert into \"items\" (\"id\", \"name\") values ($1, $2), ($3, $4)",
                "insert into \"items\" (\"id\") values ($1), ($2)",
                "insert into \"items\" (\"id\", \"name\") values ($1, $2)"), capture.sql);
        assertEquals(List.of(List.of(1L, "a", 2L, "b"), List.of(3L, 4L), Arrays.asList(5L, null)), capture.bindings);
        assertEquals(List.of(String.class), capture.nullTypes);
        assertEquals(5, evidence.successfulCount());
        assertEquals(5, evidence.affectedRows().value());
    }

    @Test
    void combinesEachInputWindowWithoutChangingBindingOrderOrNullTypes() {
        Capture capture = new Capture();
        BatchWriteRequest request = renderer.insertBatch(form,
                List.of(row(1, "a?'"), row(2, null), row(3, "c")), BatchWriteOptions.of(2));
        BatchExecutionEvidence evidence = execute(capture, request);
        assertEquals(List.of("insert into \"items\" (\"id\", \"name\") values ($1, $2), ($3, $4)",
                "insert into \"items\" (\"id\", \"name\") values ($1, $2)"), capture.sql);
        assertEquals(List.of(Arrays.asList(1L, "a?'", 2L, null), List.of(3L, "c")), capture.bindings);
        assertEquals(List.of(String.class), capture.nullTypes);
        assertEquals(0, capture.adds);
        assertEquals(BatchExecutionState.SUCCESS, evidence.state());
        assertEquals(3, evidence.successfulCount());
        assertEquals(3, evidence.affectedRows().value());
    }

    @Test
    void doesNotRewriteCallerSqlEvenWhenItsTextMatchesAnInsertPlan() {
        Capture capture = new Capture();
        BatchWriteRequest source = renderer.insertBatch(form, List.of(row(1, "a"), row(2, "b")));
        BatchWriteRequest external = new BatchWriteRequest(SqlStatementPlan.canonical(
                source.sql(), source.bindMarkerStyle(), source.parameterCount()), source.parameterTypes(),
                source.rows(), source.options(), source.rowCountPolicy(), source.generatedKeys());
        assertEquals(2, execute(capture, external).successfulCount());
        assertEquals(1, capture.adds);
        assertFalse(capture.sql.getFirst().contains("), ("));
    }

    @Test
    void exactlyOnePolicyStillExecutesAndChecksEachRow() {
        Capture capture = new Capture();
        BatchWriteRequest source = renderer.insertBatch(form, List.of(row(1, "a"), row(2, "b")));
        BatchWriteRequest strict = new BatchWriteRequest(source.statement(), source.parameterTypes(),
                source.rows(), source.options(), BatchRowCountPolicy.EXACTLY_ONE, source.generatedKeys());
        assertEquals(2, execute(capture, strict).successfulCount());
        assertEquals(2, capture.sql.size());
        assertEquals(0, capture.adds);
    }

    @Test
    void upsertsKeepTheirConflictAndScopeSql() {
        Capture capture = new Capture();
        BatchWriteRequest request = renderer.upsertBatch(form, List.of(row(1, "a"), row(2, "b")));
        assertEquals(2, execute(capture, request).successfulCount());
        assertEquals(1, capture.adds);
        assertTrue(capture.sql.getFirst().contains("on conflict"));
        assertFalse(capture.sql.getFirst().contains("), ("));
    }

    @Test
    void failedLaterWindowDoesNotLoseEarlierSuccessfulOffsets() {
        Capture capture = new Capture();
        capture.failExecution = 2;
        BatchWriteRequest request = renderer.insertBatch(form,
                List.of(row(1, "a"), row(2, "b"), row(3, "c"), row(4, "d")), BatchWriteOptions.of(2));
        BatchExecutionEvidenceException failure = assertThrows(BatchExecutionEvidenceException.class,
                () -> execute(capture, request));
        assertEquals(BatchExecutionState.PARTIAL, failure.evidence().state());
        assertEquals(List.of(0L, 1L), failure.evidence().successfulOffsets().boxed().toList());
        assertFalse(failure.evidence().affectedRows().isKnown());
        assertEquals(0, capture.adds);
    }

    private static BatchExecutionEvidence execute(Capture capture, BatchWriteRequest request) {
        return R2dbcSqlExecutor.create(ConnectionAccessTestSupport.borrowed(capture.connection()),
                RdbDialect.postgresql()).writeBatch(request).block();
    }

    private static Map<String, Object> row(long id, String name) {
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("id", id);
        row.put("name", name);
        return row;
    }

    private static final class Capture {
        final List<String> sql = new ArrayList<>();
        final List<List<Object>> bindings = new ArrayList<>();
        final List<Class<?>> nullTypes = new ArrayList<>();
        int adds;
        int executions;
        int failExecution;

        Connection connection() {
            return proxy(Connection.class, (self, method, args) -> {
                if (!method.getName().equals("createStatement")) throw new AssertionError(method.getName());
                String statementSql = (String) args[0];
                sql.add(statementSql);
                int columns = statementSql.endsWith(" default values") ? 0
                        : statementSql.substring(statementSql.indexOf('(') + 1, statementSql.indexOf(')')).split(",").length;
                List<Object> values = new ArrayList<>();
                bindings.add(values);
                int[] groupOffset = {0};
                return proxy(Statement.class, (statement, called, input) -> switch (called.getName()) {
                    case "bind", "bindNull" -> {
                        assertEquals(values.size(), groupOffset[0] + (Integer) input[0]);
                        if (called.getName().equals("bindNull")) nullTypes.add((Class<?>) input[1]);
                        values.add(called.getName().equals("bindNull") ? null : input[1]);
                        yield statement;
                    }
                    case "add" -> { adds++; groupOffset[0] = values.size(); yield statement; }
                    case "execute" -> ++executions == failExecution
                            ? Flux.error(new io.r2dbc.spi.R2dbcDataIntegrityViolationException("duplicate", "23505", 0))
                            : Flux.just(proxy(Result.class, (result, resultMethod, ignored) -> {
                                if (!resultMethod.getName().equals("getRowsUpdated"))
                                    throw new AssertionError(resultMethod.getName());
                                // PostgreSQL zero-bind SQL uses one simple query, irrespective of add().
                                return Mono.just(columns == 0 ? 1L : (long) values.size() / columns);
                            }));
                    default -> throw new AssertionError(called.getName());
                });
            });
        }
    }

    private static <T> T proxy(Class<T> type, InvocationHandler handler) {
        return type.cast(Proxy.newProxyInstance(type.getClassLoader(), new Class<?>[]{type}, handler));
    }
}
