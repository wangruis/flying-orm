package com.flying.orm.rdb.compat;

import com.flying.orm.core.condition.StructuredConditionInput;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.page.CursorPageQuery;
import com.flying.orm.core.page.CursorSort;
import com.flying.orm.core.page.PageSort;
import com.flying.orm.core.protection.SensitiveDisplayMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchAffectedRows;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.form.spec.QuerySpec;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.operator.DatabaseOperator;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.schema.ReactiveSchemaClient;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.RecordComponent;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.BitSet;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 保全既有查询、批量、可信 SQL 和 Schema 行为，并核对已批准的 4.1.0 架构契约。 */
class LegacyBehaviorCharacterizationTest {

    private static final Duration TEST_TIMEOUT = Duration.ofSeconds(2);

    @Test
    void querySpecRemainsImmutableAndShowSensitiveOnlyChangesDisplayMode() {
        DynamicForm form = form();
        DataScope scope = DataScope.all();
        SqlExecutionOptions executionOptions = SqlExecutionOptions.safeDefaults();
        List<String> projections = new ArrayList<>(List.of("id", "name"));
        List<String> groups = new ArrayList<>(List.of("name"));
        List<PageSort> sorts = new ArrayList<>(List.of(PageSort.asc("name")));

        QuerySpec base = QuerySpec.structured(
                form, StructuredConditionInput.term("id", "eq", 7L));
        QuerySpec shaped = base.withScope(scope)
                .withProjection(projections, groups)
                .withSorts(sorts)
                .withExecutionOptions(executionOptions);
        projections.set(0, "name");
        groups.clear();
        sorts.clear();

        assertEquals(List.of("id", "name"), shaped.projections());
        assertEquals(List.of("name"), shaped.groups());
        assertEquals(List.of(PageSort.asc("name")), shaped.sorts());
        assertEquals(SensitiveDisplayMode.DECLARED, base.sensitiveDisplayMode());
        QuerySpec full = shaped.showSensitive();
        assertEquals(SensitiveDisplayMode.FULL, full.sensitiveDisplayMode());
        assertEquals(SensitiveDisplayMode.DECLARED, shaped.sensitiveDisplayMode());
        assertNotSame(shaped, full);
        assertSame(shaped.form(), full.form());
        assertSame(shaped.where(), full.where());
        assertSame(shaped.scope(), full.scope());
        assertEquals(shaped.projections(), full.projections());
        assertEquals(shaped.groups(), full.groups());
        assertEquals(shaped.sorts(), full.sorts());
        assertEquals(shaped.executionOptions(), full.executionOptions());
        assertEquals(shaped.structuredInput(), full.structuredInput());
        assertEquals(shaped.structuredPolicy(), full.structuredPolicy());
    }

    @Test
    void cursorPageKeepsADeepSnapshotAndFirstPageMeaning() {
        byte[] mutableValue = new byte[]{1, 2, 3};
        CursorPageQuery first = CursorPageQuery.first(20, CursorSort.asc("id"));
        CursorPageQuery after = CursorPageQuery.after(20, List.of(mutableValue), CursorSort.asc("id"));
        mutableValue[0] = 9;

        assertTrue(first.firstPage());
        assertFalse(after.firstPage());
        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) after.cursor().getFirst());
        byte[] returned = (byte[]) after.cursor().getFirst();
        returned[1] = 9;
        assertArrayEquals(new byte[]{1, 2, 3}, (byte[]) after.cursor().getFirst());
    }

    @Test
    void batchResultReportsExecutionTruthWithoutInventingTransactionOutcome() {
        var facts = new BatchExecutionEvidence.Accumulator();
        facts.accept(2);
        facts.succeeded(2, BatchAffectedRows.known(2));
        var successful = facts.snapshot(BatchExecutionState.SUCCESS, null);
        facts.accept(1);
        facts.terminal(1, new BitSet(), new BitSet(), BatchAffectedRows.unknown(), List.of());
        var partial = facts.snapshot(BatchExecutionState.UNKNOWN,
                BatchExecutionEvidence.Failure.from(new IllegalStateException("driver failed")));
        assertEquals(BatchExecutionState.SUCCESS, successful.state());
        assertEquals(BatchAffectedRows.known(2), successful.affectedRows());
        assertEquals(2, successful.inputCount());
        assertEquals(BatchExecutionState.UNKNOWN, partial.state());
        assertEquals(3, partial.inputCount());
        assertEquals(2, partial.successfulCount());
        assertEquals(0, partial.failedCount());
        assertEquals(BatchAffectedRows.unknown(), partial.affectedRows());
    }

    @Test
    void trustedOperatorKeepsNamedValuesParameterizedAndSchemaEntryLazy() {
        CapturingExecutor executor = new CapturingExecutor();
        DatabaseOperator operator = DatabaseOperator.create(
                executor, SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());

        operator.unsafeNativeSql("update compat_users set name = :name where id = :id")
                .bind("id", 7L)
                .bind("name", "Ada")
                .execute()
                .block(TEST_TIMEOUT);

        assertEquals("update compat_users set name = $1 where id = $2", executor.lastRequest.sql());
        assertEquals(List.of("Ada", 7L), executor.lastRequest.parameters());
        int executedBeforeBuilder = executor.executions;
        assertTrue(operator.ddl().createOrAlter("compat_users") != null);
        assertEquals(executedBeforeBuilder, executor.executions,
                     "creating a Schema builder must not read metadata or execute DDL");

        ReactiveSchemaClient.create(executor, RdbDialect.postgresql()).createTable(form()).block(TEST_TIMEOUT);
        assertTrue(executor.lastRequest.sql().startsWith("create table \"compat_users\""));
    }

    @Test
    void keepsApprovedRdbRecordShapes() {
        assertRecord(BatchWriteOptions.class, "bufferSize:int", "maxRows:long",
                "maxBufferedBytes:long", "maxRowBytes:long");
        assertRecord(BatchWriteRequest.class,
                "statement:com.flying.orm.core.sql.render.SqlStatementPlan",
                "parameterTypes:java.util.List", "rows:org.reactivestreams.Publisher",
                "options:com.flying.orm.rdb.batch.BatchWriteOptions",
                "rowCountPolicy:com.flying.orm.rdb.batch.BatchRowCountPolicy",
                "generatedKeys:com.flying.orm.rdb.batch.BatchGeneratedKeys");
        assertRecord(BatchExecutionObservation.class,
                "eventType:com.flying.orm.rdb.observation.BatchExecutionEventType",
                "request:com.flying.orm.rdb.observation.BatchExecutionObservation$BatchWriteRequestView",
                "state:com.flying.orm.rdb.batch.BatchExecutionState", "inputCount:long",
                "affectedRows:com.flying.orm.rdb.batch.BatchAffectedRows",
                "successfulCount:long", "failedCount:long", "conflictCount:long", "durationNanos:long",
                "failureCategory:com.flying.orm.rdb.observation.SqlFailureCategory",
                "failure:com.flying.orm.rdb.batch.BatchExecutionEvidence$Failure");
        assertRecord(BatchExecutionObservation.BatchWriteRequestView.class,
                "sql:java.lang.String", "parameterCount:int",
                "backend:com.flying.orm.rdb.observation.SqlExecutionBackend");
        assertFalse(BatchExecutionObservation.class.isSealed());
    }

    private static DynamicForm form() {
        return DynamicForm.builder("compat_users", "compat_users")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR"))
                .build();
    }

    private static void assertRecord(Class<?> type, String... expectedComponents) {
        assertTrue(type.isRecord(), () -> type.getName() + " must remain a record");
        List<String> actual = Arrays.stream(type.getRecordComponents())
                .map(LegacyBehaviorCharacterizationTest::componentShape)
                .toList();
        assertEquals(List.of(expectedComponents), actual, () -> type.getName() + " record components changed");
    }

    private static String componentShape(RecordComponent component) {
        return component.getName() + ":" + component.getType().getTypeName();
    }

    private static final class CapturingExecutor implements ReactiveSqlExecutor {

        private SqlRequest lastRequest;
        private int executions;

        @Override
        public Flux<DynamicRow> query(SqlRequest request) {
            lastRequest = request;
            executions++;
            return Flux.empty();
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest request) {
            lastRequest = request;
            executions++;
            return Mono.just(1L);
        }
    }
}
