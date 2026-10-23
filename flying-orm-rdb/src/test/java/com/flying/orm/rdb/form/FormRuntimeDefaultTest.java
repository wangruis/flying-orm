package com.flying.orm.rdb.form;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchAffectedRows;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.internal.mapping.RepositoryUpsertValues;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.support.ListQueryFixture;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;

import static org.junit.jupiter.api.Assertions.*;

class FormRuntimeDefaultTest {
    @Test
    void singleInsertFillsNullButPreservesExplicitValuesAndNeverFillsUpdates() {
        AtomicInteger calls = new AtomicInteger();
        DynamicForm form = form(calls);
        FormDataSqlRenderer renderer = renderer();
        FormOperationPlanner planner = new FormOperationPlanner(renderer,
                new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                SqlExecutionOptions.unlimited());
        Map<String, Object> input = Collections.singletonMap("CREATED", null);
        assertEquals(List.of("generated-1"), planner.insert(WriteSpec.insert(form, input)).request().parameters());
        assertNull(input.get("CREATED"));
        assertEquals(List.of("explicit"),
                planner.insert(WriteSpec.insert(form, Map.of("created", "explicit"))).request().parameters());
        SqlRequest update = planner.update(WriteSpec.update(form, input, ConditionGroup.and().build())).request();
        assertEquals(1, update.parameters().size());
        assertEquals(1, calls.get());
    }

    @Test
    void generatedDefaultsBelongToInsertStageOnlyAndDoNotChangePhysicalSchema() {
        AtomicInteger calls = new AtomicInteger();
        DynamicForm form = form(calls);
        Map<String, Object> input = Map.of("id", 1L, "name", "n");
        RepositoryUpsertValues prepared = assertInstanceOf(RepositoryUpsertValues.class,
                RepositoryUpsertValues.prepareInsert(form, input, false, Set.of()));
        assertEquals(Map.of("id", 1L, "name", "n", "created", "generated-1"), prepared.insertValues());
        assertEquals(input, prepared.updateValues());
        assertEquals(Map.of("id", 1L, "name", "n"), input);
        DynamicForm other = form(new AtomicInteger());
        assertTrue(form.diffTo(other).changedFields().isEmpty());
        assertEquals(form.structureFingerprint(), other.structureFingerprint());
    }

    @Test
    void syncAndReactiveInsertAndUpsertGenerateOncePerRowAtExecution() {
        for (boolean reactive : List.of(false, true)) {
            for (boolean upsert : List.of(false, true)) {
                AtomicInteger calls = new AtomicInteger();
                DynamicForm form = form(calls);
                Capture capture = new Capture();
                Flux<Map<String, Object>> rows = Flux.just(Map.of("id", 1L, "name", "a"),
                        Map.of("id", 2L, "name", "b"));
                BatchSpec spec = upsert ? BatchSpec.upsert(form, rows) : BatchSpec.insert(form, rows);
                if (reactive) {
                    Mono<BatchExecutionEvidence> pending =
                            ReactiveFormClient.create(capture.reactive(), renderer()).writeBatch(spec);
                    assertEquals(0, calls.get());
                    pending.block();
                    assertEquals(2, calls.get());
                    pending.block();
                    assertEquals(4, calls.get());
                } else {
                    SyncFormClient.create(capture, capture, renderer()).writeBatch(spec);
                    assertEquals(2, calls.get());
                }
                assertTrue(capture.rows.getFirst().contains("generated-1"));
                assertTrue(capture.rows.get(1).contains("generated-2"));
                if (upsert) {
                    String update = capture.sql.substring(capture.sql.indexOf("do update set"));
                    assertFalse(update.contains("created"), capture.sql);
                    assertTrue(update.contains("name"), capture.sql);
                }
            }
        }
    }

    @Test
    void mixedUpsertRowsKeepTheirOwnUpdateColumns() {
        for (boolean reactive : List.of(false, true)) {
            AtomicInteger calls = new AtomicInteger();
            DynamicForm form = form(calls);
            Capture capture = new Capture();
            BatchSpec spec = BatchSpec.upsert(form, Flux.just(Map.of("id", 1L, "name", "a"),
                    Map.of("id", 2L, "name", "b", "created", "explicit"), Map.of("id", 3L, "name", "c")));
            if (reactive) ReactiveFormClient.create(capture.reactive(), renderer()).writeBatch(spec).block();
            else SyncFormClient.create(capture, capture, renderer()).writeBatch(spec);
            assertEquals(2, calls.get());
            assertFalse(capture.sqlRows.get(0).substring(capture.sqlRows.get(0).indexOf("do update set")).contains("created"));
            assertTrue(capture.sqlRows.get(1).contains("\"created\" = excluded.\"created\""));
            assertFalse(capture.sqlRows.get(2).substring(capture.sqlRows.get(2).indexOf("do update set")).contains("created"));
            assertTrue(capture.rows.get(1).contains("explicit"));
            assertTrue(capture.rows.get(2).contains("generated-2"));
        }
    }

    @Test
    void nullResultLeavesOriginalValuesAndDefaultFailurePropagates() {
        AtomicInteger calls = new AtomicInteger();
        DynamicForm form = DynamicForm.builder("t", "t").addField(DynamicField.of("name", "VARCHAR")
                .withRuntimeDefault(() -> { calls.incrementAndGet(); return null; })).build();
        Map<String, Object> row = Collections.singletonMap("name", null);
        assertSame(row, RepositoryUpsertValues.prepareInsert(form, row, false, Set.of()));
        assertEquals(1, calls.get());
        RuntimeException failure = new IllegalArgumentException("application generator failed");
        DynamicForm failing = DynamicForm.builder("t", "t").addField(DynamicField.of("name", "VARCHAR")
                .withRuntimeDefault(() -> { throw failure; })).build();
        assertSame(failure, assertThrows(IllegalArgumentException.class,
                () -> RepositoryUpsertValues.prepareInsert(failing, row, false, Set.of())));
    }

    private static DynamicForm form(AtomicInteger calls) {
        return DynamicForm.builder("items", "items")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR"))
                .addField(DynamicField.of("created", "VARCHAR")
                        .withRuntimeDefault(() -> "generated-" + calls.incrementAndGet()).withLength(80)).build();
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
    }

    private static final class Capture implements ListQueryFixture, SyncBatchExecutor {
        final List<List<Object>> rows = new ArrayList<>();
        final List<String> sqlRows = new ArrayList<>();
        String sql;
        public boolean supportsBatchRowLayouts() { return true; }
        public List<DynamicRow> query(SqlRequest request) { throw new AssertionError("unexpected query"); }
        public long rowsUpdated(SqlRequest request) { throw new AssertionError("unexpected single write"); }
        public BatchExecutionEvidence writeBatch(BatchWriteRequest request, LongConsumer callback) {
            return batch(request).block();
        }
        Mono<BatchExecutionEvidence> batch(BatchWriteRequest request) {
            sql = request.sql();
            return Flux.from(request.rows()).doOnNext(row -> {
                ProtectedBatchRows.RowView view = ProtectedBatchRows.admit(
                        row, request.parameterCount(), Long.MAX_VALUE, "test");
                sqlRows.add(view.request(request).sql());
                rows.add(java.util.Arrays.asList(row).subList(0, view.parameterCount()));
            })
                    .count().map(count -> {
                        BatchExecutionEvidence.Accumulator facts = new BatchExecutionEvidence.Accumulator();
                        facts.accept(count);
                        facts.succeeded(count, BatchAffectedRows.known(count));
                        return facts.snapshot(BatchExecutionState.SUCCESS, null);
                    });
        }
        ReactiveSqlExecutor reactive() {
            return new ReactiveSqlExecutor() {
                public boolean supportsBatchRowLayouts() { return true; }
                public Flux<DynamicRow> query(SqlRequest request) { return Flux.error(new AssertionError("query")); }
                public Mono<Long> rowsUpdated(SqlRequest request) { return Mono.error(new AssertionError("single")); }
                public Mono<BatchExecutionEvidence> writeBatch(BatchWriteRequest request,
                        java.util.function.LongFunction<? extends org.reactivestreams.Publisher<Void>> callback) {
                    return batch(request);
                }
            };
        }
    }
}
