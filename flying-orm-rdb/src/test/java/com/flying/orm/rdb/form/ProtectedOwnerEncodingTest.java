package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.protection.EncryptedSearchMode;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.*;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.*;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.mapping.EntityTypeMappingRegistry;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongFunction;

import static org.junit.jupiter.api.Assertions.*;

class ProtectedOwnerEncodingTest {
    @Test
    void expressionBeforeOwnerKeepsTheSameSideIndexOwnerInSingleAndBatchPlans() {
        DynamicForm form = DynamicForm.builder("items", "items")
                .addField(DynamicField.of("quantity", "INTEGER"))
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .encrypted("secret", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.CONTAINS).build()).build();
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql()).withProtectedFields(runtime);
            Map<String, Object> values = new LinkedHashMap<>();
            values.put("quantity", com.flying.orm.core.expression.ValueExpression.value(2)
                    .add(com.flying.orm.core.expression.ValueExpression.value(3)));
            values.put("id", 9L);
            values.put("secret", "alphabet");
            FormOperationPlanner planner = new FormOperationPlanner(renderer,
                    new FormScopeSupport(renderer, StructuredConditionResolver.defaults(), DataScope.none()),
                    SqlExecutionOptions.unlimited());
            FormOperationPlanner.PlannedWrite single = planner.insert(WriteSpec.insert(form, values));
            assertEquals(Map.of("id", 9L), single.protectedWrite().resolveInsertOwner(new SqlWriteResult(1, List.of())));
            FormProtectionSqlSupport.WriteOperation operation = renderer.protection().writeOperation(
                    form, renderer.protection().physicalForm(form), DataScope.none());
            FormPreparedWrite prepared = operation.prepare(values);
            BatchInsertPlan batch = renderer.batchRenderer.insertPlan(prepared.physicalForm(), prepared.values());
            assertEquals(Map.of("id", 9L), operation.insertOwner(batch, batch.firstParameters()));
        }
    }
    @TestFactory
    List<DynamicTest> ciphertextAndContainsIndexShareOneEncoding() {
        List<DynamicTest> tests = new ArrayList<>();
        for (String mode : List.of("sync/generic", "reactive/generic", "sync/field", "reactive/field")) {
            boolean reactive = mode.startsWith("reactive");
            boolean fieldCodec = mode.endsWith("field");
            for (String operation : List.of("INSERT", "UPDATE", "BATCH_INSERT", "BATCH_UPDATE", "UPSERT")) {
                tests.add(DynamicTest.dynamicTest(mode + "/" + operation, () -> {
                    AtomicInteger encodes = new AtomicInteger();
                    ValueCodec codec = new ValueCodec() {
                        public boolean supports(Class<?> type) { return type == String.class; }
                        public Object write(Object value) { return value + "-" + encodes.incrementAndGet(); }
                        public Object read(Object value, Class<?> type) { return value; }
                    };
                    ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(codec);
                    DynamicForm form = DynamicForm.builder("encoded", "encoded")
                            .addField(DynamicField.primaryKey("id", "BIGINT"))
                            .addField(DynamicField.of("secret", "VARCHAR"))
                            .encrypted("secret", EncryptedFieldDefinition.builder()
                                    .searchModes(EncryptedSearchMode.CONTAINS).build()).build();
                    try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                            ProtectedFieldKeyRing.single("v1", new byte[32]))) {
                        EntityTypeMappingRegistry mappings = EntityTypeMappingRegistry.builder()
                                .registerNamed("coded", String.class, form.field("secret").databaseType(), codec).build();
                        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                                SqlRenderer.builder().addDefaultTerms().build().withValueCodecs(codecs),
                                RdbDialect.postgresql()).withProtectedFields(runtime).withEntityFieldCodecs(fieldCodec
                                        ? Map.of(form.field("secret"), mappings.resolve("coded", String.class)) : Map.of());
                        Capture capture = new Capture(work -> {
                            Object ciphertext = work.writeRequest().parameters().stream()
                                    .filter(byte[].class::isInstance).findFirst().orElseThrow();
                            Object text = runtime.resultOperation(form, DataScope.none(),
                                    com.flying.orm.core.protection.SensitiveDisplayMode.FULL,
                                    ValueCodecRegistry.standard()).transform(
                                            DynamicRow.copyOf(Map.of("secret", ciphertext))).value(0);
                            ProtectedFieldRuntime.ContainsFieldTokens expected = runtime.writeOperation(
                                    form, renderer.protection().physicalForm(form), DataScope.none(),
                                    ValueCodecRegistry.standard()).containsTokens(Map.of("secret", text)).getFirst();
                            assertEquals(expected.tokenCount(), work.fields().getFirst().tokens().size());
                            for (int index = 0; index < expected.tokenCount(); index++) {
                                assertEquals(expected.ownedToken(index), work.fields().getFirst().ownedToken(index),
                                        "side index must describe the actual encrypted text");
                            }
                        });
                        SyncFormClient sync = SyncFormClient.create(capture, capture, renderer);
                        ReactiveFormClient async = ReactiveFormClient.create(capture.reactive(), renderer);
                        Map<String, Object> insert = Map.of("id", 7L, "secret", "alphabet");
                        Map<String, Object> values = Map.of("secret", "alphabet");
                        ConditionGroup where = ConditionGroup.and().where("id", "=", 7L).build();
                        if (operation.equals("INSERT")) {
                            if (reactive) async.insert(WriteSpec.insert(form, insert)).block();
                            else sync.insert(WriteSpec.insert(form, insert));
                        } else if (operation.equals("UPDATE")) {
                            if (reactive) async.update(WriteSpec.update(form, values, where)).block();
                            else sync.update(WriteSpec.update(form, values, where));
                        } else {
                            BatchSpec spec = switch (operation) {
                                case "BATCH_UPDATE" -> BatchSpec.update(form,
                                        Flux.just(new BatchUpdate(values, where), new BatchUpdate(values, where)));
                                case "UPSERT" -> BatchSpec.upsert(form, Flux.just(insert, insert));
                                default -> BatchSpec.insert(form, Flux.just(insert, insert));
                            };
                            if (reactive) async.writeBatch(spec).block();
                            else sync.writeBatch(spec);
                        }
                        assertEquals(operation.equals("INSERT") || operation.equals("UPDATE") ? 1 : 2,
                                capture.seen);
                        assertEquals(capture.seen, encodes.get(), "encode each protected value only once");
                    }
                }));
            }
        }
        return tests;
    }

    @Test
    void preparedContainsValuesPreserveNullOmissionAndCanonicalFieldRules() {
        DynamicForm form = DynamicForm.builder("encoded", "encoded")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .encrypted("secret", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.CONTAINS).build()).build();
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql()).withProtectedFields(runtime);
            DynamicForm physical = renderer.protection().physicalForm(form);
            FormProtectionSqlSupport.WriteOperation operation = renderer.protection()
                    .writeOperation(form, physical, DataScope.none());
            FormPreparedWrite cleared = operation.prepare(java.util.Collections.singletonMap("SECRET", null));
            assertTrue(cleared.values().containsKey("secret"));
            assertNull(cleared.values().get("secret"));
            assertEquals(1, cleared.containsTokens().size());
            assertEquals(0, cleared.containsTokens().getFirst().tokenCount());
            assertTrue(operation.prepare(Map.of("id", 1L)).containsTokens().isEmpty());
            assertThrows(UnsupportedOperationException.class, () -> cleared.containsTokens().clear());
            Map<String, Object> duplicate = new LinkedHashMap<>();
            duplicate.put("secret", null);
            duplicate.put("SECRET", "alphabet");
            assertEquals("duplicate normalized protected write field",
                    assertThrows(IllegalArgumentException.class, () -> operation.prepare(duplicate)).getMessage());
            FormPreparedWrite owner = operation.prepare(Map.of("ID", 1L));
            assertThrows(IllegalArgumentException.class, () -> operation.protectedWrite(owner,
                    renderer.insert(owner.physicalForm(), owner.values()), null, ProtectedWriteWork.Kind.UPDATE, Map.of()));
            assertTrue(renderer.protection().prepareWrite(form, Map.of("secret", "alphabet"),
                    DataScope.none()).containsTokens().isEmpty(), "raw SQL preparation must not generate side work");
        }
    }

    @TestFactory
    List<DynamicTest> defaultCodecPreservesNumericCarriersInPublicBigintBatchUpsert() {
        return List.of(false, true).stream().map(reactive -> DynamicTest.dynamicTest(
                reactive ? "reactive" : "jdbc", () -> {
                    DynamicForm form = DynamicForm.builder("numeric_owners", "numeric_owners")
                            .addField(DynamicField.primaryKey("id", "BIGINT"))
                            .addField(DynamicField.of("secret", "VARCHAR"))
                            .encrypted("secret", EncryptedFieldDefinition.builder()
                                    .searchModes(EncryptedSearchMode.CONTAINS).build()).build();
                    try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                            ProtectedFieldKeyRing.single("v1", new byte[32]))) {
                        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql())
                                .withProtectedFields(runtime);
                        List<ProtectedWriteWork> captured = new ArrayList<>();
                        Capture capture = new Capture(captured::add);
                        List<Object> ids = List.of(Integer.valueOf(7), Long.valueOf(7));
                        BatchSpec spec = BatchSpec.upsert(form, Flux.fromIterable(ids)
                                .map(id -> Map.<String, Object>of("id", id, "secret", "alphabet")));

                        if (reactive) ReactiveFormClient.create(capture.reactive(), renderer).writeBatch(spec).block();
                        else SyncFormClient.create(capture, capture, renderer).writeBatch(spec);

                        assertEquals(2, captured.size());
                        for (int index = 0; index < ids.size(); index++) {
                            Object id = ids.get(index);
                            ProtectedWriteWork work = captured.get(index);
                            assertEquals(ProtectedWriteWork.Kind.UPSERT, work.kind());
                            assertEquals(Map.of("id", id), work.knownOwner());
                            assertEquals(id.getClass(), work.knownOwner().get("id").getClass());
                            assertTrue(work.writeRequest().parameters().contains(id));
                            assertEquals(id, work.sideIndexParameters(work.knownOwner(),
                                    work.fields().getFirst(), 0).getFirst());
                        }
                    }
                })).toList();
    }

    @TestFactory
    List<DynamicTest> generatedOwnerRemainsMissingWithOmittedOrNullInput() {
        List<DynamicTest> tests = new ArrayList<>();
        for (boolean explicitNull : List.of(false, true)) {
            for (boolean batch : List.of(false, true)) {
                tests.add(DynamicTest.dynamicTest("null=" + explicitNull + "/batch=" + batch, () -> {
                    DynamicForm form = DynamicForm.builder("generated", "generated")
                            .addField(DynamicField.primaryKey("id", "BIGINT"))
                            .addField(DynamicField.of("secret", "VARCHAR"))
                            .encrypted("secret", EncryptedFieldDefinition.builder()
                                    .searchModes(EncryptedSearchMode.CONTAINS).build()).build();
                    try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                            ProtectedFieldKeyRing.single("v1", new byte[32]))) {
                        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql())
                                .withProtectedFields(runtime);
                        Map<String, Object> logical = new LinkedHashMap<>();
                        if (explicitNull) logical.put("id", null);
                        logical.put("secret", "alphabet");
                        var physical = renderer.protection().physicalForm(form);
                        var operation = renderer.protection().writeOperation(form, physical, DataScope.none());
                        var prepared = operation.prepare(logical);
                        BatchInsertPlan insert = renderer.batchRenderer.insertPlan(prepared.physicalForm(), prepared.values());
                        var request = insert.singleRequest();
                        Map<String, Object> owner;
                        if (batch) {
                            var plan = renderer.batchRenderer.insertPlan(physical, prepared.values());
                            owner = operation.insertOwner(plan, plan.firstParameters());
                        } else {
                            owner = operation.insertOwner(insert, insert.firstParameters());
                        }
                        var work = operation.protectedWrite(prepared, request, null,
                                ProtectedWriteWork.Kind.INSERT, owner).orElseThrow();
                        assertTrue(work.requiresGeneratedKeys());
                        assertEquals("id", work.generatedOwnerField());
                        assertEquals(Map.of("id", 7L), work.resolveInsertOwner(new SqlWriteResult(
                                1, List.of(DynamicRow.copyOf(Map.of("id", 7L))))));
                    }
                }));
            }
        }
        return tests;
    }

    @TestFactory
    List<DynamicTest> reusesEncodedCompositeOwnerForSingleAndBatchWrites() {
        List<DynamicTest> tests = new ArrayList<>();
        for (RdbDialect dialect : List.of(RdbDialect.mysql(), RdbDialect.postgresql(),
                RdbDialect.oracle(), RdbDialect.sqlServer())) {
            for (boolean reactive : List.of(false, true)) {
                for (String operation : List.of("INSERT", "BATCH_INSERT", "UPSERT", "SCOPED_UPSERT")) {
                    tests.add(DynamicTest.dynamicTest(dialect.name() + "/" + reactive + "/" + operation,
                            () -> verify(dialect, reactive, operation)));
                }
            }
        }
        return tests;
    }

    private static void verify(RdbDialect dialect, boolean reactive, String operation) {
        AtomicInteger encodes = new AtomicInteger();
        ValueCodecRegistry codecs = ValueCodecRegistry.standard().withFirst(new ValueCodec() {
            public boolean supports(Class<?> type) { return type == Long.class; }
            public Object write(Object value) { encodes.incrementAndGet(); return (Long) value + 100L; }
            public Object read(Object value, Class<?> type) { return ((Number) value).longValue() - 100L; }
        });
        DynamicForm form = DynamicForm.builder("owners", "owners")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.primaryKey("region", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .encrypted("secret", EncryptedFieldDefinition.builder()
                        .searchModes(EncryptedSearchMode.CONTAINS).build()).build();
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                    SqlRenderer.builder().addDefaultTerms().build().withValueCodecs(codecs), dialect)
                    .withProtectedFields(runtime);
            Capture capture = new Capture();
            ReactiveFormClient async = ReactiveFormClient.create(capture.reactive(), renderer);
            SyncFormClient sync = SyncFormClient.create(capture, capture, renderer);
            if (operation.equals("INSERT")) {
                if (reactive) async.insert(WriteSpec.insert(form, row(7L, false))).block();
                else sync.insert(WriteSpec.insert(form, row(7L, false)));
            } else {
                Flux<Map<String, Object>> rows = Flux.just(row(7L, false), row(8L, true));
                BatchSpec spec = operation.endsWith("UPSERT") ? BatchSpec.upsert(form, rows) : BatchSpec.insert(form, rows);
                if (operation.equals("SCOPED_UPSERT")) {
                    spec = spec.withScope(DataScope.where(ConditionGroup.and().where("region", "=", 9L).build()));
                }
                if (reactive) async.writeBatch(spec).block();
                else sync.writeBatch(spec);
            }
            int expectedRows = operation.equals("INSERT") ? 1 : 2;
            assertEquals(expectedRows, capture.seen);
            assertEquals(expectedRows * 2 + (operation.equals("SCOPED_UPSERT") ? 1 : 0), encodes.get(),
                    "each owner component and the shared Scope must be encoded only once");
        }
    }

    private static Map<String, Object> row(long id, boolean reverse) {
        Map<String, Object> row = new LinkedHashMap<>();
        if (reverse) { row.put("region", 9L); row.put("secret", "alphabet"); row.put("ID", id); }
        else { row.put("secret", "alphabet"); row.put("ID", id); row.put("region", 9L); }
        return row;
    }

    private static final class Capture implements com.flying.orm.rdb.support.ListQueryFixture, SyncBatchExecutor {
        private int seen;
        private final Consumer<ProtectedWriteWork> verifier;

        private Capture() {
            this(null);
        }

        private Capture(Consumer<ProtectedWriteWork> verifier) {
            this.verifier = verifier;
        }

        private SqlWriteResult accept(ProtectedWriteWork work) {
            if (verifier != null) {
                verifier.accept(work);
                seen++;
                return new SqlWriteResult(1, List.of());
            }
            long expectedId = 107L + seen++;
            assertTrue(work.writeRequest().parameters().contains(expectedId));
            assertEquals(Map.of("id", expectedId, "region", 109L), work.knownOwner());
            List<Object> side = work.sideIndexParameters(work.knownOwner(), work.fields().getFirst(), 0);
            assertEquals(List.of(expectedId, 109L), side.subList(0, 2));
            assertFalse(work.requiresGeneratedKeys());
            return new SqlWriteResult(1, List.of());
        }

        private Mono<BatchExecutionEvidence> batch(BatchWriteRequest request) {
            return Flux.from(request.rows()).doOnNext(row -> accept(
                    ProtectedBatchRows.admit(row, request.parameterCount(), Long.MAX_VALUE, "test bytes").work()))
                    .count().map(count -> {
                        BatchExecutionEvidence.Accumulator facts = new BatchExecutionEvidence.Accumulator();
                        facts.accept(count);
                        facts.succeeded(count, BatchAffectedRows.known(count));
                        return facts.snapshot(BatchExecutionState.SUCCESS, null);
                    });
        }

        private ReactiveSqlExecutor reactive() {
            return new ReactiveSqlExecutor() {
                public Flux<DynamicRow> query(SqlRequest request) { return Flux.error(new AssertionError("unexpected query")); }
                public Mono<Long> rowsUpdated(SqlRequest request) { return Mono.error(new AssertionError("unprotected write")); }
                public Mono<SqlWriteResult> protectedWrite(ProtectedWriteWork work, SqlExecutionOptions options) {
                    return Mono.fromSupplier(() -> accept(work));
                }
                public Mono<BatchExecutionEvidence> writeProtectedBatch(BatchWriteRequest request,
                        LongFunction<? extends Publisher<Void>> callback) { return batch(request); }
            };
        }

        public List<DynamicRow> query(SqlRequest request) { throw new AssertionError("unexpected query"); }
        public long rowsUpdated(SqlRequest request) { throw new AssertionError("unprotected write"); }
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
            throw new AssertionError("unexpected generated keys");
        }
        public SqlWriteResult protectedWrite(ProtectedWriteWork work, SqlExecutionOptions options) { return accept(work); }
        public BatchExecutionEvidence writeBatch(BatchWriteRequest request, LongConsumer callback) {
            throw new AssertionError("unprotected batch");
        }
        public BatchExecutionEvidence writeProtectedBatch(BatchWriteRequest request, LongConsumer callback) {
            return batch(request).block();
        }
    }
}
