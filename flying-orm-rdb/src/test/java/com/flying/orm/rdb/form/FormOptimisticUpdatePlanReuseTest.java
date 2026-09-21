package com.flying.orm.rdb.form;

import com.flying.orm.core.codec.ValueCodec;
import com.flying.orm.core.codec.ValueCodecRegistry;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.core.protection.EncryptedFieldDefinition;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.sql.render.SqlStatementPlan;
import com.flying.orm.core.sql.render.SqlFragment;
import com.flying.orm.core.sql.render.SqlTermHandler;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.codec.SqlTypedValue;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.internal.binding.SqlNullParameter;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.protection.ProtectedFieldKeyRing;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FormOptimisticUpdatePlanReuseTest {

    @Test
    void planUsesRequestOwnedValuesAndRejectsCountAfterSqlShape() {
        FormDataSqlRenderer renderer = renderer(RdbDialect.postgresql(), new CountingCodec());
        DynamicForm form = form();
        OptimisticLockOptions lock = OptimisticLockOptions.increment("version", 7L);
        SqlRequest first = renderer.update(form, values("first"), where("first"), lock);
        BatchUpdatePlan plan = renderer.optimisticUpdatePlan(
                form, values("first"), where("first"), lock, first);

        byte[] mutable = new byte[]{1, 2};
        SqlRequest mutableRequest = new SqlRequest(first.statement(), List.of(mutable, "owner", 7L));
        mutable[0] = 9;
        ((byte[]) mutableRequest.parameters().getFirst())[1] = 9;
        byte[] extracted = (byte[]) plan.parameters(mutableRequest, 1)[0];
        assertArrayEquals(new byte[]{1, 2}, extracted);
        assertSame(OwnedBindableValues.ownedValues(mutableRequest.parameters()).getFirst(), extracted);

        SqlStatementPlan wrongCount = SqlStatementPlan.canonical(
                first.sql(), first.bindMarkerStyle(), first.statement().parameterCount() + 1);
        SqlRequest wrongCountRequest = new SqlRequest(wrongCount, List.of("a", "b", 7L, "extra"));
        IllegalArgumentException countFailure = assertThrows(IllegalArgumentException.class,
                () -> plan.parameters(wrongCountRequest, 2));
        assertTrue(countFailure.getMessage().contains("different parameter count"));

        SqlStatementPlan wrongShape = SqlStatementPlan.canonical(
                first.sql() + " -- different", first.bindMarkerStyle(),
                first.statement().parameterCount() + 1);
        SqlRequest wrongShapeRequest = new SqlRequest(wrongShape, List.of("a", "b", 7L, "extra"));
        IllegalArgumentException shapeFailure = assertThrows(IllegalArgumentException.class,
                () -> plan.parameters(wrongShapeRequest, 3));
        assertTrue(shapeFailure.getMessage().contains("different SQL shape"));
    }

    @TestFactory
    Stream<DynamicTest> syncBatchExecutionOwnsMutableValuesPerRowAndPerInvocation() {
        return dialectTests(dialect -> checkBatchExecutionOwnership(dialect, false));
    }

    @TestFactory
    Stream<DynamicTest> reactiveBatchExecutionOwnsMutableValuesPerRowAndPerInvocation() {
        return dialectTests(dialect -> checkBatchExecutionOwnership(dialect, true));
    }

    private void checkBatchExecutionOwnership(RdbDialect dialect, boolean reactiveExecution) {
        DynamicForm binaryForm = DynamicForm.builder("binary_updates", "binary_updates")
                .addField(DynamicField.of("payload", "VARBINARY"))
                .addField(DynamicField.of("id", "BIGINT"))
                .addField(DynamicField.of("version", "BIGINT")).build();
        byte[] source = {1, 2};
        BatchOptimisticUpdate update = new BatchOptimisticUpdate(
                Map.of("payload", source), ConditionGroup.and().where("id", "=", 1L).build(),
                OptimisticLockOptions.increment("version", 7L));
        BatchSpec spec = BatchSpec.update(binaryForm, Flux.just(update, update));
        source[0] = 9;
        ((byte[]) update.values().get("payload"))[1] = 9;
        java.util.concurrent.atomic.AtomicInteger consumed = new java.util.concurrent.atomic.AtomicInteger();
        Consumer<Object[]> consume = row -> {
            Object value = row[0];
            if ("oracle".equals(dialect.name())) {
                SqlTypedValue typed = assertInstanceOf(SqlTypedValue.class, value);
                assertEquals(SqlTypedValue.Kind.BLOB, typed.kind());
                value = typed.value();
            }
            byte[] bytes = assertInstanceOf(byte[].class, value);
            assertArrayEquals(new byte[]{1, 2}, bytes);
            bytes[0] = 6;
            consumed.incrementAndGet();
        };
        FormDataSqlRenderer renderer = renderer(dialect, new CountingCodec());
        if (reactiveExecution) {
            CapturingReactiveExecutor reactive = new CapturingReactiveExecutor();
            reactive.onRow = consume;
            Mono<BatchExecutionEvidence> write = ReactiveFormClient.create(reactive, renderer).writeBatch(spec);
            write.block();
            write.block();
        } else {
            CapturingSyncExecutor sync = new CapturingSyncExecutor();
            sync.onRow = consume;
            SyncFormClient syncClient = SyncFormClient.create(sync, sync, renderer);
            syncClient.writeBatch(spec);
            syncClient.writeBatch(spec);
        }
        assertEquals(4, consumed.get());
        assertArrayEquals(new byte[]{1, 2}, (byte[]) update.values().get("payload"));
        assertArrayEquals(new byte[]{9, 2}, source);
    }

    @TestFactory
    Stream<DynamicTest> firstPlanDoesNotReplaySetOrWhereCodecs() {
        return dialectTests(this::checkFirstPlanDoesNotReplaySetOrWhereCodecs);
    }

    @TestFactory
    Stream<DynamicTest> syncScopedUpsertOwnsMutableScopePerRow() {
        return dialectTests(dialect -> checkScopedUpsertOwnership(dialect, false));
    }

    @TestFactory
    Stream<DynamicTest> reactiveScopedUpsertOwnsMutableScopePerRow() {
        return dialectTests(dialect -> checkScopedUpsertOwnership(dialect, true));
    }

    @TestFactory
    Stream<DynamicTest> typedScopedUpsertOwnsMutableScopePerRow() {
        return dialectTests(dialect -> {
            checkScopedUpsertOwnership(dialect, false, true);
            checkScopedUpsertOwnership(dialect, true, true);
        });
    }

    private void checkScopedUpsertOwnership(RdbDialect dialect, boolean reactiveExecution) {
        checkScopedUpsertOwnership(dialect, reactiveExecution, false);
    }

    private void checkScopedUpsertOwnership(RdbDialect dialect, boolean reactiveExecution, boolean typedScope) {
        SqlRenderer sqlRenderer = SqlRenderer.builder().addDefaultTerms()
                .addTerm(SqlTermHandler.of("typed-scope", (term, context) -> new SqlFragment(
                        context.identifier(term.field()) + " = ?",
                        List.of(new SqlTypedValue(SqlTypedValue.Kind.BLOB, term.value()))))).build();
        DynamicForm scopedForm = DynamicForm.builder("scoped_rows", "scoped_rows")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("payload", "VARCHAR"))
                .addField(DynamicField.of("scope_bin", "VARBINARY")).build();
        BatchSpec spec = BatchSpec.upsert(scopedForm, Flux.just(
                Map.<String, Object>of("id", 1L, "payload", "first"),
                Map.<String, Object>of("id", 2L, "payload", "second"),
                Map.<String, Object>of("id", 3L, "payload", "third")))
                .withScope(DataScope.where(sqlRenderer.conditions()
                        .where("scope_bin", typedScope ? "typed-scope" : "=", new byte[]{1, 2}).build()));
        java.util.concurrent.atomic.AtomicInteger consumed = new java.util.concurrent.atomic.AtomicInteger();
        Consumer<Object[]> consume = row -> {
            byte[] scope = null;
            for (Object parameter : row) {
                Object value = parameter instanceof SqlTypedValue typed ? typed.value() : parameter;
                if (value instanceof byte[] bytes) {
                    scope = bytes;
                }
            }
            assertArrayEquals(new byte[]{1, 2}, scope,
                              "scope must remain isolated for row " + consumed.get());
            scope[0] = 6;
            consumed.incrementAndGet();
        };
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(sqlRenderer, dialect);
        if (reactiveExecution) {
            CapturingReactiveExecutor executor = new CapturingReactiveExecutor();
            executor.onRow = consume;
            Mono<BatchExecutionEvidence> write = ReactiveFormClient.create(executor, renderer).writeBatch(spec);
            write.block();
            write.block();
        } else {
            CapturingSyncExecutor executor = new CapturingSyncExecutor();
            executor.onRow = consume;
            SyncFormClient client = SyncFormClient.create(executor, executor, renderer);
            client.writeBatch(spec);
            client.writeBatch(spec);
        }
        assertEquals(6, consumed.get());
    }

    private void checkFirstPlanDoesNotReplaySetOrWhereCodecs(RdbDialect dialect) {
        CountingCodec codec = new CountingCodec();
        FormDataSqlRenderer renderer = renderer(dialect, codec);
        DynamicForm form = form();
        OptimisticLockOptions lock = OptimisticLockOptions.increment("version", 7L);
        renderer.update(form, values("warm"), where("warm"), lock);
        codec.reset();
        Map<String, Object> values = values("first");
        ConditionGroup where = where("first");
        SqlRequest first = renderer.update(form, values, where, lock);
        assertEquals(1, codec.setWrites);
        assertEquals(1, codec.whereWrites);

        BatchUpdatePlan plan = renderer.optimisticUpdatePlan(form, values, where, lock, first);

        assertEquals(1, codec.setWrites, "building the plan must not encode SET values again");
        assertEquals(1, codec.whereWrites, "building the plan must not encode WHERE values again");
        assertSame(first.statement(), plan.statement());
        assertEquals(List.of(String.class, String.class, Long.class), plan.parameterTypes());
        assertArrayEquals(new Object[]{"first", "first", 7L}, plan.parameters(first, 0));
        SqlRequest second = renderer.update(form, values("second"), where("second"), lock);
        assertArrayEquals(new Object[]{"second", "second", 7L}, plan.parameters(second, 1));
        assertEquals(2, codec.setWrites);
        assertEquals(2, codec.whereWrites);
        assertEquals(BatchRowCountPolicy.EXACTLY_ONE,
                     plan.request(Flux.empty(), BatchWriteOptions.defaults()).rowCountPolicy());
        ConditionGroup differentWhere = ConditionGroup.and().where("owner", "<>", "third").build();
        IllegalArgumentException failure = assertThrows(IllegalArgumentException.class,
                () -> plan.parameters(renderer.update(form, values("third"), differentWhere, lock), 2));
        assertTrue(failure.getMessage().contains("different SQL shape"));
    }

    @TestFactory
    Stream<DynamicTest> assignPlanKeepsDeclaredTypesNullsAliasesAndBindingOrder() {
        return dialectTests(this::checkAssignPlanKeepsDeclaredTypesNullsAliasesAndBindingOrder);
    }

    private void checkAssignPlanKeepsDeclaredTypesNullsAliasesAndBindingOrder(RdbDialect dialect) {
        CountingCodec codec = new CountingCodec();
        FormDataSqlRenderer renderer = renderer(dialect, codec);
        DynamicForm form = form();
        Map<String, Object> values = new LinkedHashMap<>();
        values.put("PAYLOAD", null);
        values.put("opaque", new TextValue("encoded", false));
        OptimisticLockOptions lock = OptimisticLockOptions.assign("VERSION", 7L, 8L);
        ConditionGroup where = ConditionGroup.and()
                .where("owner", "=", new TextValue(null, true)).build();
        renderer.update(form, values, where, lock);
        codec.reset();
        SqlRequest first = renderer.update(form, values, where, lock);

        BatchUpdatePlan plan = renderer.optimisticUpdatePlan(form, values, where, lock, first);

        assertEquals(List.of(String.class, Object.class, Long.class, Object.class, Long.class),
                     plan.parameterTypes());
        assertEquals(1, codec.setWrites);
        assertEquals(1, codec.whereWrites);
        assertArrayEquals(new Object[]{new SqlNullParameter(String.class), "encoded", 8L, null, 7L},
                          plan.parameters(first, 0));
        assertSame(first.statement(), plan.statement());
    }

    @TestFactory
    Stream<DynamicTest> physicalProtectedFieldsKeepTheirBindingType() {
        return dialectTests(this::checkPhysicalProtectedFieldsKeepTheirBindingType);
    }

    private void checkPhysicalProtectedFieldsKeepTheirBindingType(RdbDialect dialect) {
        DynamicForm form = DynamicForm.builder("protected_plan", "protected_plan")
                .addField(DynamicField.of("secret", "VARCHAR"))
                .addField(DynamicField.of("owner", "VARCHAR"))
                .addField(DynamicField.of("version", "BIGINT"))
                .encrypted("secret", EncryptedFieldDefinition.builder().build()).build();
        try (ProtectedFieldRuntime runtime = ProtectedFieldRuntime.create(
                ProtectedFieldKeyRing.single("v1", new byte[32]))) {
            FormDataSqlRenderer renderer = renderer(dialect, new CountingCodec()).withProtectedFields(runtime);
            FormScopeSupport scopes = new FormScopeSupport(
                    renderer, StructuredConditionResolver.defaults(), DataScope.none());
            BatchOptimisticUpdate update = new BatchOptimisticUpdate(
                    Map.of("SECRET", "private"), where("first"),
                    OptimisticLockOptions.increment("version", 7L));
            FormScopeSupport.PreparedBatchUpdate prepared = scopes.prepareBatchUpdate(form, update, DataScope.none());

            BatchUpdatePlan plan = renderer.optimisticUpdatePlan(prepared.form(), prepared.values(),
                    prepared.where(), prepared.lock(), prepared.request());

            assertEquals(List.of(byte[].class, byte[].class, String.class, Long.class), plan.parameterTypes());
            Object ciphertext = plan.parameters(prepared.request(), 0)[0];
            if ("oracle".equals(dialect.name())) {
                SqlTypedValue typed = assertInstanceOf(SqlTypedValue.class, ciphertext);
                assertEquals(SqlTypedValue.Kind.BLOB, typed.kind());
                assertInstanceOf(byte[].class, typed.value());
            } else {
                assertInstanceOf(byte[].class, ciphertext);
            }
            assertInstanceOf(byte[].class, plan.parameters(prepared.request(), 0)[1]);
            assertSame(prepared.request().statement(), plan.statement());
            assertThrows(IllegalArgumentException.class, () -> renderer.update(form,
                    Map.of("owner", "changed"), where("first"),
                    OptimisticLockOptions.increment("secret", "old")));
            assertThrows(IllegalArgumentException.class, () -> renderer.update(form,
                    Map.of("VERSION", 8L), where("first"),
                    OptimisticLockOptions.increment("version", 7L)));
        }
    }

    @TestFactory
    Stream<DynamicTest> syncAndReactiveBatchesUseTheSamePreparedPlanWithScope() {
        return dialectTests(this::checkSyncAndReactiveBatchesUseTheSamePreparedPlanWithScope);
    }

    private void checkSyncAndReactiveBatchesUseTheSamePreparedPlanWithScope(RdbDialect dialect) {
        CountingCodec codec = new CountingCodec();
        FormDataSqlRenderer renderer = renderer(dialect, codec);
        CapturingSyncExecutor sync = new CapturingSyncExecutor();
        CapturingReactiveExecutor reactive = new CapturingReactiveExecutor();
        NativeSyncFormBatchOperations syncOperations = new NativeSyncFormBatchOperations(
                sync, renderer, StructuredConditionResolver.defaults(), DataScope.none(), BatchWriteOptions.defaults(),
                com.flying.orm.core.scope.FieldUsePolicy.unrestricted());
        ReactiveFormClient reactiveClient = ReactiveFormClient.create(reactive, renderer);
        BatchSpec warm = batch("warm");
        syncOperations.writeBatch(warm, null);
        codec.reset();

        syncOperations.writeBatch(batch("first"), null);

        assertEquals(2, codec.setWrites, "sync first row is rendered once, as is the later row");
        assertEquals(2, codec.whereWrites);
        codec.reset();
        reactiveClient.writeBatch(batch("first")).block();

        assertEquals(2, codec.setWrites, "reactive first row is rendered once, as is the later row");
        assertEquals(2, codec.whereWrites);
        assertEquals(sync.request.sql(), reactive.request.sql());
        assertEquals(List.of(String.class, String.class, Long.class, Long.class), sync.request.parameterTypes());
        assertEquals(sync.request.parameterTypes(), reactive.request.parameterTypes());
        assertEquals(BatchRowCountPolicy.EXACTLY_ONE, sync.request.rowCountPolicy());
        assertEquals(BatchRowCountPolicy.EXACTLY_ONE, reactive.request.rowCountPolicy());
        assertEquals(2, sync.rows.size());
        assertEquals(2, reactive.rows.size());
        assertArrayEquals(new Object[]{"first", "first", 9L, 7L}, sync.rows.getFirst());
        assertArrayEquals(sync.rows.getFirst(), reactive.rows.getFirst());
        assertArrayEquals(new Object[]{"later", "later", 9L, 7L}, sync.rows.get(1));
        assertArrayEquals(sync.rows.get(1), reactive.rows.get(1));
    }

    private static BatchSpec batch(String first) {
        return BatchSpec.update(form(), Flux.just(update(first), update("later")))
                .withScope(DataScope.where(ConditionGroup.and().where("tenant_id", "=", 9L).build()));
    }

    private static BatchOptimisticUpdate update(String value) {
        return new BatchOptimisticUpdate(values(value), where(value),
                                         OptimisticLockOptions.increment("version", 7L));
    }

    private static Map<String, Object> values(String value) {
        return Map.of("payload", new TextValue(value, false));
    }

    private static ConditionGroup where(String value) {
        return ConditionGroup.and().where("owner", "=", new TextValue(value, true)).build();
    }

    private static DynamicForm form() {
        return DynamicForm.builder("plan_reuse", "plan_reuse")
                .addField(DynamicField.of("payload", "VARCHAR"))
                .addField(DynamicField.of("opaque", "OTHER"))
                .addField(DynamicField.of("owner", "VARCHAR"))
                .addField(DynamicField.of("version", "BIGINT"))
                .addField(DynamicField.of("tenant_id", "BIGINT")).build();
    }

    private static FormDataSqlRenderer renderer(RdbDialect dialect, CountingCodec codec) {
        return FormDataSqlRenderer.create(SqlRenderer.builder().addDefaultTerms()
                .valueCodecs(ValueCodecRegistry.standard().withFirst(codec)).build(), dialect);
    }

    private static Stream<RdbDialect> dialects() {
        return Stream.of(RdbDialect.h2(), RdbDialect.mysql(), RdbDialect.postgresql(),
                         RdbDialect.oracle(), RdbDialect.sqlServer());
    }

    private static Stream<DynamicTest> dialectTests(Consumer<RdbDialect> assertion) {
        return dialects().map(dialect -> DynamicTest.dynamicTest(dialect.name(), () -> assertion.accept(dialect)));
    }

    private record TextValue(String text, boolean predicate) {
    }

    private static final class CountingCodec implements ValueCodec {
        private int setWrites;
        private int whereWrites;

        @Override
        public boolean supports(Class<?> type) {
            return type == TextValue.class;
        }

        @Override
        public Object write(Object value) {
            TextValue text = (TextValue) value;
            if (text.predicate()) {
                whereWrites++;
            } else {
                setWrites++;
            }
            return text.text();
        }

        @Override
        public Object read(Object value, Class<?> type) {
            return value;
        }

        void reset() {
            setWrites = 0;
            whereWrites = 0;
        }
    }

    private static final class CapturingSyncExecutor implements SyncBatchExecutor, SyncSqlExecutor {
        private BatchWriteRequest request;
        private List<Object[]> rows;
        private Consumer<Object[]> onRow = ignored -> { };

        @Override
        public List<DynamicRow> query(SqlRequest value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long rowsUpdated(SqlRequest value) {
            throw new UnsupportedOperationException();
        }

        @Override
        public com.flying.orm.rdb.execution.SqlWriteResult rowsUpdatedReturningKeys(
                SqlRequest value, com.flying.orm.rdb.execution.SqlExecutionOptions options) {
            throw new UnsupportedOperationException();
        }

        @Override
        public BatchExecutionEvidence writeBatch(BatchWriteRequest value, java.util.function.LongConsumer rowCompleted) {
            request = value;
            rows = Flux.from(value.rows()).doOnNext(onRow).collectList().block();
            return com.flying.orm.rdb.repository.BatchEvidenceFixtures.successful(0);
        }

    }

    private static final class CapturingReactiveExecutor implements ReactiveSqlExecutor {
        private BatchWriteRequest request;
        private final List<Object[]> rows = new ArrayList<>();
        private Consumer<Object[]> onRow = ignored -> { };

        @Override
        public Flux<DynamicRow> query(SqlRequest value) {
            return Flux.error(new UnsupportedOperationException());
        }

        @Override
        public Mono<Long> rowsUpdated(SqlRequest value) {
            return Mono.error(new UnsupportedOperationException());
        }

        @Override
        public Mono<BatchExecutionEvidence> writeBatch(BatchWriteRequest value, java.util.function.LongFunction<? extends org.reactivestreams.Publisher<Void>> rowCompleted) {
            request = value;
            return Flux.from(value.rows()).doOnNext(rows::add).doOnNext(onRow)
                    .then(Mono.just(com.flying.orm.rdb.repository.BatchEvidenceFixtures.successful(0)));
        }
    }
}
