package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.form.TenantStrategy;
import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.core.scope.DataScope;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.codec.ArrayValueCodec;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.WriteSpec;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.lock.OptimisticLockConflictException;
import com.flying.orm.rdb.lock.OptimisticLockOptions;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.protection.ProtectedFieldRuntime;
import com.flying.orm.rdb.template.SqlTemplateRegistry;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Modifier;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FormWriteOwnershipConvergenceTest {

    @Test
    void ordinaryUpdateRetainsScopedNullBindingsAndOptimisticLock() {
        DynamicForm form = DynamicForm.builder("events", "events")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("name", "VARCHAR"))
                .addField(DynamicField.of("version", "BIGINT")).build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        FormScopeSupport scopes = new FormScopeSupport(
                renderer, StructuredConditionResolver.defaults(), DataScope.tenant("tenant_id", 7L));
        FormOperationPlanner planner = new FormOperationPlanner(renderer, scopes, SqlExecutionOptions.unlimited());
        Map<String, Object> values = new java.util.LinkedHashMap<>();
        values.put("name", null);
        ConditionGroup where = ConditionGroup.or().where("id", "=", 1L).where("id", "=", 2L).build();
        WriteSpec update = WriteSpec.update(form, values, where);
        for (WriteSpec spec : List.of(update, update.withLock(OptimisticLockOptions.increment("version", 3L)))) {
            DataScope scope = scopes.effectiveScope(spec.scope());
            ConditionGroup scoped = scopes.applyUpdateScope(form, spec.ownedValues(), spec.where(), scope);
            FormPreparedWrite prepared = renderer.protection().prepareWrite(form, spec.ownedValues(), scope);
            ConditionGroup preparedWhere = renderer.protection().prepareQuery(form, scoped, scope, List.of()).where();
            SqlRequest expected = spec.lock().isEmpty()
                    ? renderer.update(prepared.physicalForm(), prepared.values(), preparedWhere)
                    : renderer.update(prepared.physicalForm(), prepared.values(), preparedWhere, spec.lock().orElseThrow());
            FormOperationPlanner.PlannedWrite actual = planner.update(spec);
            assertEquals(expected.sql(), actual.request().sql());
            assertEquals(expected.parameters(), actual.request().parameters());
            assertFalse(actual.protectedWriteRequired());
            assertEquals(spec.lock().orElse(null), actual.lock());
        }
    }

    @Test
    void unlockedWritesDoNotWrapResultsInAnInactiveOptimisticCheck() {
        Mono<Long> rows = Mono.just(0L);
        Mono<SqlWriteResult> keys = Mono.just(new SqlWriteResult(1L, List.of()));
        ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
            @Override public Flux<DynamicRow> query(SqlRequest request) { return Flux.empty(); }
            @Override public Mono<Long> rowsUpdated(SqlRequest request) { return rows; }
            @Override public Mono<SqlWriteResult> rowsUpdatedReturningKeys(
                    SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) { return keys; }
        };
        DynamicForm form = DynamicForm.builder("events", "events")
                .addField(DynamicField.of("name", "VARCHAR"))
                .addField(DynamicField.of("version", "BIGINT")).build();
        ReactiveFormClient client = ReactiveFormClient.create(executor, FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql()));
        WriteSpec insert = WriteSpec.insert(form, Map.of("name", "created"));
        WriteSpec update = WriteSpec.update(form, Map.of("name", "changed"), ConditionGroup.and().build());
        WriteSpec delete = WriteSpec.delete(form, ConditionGroup.and().build());
        org.junit.jupiter.api.Assertions.assertAll(
                () -> assertSame(rows, client.insert(insert)),
                () -> assertSame(rows, client.update(update)),
                () -> assertSame(rows, client.delete(delete)),
                () -> assertSame(rows, client.physicalDelete(delete)),
                () -> assertSame(keys, client.insertReturningKeys(insert)));
        assertEquals(0L, client.update(update).block());
        OptimisticLockOptions lock = OptimisticLockOptions.increment("version", 1L);
        assertThrows(OptimisticLockConflictException.class, () -> client.update(update.withLock(lock)).block());
        assertThrows(OptimisticLockConflictException.class, () -> client.delete(delete.withLock(lock)).block());
        assertThrows(OptimisticLockConflictException.class,
                () -> client.physicalDelete(delete.withLock(lock)).block());
        assertThrows(IllegalArgumentException.class, () -> client.insertReturningKeys(insert.withLock(lock)));
    }

    @Test
    void scalarInsertPublishesItsOwnedParametersAndSharesTheBatchStatement() {
        DynamicForm form = DynamicForm.builder("events", "events")
                .addField(DynamicField.of("name", "VARCHAR")).build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        SqlRequest request = renderer.insert(form, Map.of("name", "single"));
        assertTrue(OwnedBindableValues.isPublished(request.parameters()),
                "insert must publish its completed framework-owned parameter layout");
        BatchWriteRequest batch = renderer.insertBatch(
                form, List.of(Map.of("name", "batch")));
        assertSame(request.statement(), batch.statement());
        assertEquals(List.of("single"), request.parameters());
        assertThrows(UnsupportedOperationException.class,
                () -> request.parameters().set(0, "changed"));
    }

    @Test
    void noTenantAndNoProtectionReuseTheWriteSpecOwnedValues() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("name", "VARCHAR"))
                                      .build();
        WriteSpec spec = WriteSpec.insert(form, Map.of("name", "created"));
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        FormScopeSupport guard = new FormScopeSupport(
                renderer, StructuredConditionResolver.defaults(), DataScope.none());

        Map<String, Object> scoped = guard.prepareWriteValues(
                form, spec.ownedValues(), guard.effectiveScope(spec.scope()));
        FormPreparedWrite prepared = renderer.protection().prepareWrite(
                form, scoped, DataScope.none());

        assertSame(spec.ownedValues(), scoped);
        assertSame(scoped, prepared.values());
    }

    @Test
    void directPublicRendererStillSnapshotsMutableBinaryParameters() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("payload", "VARBINARY"))
                                      .build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        byte[] source = {1, 2, 3};

        com.flying.orm.core.sql.render.SqlRequest request = renderer.insert(
                form, Map.of("payload", source));
        source[0] = 9;

        assertEquals(1, ((byte[]) request.parameters().getFirst())[0]);
    }

    @Test
    void ownedSeamsRemainInternalIncludingTheSharedArrayDecoder() throws Exception {
        assertFalse(publicMethod(SqlRequest.class, "owned"));
        assertFalse(publicMethod(ProtectedFieldRuntime.class, "prepareOwnedWrite"));
        assertFalse(publicMethod(SqlTemplateRegistry.class, "entry"));
        assertFalse(Arrays.stream(SqlTemplateRegistry.class.getDeclaredClasses())
                          .anyMatch(type -> type.getSimpleName().equals("Entry")
                                  && Modifier.isPublic(type.getModifiers())));
        var readList = ArrayValueCodec.class.getDeclaredMethod("readList", Object.class, Class.class,
                com.flying.orm.core.codec.ValueCodecRegistry.class);
        org.junit.jupiter.api.Assertions.assertTrue(Modifier.isPublic(readList.getModifiers()));
        org.junit.jupiter.api.Assertions.assertTrue(readList.isAnnotationPresent(
                com.flying.orm.rdb.internal.InternalApi.class));
    }

    @Test
    void publicSqlRequestCannotForgeOwnedMutableValues() {
        byte[] bytes = {1};
        ByteBuffer buffer = ByteBuffer.wrap(new byte[]{2});
        SqlRequest request = new SqlRequest("select ?, ?", List.of(bytes, buffer));

        bytes[0] = 9;
        buffer.put(0, (byte) 8);

        assertEquals(1, ((byte[]) request.parameters().get(0))[0]);
        assertEquals(2, ((ByteBuffer) request.parameters().get(1)).get(0));
    }

    @Test
    void matchingCanonicalAutoTenantReusesTheOwnedMap() {
        DynamicForm form = DynamicForm.builder("events", "events")
                                      .addField(DynamicField.of("tenant_id", "BIGINT"))
                                      .addField(DynamicField.of("name", "VARCHAR"))
                                      .tenant("tenant_id", TenantStrategy.AUTO)
                                      .build();
        WriteSpec spec = WriteSpec.insert(form, Map.of("tenant_id", 7L, "name", "created"));
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
        FormScopeSupport guard = new FormScopeSupport(
                renderer, StructuredConditionResolver.defaults(), DataScope.tenant("tenant_id", 7L));

        Map<String, Object> prepared = guard.prepareWriteValues(
                form, spec.ownedValues(), guard.effectiveScope(spec.scope()));

        assertSame(spec.ownedValues(), prepared);
    }

    private static boolean publicMethod(Class<?> type, String name) {
        return Arrays.stream(type.getDeclaredMethods())
                     .anyMatch(method -> method.getName().equals(name)
                             && Modifier.isPublic(method.getModifiers()));
    }
}
