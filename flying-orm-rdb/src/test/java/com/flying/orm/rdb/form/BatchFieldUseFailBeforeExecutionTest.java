package com.flying.orm.rdb.form;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.BatchSpec;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class BatchFieldUseFailBeforeExecutionTest {

    @Test
    void unrestrictedUpdateStillChecksExtensionUseBeforeExecution() {
        AtomicInteger executions = new AtomicInteger();
        SqlRenderer conditions = SqlRenderer.builder().addDefaultTerms().addTerm(
                com.flying.orm.core.sql.render.SqlTermHandler.of(
                        com.flying.orm.core.condition.TermExtensionDescriptor.join("join-only", java.util.Set.of(), 1, 1),
                        com.flying.orm.core.condition.ConditionValueShape.SCALAR,
                        (term, context) -> new com.flying.orm.core.sql.render.SqlFragment(
                                context.identifier(term.field()) + " = ?", java.util.List.of(term.value())))).build();
        FormDataSqlRenderer renderer = FormDataSqlRenderer.create(conditions, RdbDialect.postgresql());
        BatchSpec spec = BatchSpec.update(form(), Flux.just(new BatchUpdate(Map.of("secret", "changed"),
                conditions.conditions().where("id", "join-only", 1L).build())));
        SyncFormClient sync = SyncFormClient.create(syncSqlExecutor(), syncBatchExecutor(executions), renderer);
        ReactiveFormClient reactive = ReactiveFormClient.create(reactiveExecutor(executions), renderer);
        assertThrows(IllegalArgumentException.class, () -> sync.writeBatch(spec));
        assertThrows(IllegalArgumentException.class, () -> reactive.writeBatch(spec).block());
        assertEquals(0, executions.get());
    }

    @Test
    void syncBatchRejectsDeniedInsertBeforeTheExecutor() {
        AtomicInteger executions = new AtomicInteger();
        SyncFormClient client = SyncFormClient.create(
                syncSqlExecutor(), syncBatchExecutor(executions), renderer())
                .withFieldUsePolicy(deniesSecret());

        assertThrows(ScopeAccessException.class, () -> client.writeBatch(insertSpec()));
        assertEquals(0, executions.get());
    }

    @Test
    void reactiveBatchRejectsDeniedInsertBeforeTheExecutor() {
        AtomicInteger executions = new AtomicInteger();
        ReactiveFormClient client = ReactiveFormClient.create(reactiveExecutor(executions), renderer())
                .withFieldUsePolicy(deniesSecret());

        assertThrows(ScopeAccessException.class, () -> client.writeBatch(insertSpec()).block());
        assertEquals(0, executions.get());
    }

    private static BatchSpec insertSpec() {
        return BatchSpec.insert(form(), Flux.just(Map.of("id", 1L, "secret", "restricted")));
    }

    private static FieldUsePolicy deniesSecret() {
        return FieldUsePolicy.builder()
                             .visibility("id", FieldVisibility.FULL)
                             .build();
    }

    private static DynamicForm form() {
        return DynamicForm.builder("accounts", "accounts")
                          .addField(DynamicField.primaryKey("id", "BIGINT"))
                          .addField(DynamicField.of("secret", "VARCHAR"))
                          .build();
    }

    private static SyncSqlExecutor syncSqlExecutor() {
        return (SyncSqlExecutor) Proxy.newProxyInstance(
                SyncSqlExecutor.class.getClassLoader(), new Class<?>[]{com.flying.orm.rdb.support.ListQueryFixture.class},
                (proxy, method, arguments) -> {
                    throw new UnsupportedOperationException(method.toString());
                });
    }

    private static SyncBatchExecutor syncBatchExecutor(AtomicInteger executions) {
        return (SyncBatchExecutor) Proxy.newProxyInstance(
                SyncBatchExecutor.class.getClassLoader(), new Class<?>[]{SyncBatchExecutor.class},
                (proxy, method, arguments) -> {
                    executions.incrementAndGet();
                    throw new AssertionError("batch executor must not be reached: " + method.getName());
                });
    }

    private static ReactiveSqlExecutor reactiveExecutor(AtomicInteger executions) {
        return new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(com.flying.orm.core.sql.render.SqlRequest request) {
                return Flux.error(new UnsupportedOperationException("query"));
            }

            @Override
            public Mono<Long> rowsUpdated(com.flying.orm.core.sql.render.SqlRequest request) {
                return Mono.error(new UnsupportedOperationException("rowsUpdated"));
            }

            @Override
            public Mono<com.flying.orm.rdb.batch.BatchExecutionEvidence> writeBatch(BatchWriteRequest request,
                    java.util.function.LongFunction<? extends org.reactivestreams.Publisher<Void>> rowCompleted) {
                executions.incrementAndGet();
                return Mono.error(new AssertionError("batch executor must not be reached: writeBatch"));
            }


        };
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
    }
}
