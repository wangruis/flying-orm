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
    void syncBatchResultEvidenceAndChunksRejectDeniedInsertBeforeTheExecutor() {
        AtomicInteger executions = new AtomicInteger();
        SyncFormClient client = SyncFormClient.create(
                syncSqlExecutor(), syncBatchExecutor(executions), renderer())
                .withFieldUsePolicy(deniesSecret());

        assertThrows(ScopeAccessException.class, () -> client.writeBatch(insertSpec()));
        assertThrows(ScopeAccessException.class, () -> client.writeBatchEvidence(insertSpec()));
        assertThrows(ScopeAccessException.class, () -> client.writeBatchChunks(independentInsertSpec()));
        assertEquals(0, executions.get());
    }

    @Test
    void reactiveBatchResultEvidenceAndChunksRejectDeniedInsertBeforeTheExecutor() {
        AtomicInteger executions = new AtomicInteger();
        ReactiveFormClient client = ReactiveFormClient.create(reactiveExecutor(executions), renderer())
                .withFieldUsePolicy(deniesSecret());

        assertThrows(ScopeAccessException.class, () -> client.writeBatch(insertSpec()).block());
        assertThrows(ScopeAccessException.class, () -> client.writeBatchEvidence(insertSpec()).block());
        assertThrows(ScopeAccessException.class,
                     () -> client.writeBatchChunks(independentInsertSpec()).collectList().block());
        assertEquals(0, executions.get());
    }

    private static BatchSpec insertSpec() {
        return BatchSpec.insert(form(), Flux.just(Map.of("secret", "classified")));
    }

    private static BatchSpec independentInsertSpec() {
        return insertSpec().withOptions(BatchWriteOptions.unlimitedIndependent(1, 1));
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
                SyncSqlExecutor.class.getClassLoader(), new Class<?>[]{SyncSqlExecutor.class},
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
            public Mono<com.flying.orm.rdb.batch.BatchWriteResult> writeBatch(BatchWriteRequest request) {
                executions.incrementAndGet();
                return Mono.error(new AssertionError("batch executor must not be reached: writeBatch"));
            }

            @Override
            public Mono<com.flying.orm.rdb.batch.BatchExecutionEvidence> writeBatchEvidence(
                    BatchWriteRequest request) {
                executions.incrementAndGet();
                return Mono.error(new AssertionError("batch executor must not be reached: writeBatchEvidence"));
            }

            @Override
            public Flux<com.flying.orm.rdb.batch.BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
                executions.incrementAndGet();
                return Flux.error(new AssertionError("batch executor must not be reached: writeBatchChunks"));
            }
        };
    }

    private static FormDataSqlRenderer renderer() {
        return FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), RdbDialect.postgresql());
    }
}
