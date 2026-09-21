package com.flying.orm.rdb.lock;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import java.util.function.LongConsumer;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

final class LockingReadTestSupport {

    private LockingReadTestSupport() {
    }

    static DynamicForm form() {
        return DynamicForm.builder("accounts", "accounts")
                .addField(DynamicField.primaryKey("id", "BIGINT"))
                .addField(DynamicField.of("tenant_id", "BIGINT"))
                .addField(DynamicField.of("secret", "VARCHAR"))
                .build();
    }

    static FormDataSqlRenderer renderer(RdbDialect dialect) {
        return FormDataSqlRenderer.create(
                SqlRenderer.builder().addDefaultTerms().build(), dialect);
    }

    static SyncFormClient syncClient(RdbDialect dialect,
                                     AtomicInteger queries,
                                     AtomicReference<SqlRequest> request) {
        SyncSqlExecutor executor = new SyncSqlExecutor() {
            @Override
            public List<DynamicRow> query(SqlRequest sql) {
                request.set(sql);
                queries.incrementAndGet();
                return List.of();
            }

            @Override
            public long rowsUpdated(SqlRequest sql) {
                throw new UnsupportedOperationException("test executor does not write");
            }

            @Override
            public SqlWriteResult rowsUpdatedReturningKeys(
                    SqlRequest sql,
                    com.flying.orm.rdb.execution.SqlExecutionOptions options) {
                throw new UnsupportedOperationException("test executor does not write");
            }
        };
        SyncBatchExecutor batches = new SyncBatchExecutor() {
            @Override
            public BatchExecutionEvidence writeBatch(BatchWriteRequest batch, LongConsumer rowCompleted) {
                throw new UnsupportedOperationException("test executor does not batch");
            }

        };
        return SyncFormClient.create(executor, batches, renderer(dialect));
    }

    static ReactiveFormClient reactiveClient(
            RdbDialect dialect,
            AtomicInteger queries,
            AtomicReference<SqlRequest> request) {
        ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
            @Override
            public Flux<DynamicRow> query(SqlRequest sql) {
                return Flux.defer(() -> {
                    request.set(sql);
                    queries.incrementAndGet();
                    return Flux.empty();
                });
            }

            @Override
            public Mono<Long> rowsUpdated(SqlRequest sql) {
                return Mono.error(new UnsupportedOperationException("test executor does not write"));
            }
        };
        return ReactiveFormClient.create(executor, renderer(dialect));
    }

}
