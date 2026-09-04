package com.flying.orm.rdb.lock;

import com.flying.orm.core.form.DynamicField;
import com.flying.orm.core.form.DynamicForm;
import com.flying.orm.core.sql.render.SqlRenderer;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.form.FormDataSqlRenderer;
import com.flying.orm.rdb.form.ReactiveFormClient;
import com.flying.orm.rdb.form.SyncFormClient;
import com.flying.orm.rdb.reactive.ReactiveSqlExecutor;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.sync.SyncBatchExecutor;
import com.flying.orm.rdb.sync.SyncSqlExecutor;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import io.r2dbc.spi.Connection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.lang.reflect.Proxy;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

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
                                     Supplier<Optional<JdbcTransactionContext>> transaction,
                                     AtomicInteger queries,
                                     AtomicReference<SqlRequest> request) {
        SyncSqlExecutor executor = new SyncSqlExecutor() {
            @Override
            public Optional<JdbcTransactionContext> currentTransaction() {
                return transaction.get();
            }

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
            public BatchWriteResult writeBatch(BatchWriteRequest batch) {
                throw new UnsupportedOperationException("test executor does not batch");
            }

            @Override
            public List<com.flying.orm.rdb.batch.BatchChunkResult> writeBatchChunks(
                    BatchWriteRequest batch) {
                throw new UnsupportedOperationException("test executor does not batch");
            }
        };
        return SyncFormClient.create(executor, batches, renderer(dialect));
    }

    static ReactiveFormClient reactiveClient(
            RdbDialect dialect,
            Supplier<Mono<R2dbcTransactionContext>> transaction,
            AtomicInteger queries,
            AtomicReference<SqlRequest> request) {
        ReactiveSqlExecutor executor = new ReactiveSqlExecutor() {
            @Override
            public Mono<R2dbcTransactionContext> currentTransaction() {
                return Mono.defer(transaction);
            }

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

    static JdbcTransactionContext jdbcTransaction(AtomicInteger lifecycleCalls) {
        java.sql.Connection connection = (java.sql.Connection) Proxy.newProxyInstance(
                java.sql.Connection.class.getClassLoader(),
                new Class<?>[]{java.sql.Connection.class},
                (proxy, method, arguments) -> {
                    if (method.getName().equals("commit")
                            || method.getName().equals("rollback")
                            || method.getName().equals("close")) {
                        lifecycleCalls.incrementAndGet();
                        return null;
                    }
                    if (method.getName().equals("isClosed")) {
                        return false;
                    }
                    throw new SQLException("unexpected test connection call: " + method.getName());
                });
        return JdbcTransactionContext.external(connection);
    }

    static R2dbcTransactionContext r2dbcTransaction(AtomicInteger lifecycleCalls) {
        Connection connection = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(), new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> switch (method.getName()) {
                    case "beginTransaction", "commitTransaction", "rollbackTransaction", "close" -> {
                        lifecycleCalls.incrementAndGet();
                        yield Mono.empty();
                    }
                    case "isAutoCommit" -> false;
                    default -> throw new UnsupportedOperationException(method.toString());
                });
        return R2dbcTransactionContext.external(connection);
    }
}
