package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchResolution;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionSequence;
import com.flying.orm.rdb.execution.SqlExecutionSequenceResult;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;

/**
 * 给执行器补一份默认保护。调用方不传 options 时用默认值，显式传入时以本次配置为准。
 * 这是包内装饰器，统一从 {@link ReactiveSqlExecutor#withDefaultExecutionOptions(SqlExecutionOptions)} 进入。
 * 原生执行器在连接可用后实施 SQL timeout；只实现单参数方法的自定义执行器由接口默认逻辑保留结果容量保护，
 * 不会越权给可能包含连接池排队的整个 Publisher 计时。
 * 对象创建后字段不再变化，可以和底层执行器一样被多个请求并发复用。
 *
 * @author wangr
 * @date 2026-07-30
 * @version v1.0
 */
final class DefaultOptionsReactiveSqlExecutor implements ReactiveSqlExecutor, ConnectionScopedReactiveSqlExecutor {

    private final ReactiveSqlExecutor delegate;

    private final SqlExecutionOptions options;

    private DefaultOptionsReactiveSqlExecutor(ReactiveSqlExecutor delegate, SqlExecutionOptions options) {
        this.delegate = Objects.requireNonNull(delegate, "reactive sql executor must not be null");
        this.options = Objects.requireNonNull(options, "sql execution options must not be null");
    }

    static ReactiveSqlExecutor create(ReactiveSqlExecutor delegate, SqlExecutionOptions options) {
        ReactiveSqlExecutor safeDelegate = Objects.requireNonNull(delegate, "reactive sql executor must not be null");
        // 重复设置默认值时直接替换旧包装，避免应用启动阶段叠出没有意义的委托层。
        if (safeDelegate instanceof DefaultOptionsReactiveSqlExecutor defaulted) {
            safeDelegate = defaulted.delegate;
        }
        return new DefaultOptionsReactiveSqlExecutor(safeDelegate, options);
    }

    @Override
    public Mono<R2dbcTransactionContext> currentTransaction() {
        return delegate.currentTransaction();
    }

    @Override
    public Flux<DynamicRow> query(SqlRequest request) {
        return delegate.query(request, options);
    }

    @Override
    public Flux<DynamicRow> query(SqlRequest request, SqlExecutionOptions options) {
        return delegate.query(request, options);
    }

    @Override
    public Mono<Long> rowsUpdated(SqlRequest request) {
        return delegate.rowsUpdated(request, options);
    }

    @Override
    public Mono<Long> rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
        return delegate.rowsUpdated(request, options);
    }

    @Override
    public Mono<SqlWriteResult> rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
        return delegate.rowsUpdatedReturningKeys(request, options);
    }

    /** 显式保护写入已经携带本次执行选项；装饰器只负责保持底层原子能力。 */
    @Override
    public Mono<SqlWriteResult> atomicProtectedWrite(ProtectedWriteWork work, SqlExecutionOptions options) {
        return delegate.atomicProtectedWrite(work, options);
    }

    @Override
    public Mono<SqlExecutionSequenceResult> executeInConnection(SqlExecutionSequence sequence,
                                                                SqlExecutionOptions options) {
        if (delegate instanceof ConnectionScopedReactiveSqlExecutor scoped) {
            return scoped.executeInConnection(sequence, options);
        }
        return Mono.error(new UnsupportedOperationException(
                "wrapped reactive SQL executor does not support connection-scoped execution"));
    }

    @Override
    public Mono<BatchWriteResult> writeBatch(BatchWriteRequest request) {
        return delegate.writeBatch(request);
    }

    @Override
    public Mono<BatchWriteResult> writeProtectedBatch(BatchWriteRequest request) {
        return delegate.writeProtectedBatch(request);
    }

    @Override
    public Flux<BatchChunkResult> writeBatchChunks(BatchWriteRequest request) {
        return delegate.writeBatchChunks(request);
    }

    @Override
    public Flux<BatchChunkResult> writeProtectedBatchChunks(BatchWriteRequest request) {
        return delegate.writeProtectedBatchChunks(request);
    }

    @Override
    public Mono<BatchResolution> resolveUnknown(BatchChunkResult.RecoveryToken token) {
        return delegate.resolveUnknown(token);
    }
}
