package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionSequence;
import com.flying.orm.rdb.execution.SqlExecutionSequenceResult;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.result.DynamicRow;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongFunction;
import org.reactivestreams.Publisher;

/**
 * Package-local base for decorators that change one executor policy and preserve every other capability.
 */
abstract class ForwardingReactiveSqlExecutor implements ReactiveSqlExecutor {

    private final ReactiveSqlExecutor delegate;

    ForwardingReactiveSqlExecutor(ReactiveSqlExecutor delegate) {
        this.delegate = Objects.requireNonNull(delegate, "reactive sql executor must not be null");
    }

    ReactiveSqlExecutor delegate() {
        return delegate;
    }

    /** Rebuilds one logical policy layer around a new delegate. Structural wrappers never enter this hook. */
    ForwardingReactiveSqlExecutor redecoratePolicy(ReactiveSqlExecutor delegate) {
        return new ForwardingReactiveSqlExecutor(delegate) { };
    }

    /**
     * Removes every occurrence of one policy while rebuilding the remaining logical chain once. This keeps policy
     * replacement independent of decorator order and avoids pair-wise unwrapping branches in every factory.
     */
    static ReactiveSqlExecutor withoutPolicy(
            ReactiveSqlExecutor executor,
            Class<? extends ForwardingReactiveSqlExecutor> policyType,
            Consumer<ForwardingReactiveSqlExecutor> removedPolicy) {
        ReactiveSqlExecutor safeExecutor = Objects.requireNonNull(
                executor, "reactive SQL executor must not be null");
        Class<? extends ForwardingReactiveSqlExecutor> safeType = Objects.requireNonNull(
                policyType, "reactive SQL policy type must not be null");
        Consumer<ForwardingReactiveSqlExecutor> safeRemoved = Objects.requireNonNull(
                removedPolicy, "removed policy consumer must not be null");
        return withoutPolicy0(safeExecutor, safeType, safeRemoved);
    }

    private static ReactiveSqlExecutor withoutPolicy0(
            ReactiveSqlExecutor executor,
            Class<? extends ForwardingReactiveSqlExecutor> policyType,
            Consumer<ForwardingReactiveSqlExecutor> removedPolicy) {
        ForwardingReactiveSqlExecutor decorator = logicalDecorator(executor);
        if (decorator == null) {
            return executor;
        }
        ReactiveSqlExecutor rebuiltDelegate = withoutPolicy0(
                logicalDelegate(executor, decorator), policyType, removedPolicy);
        if (policyType.isInstance(decorator)) {
            removedPolicy.accept(decorator);
            return rebuiltDelegate;
        }
        return preservingScopedCapability(
                rebuiltDelegate, decorator.redecoratePolicy(rebuiltDelegate));
    }

    private static ForwardingReactiveSqlExecutor logicalDecorator(ReactiveSqlExecutor executor) {
        if (executor instanceof ScopedForwardingReactiveSqlExecutor scoped) {
            return scoped.decorator();
        }
        return executor instanceof ForwardingReactiveSqlExecutor forwarding ? forwarding : null;
    }

    private static ReactiveSqlExecutor logicalDelegate(
            ReactiveSqlExecutor executor,
            ForwardingReactiveSqlExecutor decorator) {
        return executor instanceof ScopedForwardingReactiveSqlExecutor scoped
                ? scoped.delegate() : decorator.delegate();
    }

    static ReactiveSqlExecutor preservingScopedCapability(
            ReactiveSqlExecutor originalDelegate,
            ForwardingReactiveSqlExecutor decorator) {
        ReactiveSqlExecutor safeDelegate = Objects.requireNonNull(
                originalDelegate, "reactive SQL delegate must not be null");
        ForwardingReactiveSqlExecutor safeDecorator = Objects.requireNonNull(
                decorator, "reactive SQL decorator must not be null");
        return safeDelegate instanceof ConnectionScopedReactiveSqlExecutor scoped
                ? new ScopedForwardingReactiveSqlExecutor(safeDecorator, safeDelegate, scoped)
                : safeDecorator;
    }

    @Override
    public <T> Mono<T> withConnection(SqlRequest firstRequest,
            Function<ReactiveSqlExecutor, Mono<T>> work) {
        return delegate.withConnection(firstRequest, bound -> work.apply(redecoratePolicy(bound)));
    }

    @Override
    public Flux<DynamicRow> query(SqlRequest request) {
        return delegate.query(request);
    }

    @Override
    public Flux<DynamicRow> query(SqlRequest request, SqlExecutionOptions options) {
        return delegate.query(request, options);
    }

    @Override
    public Mono<Long> rowsUpdated(SqlRequest request) {
        return delegate.rowsUpdated(request);
    }

    @Override
    public Mono<Long> rowsUpdated(SqlRequest request, SqlExecutionOptions options) {
        return delegate.rowsUpdated(request, options);
    }

    @Override
    public Mono<SqlWriteResult> rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
        return delegate.rowsUpdatedReturningKeys(request, options);
    }

    @Override
    public Mono<SqlWriteResult> rowsUpdatedReturningKeys(SqlRequest request,
                                                         SqlExecutionOptions options,
                                                         String generatedKeyColumn) {
        return delegate.rowsUpdatedReturningKeys(request, options, generatedKeyColumn);
    }

    @Override
    public Mono<SqlWriteResult> protectedWrite(ProtectedWriteWork work, SqlExecutionOptions options) {
        return delegate.protectedWrite(work, options);
    }

    @Override
    public Mono<BatchExecutionEvidence> writeBatch(BatchWriteRequest request,
            LongFunction<? extends Publisher<Void>> rowCompleted) {
        return delegate.writeBatch(request, rowCompleted);
    }

    @Override
    public Mono<BatchExecutionEvidence> writeProtectedBatch(BatchWriteRequest request,
            LongFunction<? extends Publisher<Void>> rowCompleted) {
        return delegate.writeProtectedBatch(request, rowCompleted);
    }

    /** 只在底层执行器具备连接级序列能力时保留该能力，普通调用仍经过逻辑策略装饰器。 */
    private static final class ScopedForwardingReactiveSqlExecutor
            extends ForwardingReactiveSqlExecutor
            implements ConnectionScopedReactiveSqlExecutor {

        private final ConnectionScopedReactiveSqlExecutor scopedDelegate;
        private final ReactiveSqlExecutor structuralDelegate;

        private ScopedForwardingReactiveSqlExecutor(
                ReactiveSqlExecutor decorated,
                ReactiveSqlExecutor structuralDelegate,
                ConnectionScopedReactiveSqlExecutor scopedDelegate) {
            super(decorated);
            this.structuralDelegate = Objects.requireNonNull(
                    structuralDelegate, "structural delegate must not be null");
            this.scopedDelegate = Objects.requireNonNull(
                    scopedDelegate, "connection-scoped delegate must not be null");
        }

        @Override
        ReactiveSqlExecutor delegate() {
            return structuralDelegate;
        }

        private ForwardingReactiveSqlExecutor decorator() {
            return (ForwardingReactiveSqlExecutor) super.delegate();
        }

        @Override
        public <T> Mono<T> withConnection(SqlRequest firstRequest,
                Function<ReactiveSqlExecutor, Mono<T>> work) {
            return super.delegate().withConnection(firstRequest, work);
        }

        @Override
        public Mono<SqlExecutionSequenceResult> executeInConnection(
                SqlExecutionSequence sequence,
                SqlExecutionOptions options) {
            return scopedDelegate.executeInConnection(sequence, options);
        }
    }

}
