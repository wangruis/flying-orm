package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionSequence;
import com.flying.orm.rdb.execution.SqlExecutionSequenceResult;
import reactor.core.publisher.Mono;

import java.util.Objects;

/** 仅当原 delegate 真正具有 scoped 能力时附加该能力，普通调用仍经过策略装饰器。 */
final class ScopedForwardingReactiveSqlExecutor extends ForwardingReactiveSqlExecutor
        implements ConnectionScopedReactiveSqlExecutor {

    private final ConnectionScopedReactiveSqlExecutor scopedDelegate;

    private final ReactiveSqlExecutor structuralDelegate;

    ScopedForwardingReactiveSqlExecutor(ReactiveSqlExecutor decorated,
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

    ForwardingReactiveSqlExecutor decorator() {
        return (ForwardingReactiveSqlExecutor) super.delegate();
    }

    @Override
    public Mono<SqlExecutionSequenceResult> executeInConnection(
            SqlExecutionSequence sequence,
            SqlExecutionOptions options) {
        return scopedDelegate.executeInConnection(sequence, options);
    }
}
