package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.exception.RdbExceptionTranslator;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionPhase;
import com.flying.orm.rdb.execution.SqlExecutionSequence;
import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
import com.flying.orm.rdb.execution.SqlExecutionSequenceResult;
import com.flying.orm.rdb.execution.SqlExecutionStepResult;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import io.r2dbc.spi.Connection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 在同一条连接上依次执行 setup、work 和 cleanup。
 * cleanup 无论正常完成、执行失败还是取消都会被尝试；如果 cleanup 自身失败，连接会进入失效流程，
 * 不会作为健康连接重新放回连接池。逐条 SQL 的观测仍由统一观测支持类完成。
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class R2dbcSequenceExecutor {

    private final R2dbcExecutionSession executionSession;

    private final ReactiveSqlExecutionObservationSupport observationSupport;

    R2dbcSequenceExecutor(R2dbcExecutionSession executionSession,
                          ReactiveSqlExecutionObservationSupport observationSupport) {
        this.executionSession = Objects.requireNonNull(executionSession,
                                                       "R2DBC execution session must not be null");
        this.observationSupport = Objects.requireNonNull(observationSupport,
                                                        "observation support must not be null");
    }

    Mono<SqlExecutionSequenceResult> execute(SqlExecutionSequence sequence, SqlExecutionOptions options) {
        SqlExecutionSequence safeSequence = Objects.requireNonNull(
                sequence, "SQL execution sequence must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(
                options, "sql execution options must not be null");
        return executionSession.resolveTransaction()
                .flatMap(resolution -> resolution.bind(executeResolved(safeSequence, safeOptions)));
    }

    private Mono<SqlExecutionSequenceResult> executeResolved(SqlExecutionSequence sequence,
                                                              SqlExecutionOptions options) {
        return Mono.usingWhen(
                executionSession.acquireConnection().map(SequenceResource::new),
                resource -> R2dbcSqlDeadline.start(options)
                        .bind(executeSequence(resource, sequence, options)),
                resource -> executionSession.closeAfterResult(
                        resource.lease(), SqlExecutionOperation.UPDATE, options, true, resource.cleanupDeadline(options)),
                (resource, error) -> {
                    Throwable cleanupFailure = sequenceCleanupFailure(error);
                    return cleanupFailure == null
                            ? executionSession.closeAfterResult(
                                    resource.lease(), SqlExecutionOperation.UPDATE, options, false,
                                    resource.cleanupDeadline(options))
                            : executionSession.invalidateAfterCleanupFailure(
                            resource.lease(),
                            SqlExecutionOperation.UPDATE,
                            ResourceCleanupObservation.Phase.SESSION_CLEANUP,
                            options,
                            false,
                            cleanupFailure,
                            resource.cleanupDeadline(options));
                },
                resource -> executeSequenceCleanup(
                        resource.lease().connection(), sequence.cleanup(), List.of(), options,
                        resource.cleanupDeadline(options))
                        .then(executionSession.closeAfterResult(
                                resource.lease(), SqlExecutionOperation.UPDATE, options, false,
                                resource.cleanupDeadline(options)))
                        .onErrorResume(cleanupError -> executionSession.invalidateAfterCleanupFailure(
                                resource.lease(),
                                SqlExecutionOperation.UPDATE,
                                ResourceCleanupObservation.Phase.SESSION_CLEANUP,
                                options,
                                false,
                                cleanupError,
                                 resource.cleanupDeadline(options))));
    }

    private Mono<SqlExecutionSequenceResult> executeSequence(SequenceResource resource,
                                                              SqlExecutionSequence sequence,
                                                              SqlExecutionOptions options) {
        Connection connection = resource.lease().connection();
        List<SqlExecutionStepResult> completed = new ArrayList<>();
        Mono<SqlExecutionSequenceResult> work = executeSequencePhase(
                connection, sequence.setup(), SqlExecutionPhase.SETUP, completed, false, options)
                .thenMany(executeSequencePhase(
                        connection, sequence.work(), SqlExecutionPhase.WORK, completed, true, options))
                .collectList()
                .map(ignored -> new SqlExecutionSequenceResult(completed));
        Mono<SqlExecutionSequenceResult> protectedWork = executionSession.protectMono(work, options);
        Mono<SqlExecutionSequenceResult> workWithFailureCleanup = protectedWork.onErrorResume(error ->
                executeSequenceCleanup(
                        connection, sequence.cleanup(), completed, options, resource.cleanupDeadline(options))
                        .onErrorResume(cleanupError -> {
                            ReactiveSqlExecutionProtection.addSuppressedIfAcyclic(error, cleanupError);
                            return Mono.empty();
                        })
                        .then(Mono.error(error)));
        // 成功后的 cleanup 失败直接向上返回，不能落入上面的失败补偿再执行第二次。
        return workWithFailureCleanup.flatMap(result ->
                executeSequenceCleanup(
                        connection, sequence.cleanup(), completed, options,
                        resource.cleanupDeadline(options)).thenReturn(result));
    }

    private Mono<Void> executeSequenceCleanup(Connection connection,
                                              List<SqlRequest> cleanup,
                                              List<SqlExecutionStepResult> completed,
                                              SqlExecutionOptions options,
                                              R2dbcCleanupDeadline deadline) {
        Mono<Void> execution = executeSequencePhase(
                connection, cleanup, SqlExecutionPhase.CLEANUP, completed, false, options).then();
        return deadline.protect(execution);
    }

    private Flux<SqlExecutionStepResult> executeSequencePhase(Connection connection,
                                                              List<SqlRequest> requests,
                                                              SqlExecutionPhase phase,
                                                              List<SqlExecutionStepResult> completed,
                                                              boolean collectWork,
                                                              SqlExecutionOptions options) {
        return Flux.fromIterable(requests)
                   .index()
                   .concatMap(indexed -> Mono.defer(() -> {
                       int stepIndex = Math.toIntExact(indexed.getT1());
                       SqlRequest request = indexed.getT2();
                       long startedAt = System.nanoTime();
                       Mono<Long> execution = executeUpdate(connection, request);
                       return observationSupport.observeMono(SqlExecutionOperation.UPDATE,
                                                            request.sql(),
                                                            request.parameters().size(),
                                                            0,
                                                             request.parameters(),
                                                             execution,
                                                             options)
                                             .map(rows -> new SqlExecutionStepResult(
                                                     stepIndex, request, rows, System.nanoTime() - startedAt))
                                             .doOnNext(result -> {
                                                 if (collectWork) {
                                                     completed.add(result);
                                                 }
                                             })
                                             .onErrorMap(error -> new SqlExecutionSequenceException(
                                                     phase, stepIndex, completed, RdbExceptionTranslator.translate(error)));
                   }));
    }

    private Mono<Long> executeUpdate(Connection connection, SqlRequest request) {
        var statement = executionSession.prepareStatement(connection,
                                                         request.sql(),
                                                         request.parameters().size(),
                                                         request.bindMarkerStyle(),
                                                         request.parameters());
        return Flux.from(statement.execute())
                   .flatMap(result -> result.getRowsUpdated())
                   .reduce(0L, R2dbcExecutionCounts::add);
    }

    private static Throwable sequenceCleanupFailure(Throwable error) {
        if (error instanceof java.util.concurrent.TimeoutException) {
            return error;
        }
        if (error instanceof SqlExecutionSequenceException sequenceError
                && sequenceError.phase() == SqlExecutionPhase.CLEANUP) {
            return sequenceError;
        }
        for (Throwable suppressed : error.getSuppressed()) {
            if (suppressed instanceof java.util.concurrent.TimeoutException) {
                return suppressed;
            }
            if (suppressed instanceof SqlExecutionSequenceException sequenceError
                    && sequenceError.phase() == SqlExecutionPhase.CLEANUP) {
                return sequenceError;
            }
        }
        return null;
    }

    /** 单次订阅内延迟创建清理截止时间，业务执行时间不会提前消耗清理预算。 */
    private record SequenceResource(R2dbcExecutionSession.ConnectionLease lease,
                                    AtomicReference<R2dbcCleanupDeadline> cleanupDeadline) {

        private SequenceResource(R2dbcExecutionSession.ConnectionLease lease) {
            this(Objects.requireNonNull(lease, "connection lease must not be null"), new AtomicReference<>());
        }

        private R2dbcCleanupDeadline cleanupDeadline(SqlExecutionOptions options) {
            R2dbcCleanupDeadline current = cleanupDeadline.get();
            if (current != null) {
                return current;
            }
            R2dbcCleanupDeadline created = R2dbcCleanupDeadline.start(options.cleanupTimeout());
            return cleanupDeadline.compareAndSet(null, created) ? created : cleanupDeadline.get();
        }
    }
}
