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
        return Mono.usingWhen(
                executionSession.acquireConnection(options),
                lease -> executionSession.protectMono(executeSequence(lease.connection(), sequence, options), options),
                lease -> executionSession.closeAfterResult(lease, SqlExecutionOperation.UPDATE, options, true),
                (lease, error) -> {
                    Throwable cleanupFailure = sequenceCleanupFailure(error);
                    return cleanupFailure == null
                            ? executionSession.closeAfterResult(lease, SqlExecutionOperation.UPDATE, options, false)
                            : executionSession.invalidateAfterCleanupFailure(
                            lease,
                            SqlExecutionOperation.UPDATE,
                            ResourceCleanupObservation.Phase.SESSION_CLEANUP,
                            options,
                            false,
                            cleanupFailure);
                },
                lease -> executeSequenceCleanup(lease.connection(), sequence.cleanup(), List.of(), options)
                        .then(executionSession.closeAfterResult(lease, SqlExecutionOperation.UPDATE, options, false))
                        .onErrorResume(cleanupError -> executionSession.invalidateAfterCleanupFailure(
                                lease,
                                SqlExecutionOperation.UPDATE,
                                ResourceCleanupObservation.Phase.SESSION_CLEANUP,
                                options,
                                false,
                                cleanupError)));
    }

    private Mono<SqlExecutionSequenceResult> executeSequence(Connection connection,
                                                             SqlExecutionSequence sequence,
                                                             SqlExecutionOptions options) {
        List<SqlExecutionStepResult> completed = new ArrayList<>();
        Mono<SqlExecutionSequenceResult> work = executeSequencePhase(
                connection, sequence.setup(), SqlExecutionPhase.SETUP, completed, false)
                .thenMany(executeSequencePhase(connection, sequence.work(), SqlExecutionPhase.WORK, completed, true))
                .collectList()
                .map(ignored -> new SqlExecutionSequenceResult(completed));
        Mono<SqlExecutionSequenceResult> workWithFailureCleanup = work.onErrorResume(error ->
                executeSequenceCleanup(connection, sequence.cleanup(), completed, options)
                        .onErrorResume(cleanupError -> {
                            ReactiveSqlExecutionProtection.addSuppressedIfAcyclic(error, cleanupError);
                            return Mono.empty();
                        })
                        .then(Mono.error(error)));
        // 成功后的 cleanup 失败直接向上返回，不能落入上面的失败补偿再执行第二次。
        return workWithFailureCleanup.flatMap(result ->
                executeSequenceCleanup(connection, sequence.cleanup(), completed, options).thenReturn(result));
    }

    private Mono<Void> executeSequenceCleanup(Connection connection,
                                             List<SqlRequest> cleanup,
                                             List<SqlExecutionStepResult> completed,
                                             SqlExecutionOptions options) {
        Mono<Void> execution = executeSequencePhase(
                connection, cleanup, SqlExecutionPhase.CLEANUP, completed, false).then();
        return options.cleanupTimeout().isZero() ? execution : execution.timeout(options.cleanupTimeout());
    }

    private Flux<SqlExecutionStepResult> executeSequencePhase(Connection connection,
                                                              List<SqlRequest> requests,
                                                              SqlExecutionPhase phase,
                                                              List<SqlExecutionStepResult> completed,
                                                              boolean collectWork) {
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
                                                            execution)
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
}
