package com.flying.orm.rdb.reactive;

import static com.flying.orm.core.internal.error.ThrowableGraph.addSuppressedIfAcyclic;

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
 * cleanup 无论正常完成、执行失败还是取消都会被尝试；ORM 只关闭其自有逻辑连接，
 * 不推断连接池的物理连接健康状态。逐条 SQL 的观测仍由统一观测支持类完成。
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
        List<List<Object>> setupParameters = snapshotParameters(safeSequence.setup());
        List<List<Object>> workParameters = snapshotParameters(safeSequence.work());
        List<List<Object>> cleanupParameters = snapshotParameters(safeSequence.cleanup());
        SqlExecutionOptions safeOptions = Objects.requireNonNull(
                options, "sql execution options must not be null");
        return executionSession.resolveTransaction()
                .flatMap(resolution -> resolution.bind(executeResolved(
                        safeSequence, setupParameters, workParameters, cleanupParameters, safeOptions)));
    }

    /** 同普通 R2DBC 入口一样，只补充冻结 core 无法读取的 R2DBC 包装器载荷。 */
    private static List<List<Object>> snapshotParameters(List<SqlRequest> requests) {
        List<List<Object>> snapshot = null;
        for (int index = 0; index < requests.size(); index++) {
            SqlRequest request = requests.get(index);
            List<Object> parameters = R2dbcExecutionSession.snapshotExecutionParameters(request);
            if (snapshot != null) {
                snapshot.add(parameters);
            } else if (parameters != request.parameters()) {
                snapshot = new ArrayList<>(requests.size());
                for (int previous = 0; previous < index; previous++) {
                    snapshot.add(requests.get(previous).parameters());
                }
                snapshot.add(parameters);
            }
        }
        // 空列表表示整段复用请求已拥有的参数；只有不透明包装器需要额外快照列表。
        return snapshot == null ? List.of() : List.copyOf(snapshot);
    }

    private Mono<SqlExecutionSequenceResult> executeResolved(SqlExecutionSequence sequence,
                                                              List<List<Object>> setupParameters,
                                                              List<List<Object>> workParameters,
                                                              List<List<Object>> cleanupParameters,
                                                              SqlExecutionOptions options) {
        return Mono.usingWhen(
                executionSession.acquireConnection().map(SequenceResource::new),
                resource -> R2dbcSqlDeadline.start(options)
                        .bind(executeSequence(resource, sequence,
                                              setupParameters, workParameters, cleanupParameters, options)),
                resource -> executionSession.closeAfterResult(
                        resource.lease(), SqlExecutionOperation.UPDATE, true, resource.cleanupDeadline(options)),
                (resource, error) -> {
                    Throwable cleanupFailure = sequenceCleanupFailure(error);
                    return cleanupFailure == null
                            ? executionSession.closeAfterResult(
                                    resource.lease(), SqlExecutionOperation.UPDATE, false,
                                    resource.cleanupDeadline(options))
                            : executionSession.closeAfterCleanupFailure(
                            resource.lease(),
                            SqlExecutionOperation.UPDATE,
                            ResourceCleanupObservation.Phase.SESSION_CLEANUP,
                            false,
                            cleanupFailure,
                            resource.cleanupDeadline(options));
                },
                resource -> executeSequenceCleanup(
                        resource.lease().connection(), sequence.cleanup(), cleanupParameters, List.of(), options,
                        resource.cleanupDeadline(options))
                        .then(executionSession.closeAfterResult(
                                resource.lease(), SqlExecutionOperation.UPDATE, false,
                                resource.cleanupDeadline(options)))
                        .onErrorResume(cleanupError -> executionSession.closeAfterCleanupFailure(
                                resource.lease(),
                                SqlExecutionOperation.UPDATE,
                                ResourceCleanupObservation.Phase.SESSION_CLEANUP,
                                false,
                                cleanupError,
                                 resource.cleanupDeadline(options))));
    }

    private Mono<SqlExecutionSequenceResult> executeSequence(SequenceResource resource,
                                                              SqlExecutionSequence sequence,
                                                              List<List<Object>> setupParameters,
                                                              List<List<Object>> workParameters,
                                                              List<List<Object>> cleanupParameters,
                                                              SqlExecutionOptions options) {
        Connection connection = resource.lease().connection();
        List<SqlExecutionStepResult> completed = new ArrayList<>();
        Mono<SqlExecutionSequenceResult> work = executeSequencePhase(
                connection, sequence.setup(), setupParameters, SqlExecutionPhase.SETUP, completed, false, options)
                .thenMany(executeSequencePhase(
                        connection, sequence.work(), workParameters, SqlExecutionPhase.WORK, completed, true, options))
                .collectList()
                .map(ignored -> new SqlExecutionSequenceResult(completed));
        Mono<SqlExecutionSequenceResult> protectedWork = executionSession.protectMono(work, options);
        Mono<SqlExecutionSequenceResult> workWithFailureCleanup = protectedWork.onErrorResume(error ->
                executeSequenceCleanup(
                        connection, sequence.cleanup(), cleanupParameters, completed, options, resource.cleanupDeadline(options))
                        .onErrorResume(cleanupError -> {
                            addSuppressedIfAcyclic(error, cleanupError);
                            return Mono.empty();
                        })
                        .then(Mono.error(error)));
        // 成功后的 cleanup 失败直接向上返回，不能落入上面的失败补偿再执行第二次。
        return workWithFailureCleanup.flatMap(result ->
                executeSequenceCleanup(
                        connection, sequence.cleanup(), cleanupParameters, completed, options,
                        resource.cleanupDeadline(options)).thenReturn(result));
    }

    private Mono<Void> executeSequenceCleanup(Connection connection,
                                              List<SqlRequest> cleanup,
                                              List<List<Object>> cleanupParameters,
                                              List<SqlExecutionStepResult> completed,
                                              SqlExecutionOptions options,
                                              R2dbcCleanupDeadline deadline) {
        Mono<Void> execution = executeSequencePhase(
                connection, cleanup, cleanupParameters, SqlExecutionPhase.CLEANUP, completed, false, options).then();
        return deadline.protect(execution);
    }

    private Flux<SqlExecutionStepResult> executeSequencePhase(Connection connection,
                                                              List<SqlRequest> requests,
                                                              List<List<Object>> executionParameters,
                                                              SqlExecutionPhase phase,
                                                              List<SqlExecutionStepResult> completed,
                                                              boolean collectWork,
                                                              SqlExecutionOptions options) {
        return Flux.fromIterable(requests)
                   .index()
                   .concatMap(indexed -> Mono.defer(() -> {
                       int stepIndex = Math.toIntExact(indexed.getT1());
                       SqlRequest request = indexed.getT2();
                       List<Object> parameters = executionParameters.isEmpty()
                               ? request.parameters() : executionParameters.get(stepIndex);
                       long startedAt = System.nanoTime();
                       Mono<Long> execution = executeUpdate(connection, request, parameters);
                       return observationSupport.observeMono(SqlExecutionOperation.UPDATE,
                                                            request,
                                                            parameters,
                                                            0,
                                                             execution,
                                                             rows -> rows,
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

    private Mono<Long> executeUpdate(Connection connection, SqlRequest request, List<Object> parameters) {
        var statement = executionSession.prepareStatement(connection,
                                                         request,
                                                         parameters);
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
