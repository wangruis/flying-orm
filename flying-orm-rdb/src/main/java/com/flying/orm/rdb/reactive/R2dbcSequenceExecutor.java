package com.flying.orm.rdb.reactive;

import static com.flying.orm.core.internal.error.ThrowableGraph.addSuppressedIfAcyclic;
import static com.flying.orm.core.internal.error.ThrowableGraph.findVirtualMachineError;

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
import java.util.concurrent.CancellationException;
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
                                              setupParameters, workParameters, options)),
                resource -> finishSequence(resource, sequence.cleanup(), cleanupParameters, options, null),
                (resource, error) -> finishSequence(
                        resource, sequence.cleanup(), cleanupParameters, options, error),
                resource -> finishSequence(resource, sequence.cleanup(), cleanupParameters, options,
                        new CancellationException("connection-scoped SQL was cancelled")))
                .onErrorMap(R2dbcSequenceExecutor::cleanupFailureCause);
    }

    private Mono<SqlExecutionSequenceResult> executeSequence(SequenceResource resource,
                                                              SqlExecutionSequence sequence,
                                                              List<List<Object>> setupParameters,
                                                              List<List<Object>> workParameters,
                                                              SqlExecutionOptions options) {
        Connection connection = resource.lease().connection();
        List<SqlExecutionStepResult> completed = resource.completed();
        Mono<SqlExecutionSequenceResult> work = executeSequencePhase(
                connection, sequence.setup(), setupParameters, SqlExecutionPhase.SETUP, completed, false, options)
                .thenMany(executeSequencePhase(
                        connection, sequence.work(), workParameters, SqlExecutionPhase.WORK, completed, true, options))
                .collectList()
                .map(ignored -> new SqlExecutionSequenceResult(completed));
        return executionSession.protectMono(work, options);
    }

    /** 清理只归 usingWhen 的单个终态回调所有，取消不能重启已经开始的清理序列。 */
    private Mono<Void> finishSequence(SequenceResource resource, List<SqlRequest> cleanup,
                                      List<List<Object>> cleanupParameters, SqlExecutionOptions options,
                                      Throwable primary) {
        R2dbcCleanupDeadline deadline = resource.cleanupDeadline(options);
        return executeSequenceCleanup(resource.lease().connection(), cleanup, cleanupParameters,
                resource.completed(), options, deadline).materialize().flatMap(signal -> {
                    if (!signal.hasError()) {
                        return executionSession.closeAfterResult(
                                resource.lease(), SqlExecutionOperation.UPDATE, primary == null, deadline);
                    }
                    Throwable failure = Objects.requireNonNull(signal.getThrowable());
                    if (primary != null) {
                        addSuppressedIfAcyclic(primary, failure);
                    }
                    return executionSession.closeAfterCleanupFailure(
                            resource.lease(), SqlExecutionOperation.UPDATE,
                            ResourceCleanupObservation.Phase.SESSION_CLEANUP, false, failure, deadline)
                            .then(primary == null ? Mono.error(failure) : Mono.empty());
                });
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

    /** usingWhen 包装成功后的清理错误；恢复原有清理阶段异常，不拆业务失败。 */
    private static Throwable cleanupFailureCause(Throwable error) {
        if (error.getClass() == RuntimeException.class && findVirtualMachineError(error) == null) {
            Throwable cause = error.getCause();
            if (cause instanceof java.util.concurrent.TimeoutException
                    || cause instanceof SqlExecutionSequenceException sequenceError
                            && sequenceError.phase() == SqlExecutionPhase.CLEANUP) {
                return cause;
            }
        }
        return error;
    }

    /** 单次订阅内延迟创建清理截止时间，业务执行时间不会提前消耗清理预算。 */
    private record SequenceResource(R2dbcExecutionSession.ConnectionLease lease,
                                    AtomicReference<R2dbcCleanupDeadline> cleanupDeadline,
                                    List<SqlExecutionStepResult> completed) {

        private SequenceResource(R2dbcExecutionSession.ConnectionLease lease) {
            this(Objects.requireNonNull(lease, "connection lease must not be null"),
                    new AtomicReference<>(), new ArrayList<>());
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
