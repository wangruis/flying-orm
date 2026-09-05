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
        Mono<SqlExecutionSequenceResult> work = executeSequencePhase(
                resource, sequence.setup(), setupParameters, SqlExecutionPhase.SETUP, options)
                .thenMany(executeSequencePhase(
                        resource, sequence.work(), workParameters, SqlExecutionPhase.WORK, options))
                .collectList()
                .map(ignored -> new SqlExecutionSequenceResult(resource.completed));
        // 截止异常也属于当前步骤；证据转换必须在唯一执行时限之外。
        return executionSession.protectMono(work, options).onErrorMap(resource::failure);
    }

    /** 清理只归 usingWhen 的单个终态回调所有，取消不能重启已经开始的清理序列。 */
    private Mono<Void> finishSequence(SequenceResource resource, List<SqlRequest> cleanup,
                                      List<List<Object>> cleanupParameters, SqlExecutionOptions options,
                                      Throwable primary) {
        R2dbcCleanupDeadline deadline = resource.cleanupDeadline(options);
        return executeSequenceCleanup(resource, cleanup, cleanupParameters,
                options, deadline).materialize().flatMap(signal -> {
                    if (!signal.hasError()) {
                        return executionSession.closeAfterResult(
                                resource.lease, SqlExecutionOperation.UPDATE, primary == null, deadline);
                    }
                    Throwable failure = Objects.requireNonNull(signal.getThrowable());
                    if (primary != null) {
                        addSuppressedIfAcyclic(primary, failure);
                    }
                    return executionSession.closeAfterCleanupFailure(
                            resource.lease, SqlExecutionOperation.UPDATE,
                            ResourceCleanupObservation.Phase.SESSION_CLEANUP, false, failure, deadline)
                            .then(primary == null ? Mono.error(failure) : Mono.empty());
                });
    }

    private Mono<Void> executeSequenceCleanup(SequenceResource resource,
                                              List<SqlRequest> cleanup,
                                              List<List<Object>> cleanupParameters,
                                              SqlExecutionOptions options,
                                              R2dbcCleanupDeadline deadline) {
        resource.startStep(SqlExecutionPhase.CLEANUP, 0);
        Mono<Void> execution = executeSequencePhase(
                resource, cleanup, cleanupParameters, SqlExecutionPhase.CLEANUP, options).then();
        return deadline.protect(execution).onErrorMap(resource::failure);
    }

    private Flux<SqlExecutionStepResult> executeSequencePhase(SequenceResource resource,
                                                              List<SqlRequest> requests,
                                                              List<List<Object>> executionParameters,
                                                              SqlExecutionPhase phase,
                                                              SqlExecutionOptions options) {
        return Flux.fromIterable(requests)
                   .index()
                   .concatMap(indexed -> {
                       int stepIndex = Math.toIntExact(indexed.getT1());
                       resource.startStep(phase, stepIndex);
                       SqlRequest request = indexed.getT2();
                       List<Object> parameters = executionParameters.isEmpty()
                               ? request.parameters() : executionParameters.get(stepIndex);
                       long startedAt = System.nanoTime();
                       // 驱动准备和 execute() 的同步异常也属于当前步骤，必须进入同一观测和失败证据链。
                       Mono<Long> execution = Mono.defer(() -> executeUpdate(
                               resource.lease.connection(), request, parameters));
                       return observationSupport.observeMono(SqlExecutionOperation.UPDATE,
                                                            request,
                                                            parameters,
                                                            0,
                                                             execution,
                                                             rows -> rows,
                                                             options)
                                             .map(rows -> new SqlExecutionStepResult(
                                                     stepIndex, request, rows, System.nanoTime() - startedAt))
                                             .doOnNext(result -> resource.completeStep(phase, result));
                   });
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

    /** 单次订阅唯一的资源和执行进度；截止线程只读取同一份已确认步骤。 */
    private static final class SequenceResource {

        private final R2dbcExecutionSession.ConnectionLease lease;
        private final AtomicReference<R2dbcCleanupDeadline> cleanupDeadline = new AtomicReference<>();
        private final List<SqlExecutionStepResult> completed = new ArrayList<>();
        private SqlExecutionPhase phase = SqlExecutionPhase.SETUP;
        private int stepIndex;

        private SequenceResource(R2dbcExecutionSession.ConnectionLease lease) {
            this.lease = Objects.requireNonNull(lease, "connection lease must not be null");
        }

        private synchronized void startStep(SqlExecutionPhase currentPhase, int currentIndex) {
            phase = currentPhase;
            stepIndex = currentIndex;
        }

        private synchronized void completeStep(SqlExecutionPhase completedPhase, SqlExecutionStepResult result) {
            if (completedPhase == SqlExecutionPhase.WORK) {
                completed.add(result);
            }
            // 成功证据与下一位置一起发布，截止不能把刚完成的 SQL 再记成失败。
            stepIndex = result.stepIndex() + 1;
        }

        private synchronized Throwable failure(Throwable error) {
            // 只在错误时复制证据；与步骤记录互斥，避免计时线程读取正在追加的 ArrayList。
            VirtualMachineError fatal = findVirtualMachineError(error);
            return fatal == null ? new SqlExecutionSequenceException(
                    phase, stepIndex, completed, RdbExceptionTranslator.translate(error)) : fatal;
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
