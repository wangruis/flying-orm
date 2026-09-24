package com.flying.orm.rdb.reactive;

import static com.flying.orm.core.internal.error.ThrowableGraph.findVirtualMachineError;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.exception.RdbExceptionTranslator;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionPhase;
import com.flying.orm.rdb.execution.SqlExecutionSequence;
import com.flying.orm.rdb.execution.SqlExecutionSequenceException;
import com.flying.orm.rdb.execution.SqlExecutionSequenceResult;
import com.flying.orm.rdb.execution.SqlExecutionStepResult;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import io.r2dbc.spi.Connection;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CancellationException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;

/**
 * 在同一条连接上依次执行 setup、work 和 cleanup。
 * cleanup 无论正常完成、执行失败还是取消都会被尝试；连接始终交给上层 release 端口释放，
 * ORM 不直接关闭连接或推断物理连接健康状态。逐条 SQL 的观测仍由统一观测支持类完成。
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
        return executeResolved(safeSequence, setupParameters, workParameters, cleanupParameters, safeOptions);
    }

    /** 每个请求只经过一次既有快照边界；三个阶段均保存与步骤位置对齐的执行值。 */
    private static List<List<Object>> snapshotParameters(List<SqlRequest> requests) {
        return requests.isEmpty() ? List.of()
                : requests.stream().map(R2dbcExecutionSession::snapshotExecutionParameters).toList();
    }

    private Mono<SqlExecutionSequenceResult> executeResolved(SqlExecutionSequence sequence,
                                                              List<List<Object>> setupParameters,
                                                              List<List<Object>> workParameters,
                                                              List<List<Object>> cleanupParameters,
                                                              SqlExecutionOptions options) {
        List<SqlRequest> firstPhase = sequence.setup();
        if (firstPhase.isEmpty()) {
            firstPhase = sequence.work();
        }
        if (firstPhase.isEmpty()) {
            firstPhase = sequence.cleanup();
        }
        if (firstPhase.isEmpty()) {
            return Mono.just(new SqlExecutionSequenceResult(List.of()));
        }
        return Mono.usingWhen(
                executionSession.acquireConnection(firstPhase.getFirst()).map(SequenceResource::new),
                resource -> executeSequence(resource, sequence,
                                            setupParameters, workParameters, options),
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
                .then(Mono.fromSupplier(() -> new SqlExecutionSequenceResult(resource.completed)));
        return work.onErrorMap(resource::failure);
    }

    /** 清理只归 usingWhen 的单个终态回调所有，取消不能重启已经开始的清理序列。 */
    private Mono<Void> finishSequence(SequenceResource resource, List<SqlRequest> cleanup,
                                      List<List<Object>> cleanupParameters, SqlExecutionOptions options,
                                      Throwable primary) {
        reactor.core.publisher.SignalType terminal = primary instanceof CancellationException
                ? reactor.core.publisher.SignalType.CANCEL
                : primary == null ? reactor.core.publisher.SignalType.ON_COMPLETE
                : reactor.core.publisher.SignalType.ON_ERROR;
        return executeSequenceCleanup(resource, cleanup, cleanupParameters, options)
                .materialize().flatMap(signal -> {
                    Throwable cleanupError = signal.getThrowable();
                    Throwable error = cleanupError == null ? primary
                            : R2dbcExecutionSession.merge(primary, cleanupError);
                    return executionSession.release(resource.resources, SqlExecutionOperation.UPDATE,
                            terminal, error).then(error != null && error != primary
                                    ? Mono.<Void>error(error).hide() : Mono.empty());
                });
    }

    private Mono<Void> executeSequenceCleanup(SequenceResource resource,
                                              List<SqlRequest> cleanup,
                                              List<List<Object>> cleanupParameters,
                                              SqlExecutionOptions options) {
        resource.startStep(SqlExecutionPhase.CLEANUP, 0);
        Mono<Void> execution = executeSequencePhase(
                resource, cleanup, cleanupParameters, SqlExecutionPhase.CLEANUP, options).then();
        return execution.onErrorMap(resource::failure);
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
                       List<Object> parameters = executionParameters.get(stepIndex);
                       long startedAt = System.nanoTime();
                       AtomicLong confirmedRows = observationSupport.enabled() ? new AtomicLong() : null;
                       // 驱动准备和 execute() 的同步异常也属于当前步骤，必须进入同一观测和失败证据链。
                       Mono<Long> execution = Mono.defer(() -> executeUpdate(
                               resource.resources.connection(), request, parameters,
                               confirmedRows == null ? null : confirmedRows::set));
                       return observationSupport.observeMono(SqlExecutionOperation.UPDATE,
                                                            request,
                                                            parameters,
                                                            0,
                                                             execution,
                                                             rows -> rows,
                                                             confirmedRows == null ? () -> 0L : confirmedRows::get,
                                                             options)
                                             .map(rows -> new SqlExecutionStepResult(
                                                     stepIndex, request, rows, System.nanoTime() - startedAt))
                                             .doOnNext(result -> resource.completeStep(phase, result));
                   });
    }

    private Mono<Long> executeUpdate(Connection connection, SqlRequest request, List<Object> parameters,
                                     LongConsumer confirmedRows) {
        var statement = executionSession.prepareStatement(connection,
                                                         request,
                                                         parameters);
        Flux<Long> counts = Flux.from(statement.execute())
                   .flatMap(result -> {
                       try {
                           // Flux.from 和 flatMap 都可能调用标量 Publisher；错误必须进入清理信号链。
                           return Flux.from(result.getRowsUpdated()).hide();
                       } catch (VirtualMachineError fatal) {
                           return Mono.<Long>error(fatal).hide();
                       }
                   });
        return R2dbcExecutionCounts.sum(counts, confirmedRows);
    }

    /** 只识别 usingWhen 清理包装和既有清理阶段异常，不拆普通业务失败。 */
    private static Throwable cleanupFailureCause(Throwable error) {
        Throwable cleanup = R2dbcExecutionSession.unwrapCleanupFailure(error);
        VirtualMachineError fatal = findVirtualMachineError(cleanup);
        if (fatal != null) return fatal;
        if (error.getClass() == RuntimeException.class) {
            Throwable cause = error.getCause();
            if (cause instanceof java.util.concurrent.TimeoutException
                    || cause instanceof SqlExecutionSequenceException sequenceError
                            && sequenceError.phase() == SqlExecutionPhase.CLEANUP) {
                return cause;
            }
        }
        return error;
    }

    /** 单次订阅唯一的资源和执行进度；保留已确认步骤。 */
    private static final class SequenceResource {

        private final R2dbcExecutionSession.Resources resources;
        private final List<SqlExecutionStepResult> completed = new ArrayList<>();
        private SqlExecutionPhase phase = SqlExecutionPhase.SETUP;
        private int stepIndex;

        private SequenceResource(R2dbcExecutionSession.Resources resources) {
            this.resources = Objects.requireNonNull(resources, "connection resources must not be null");
        }

        private synchronized void startStep(SqlExecutionPhase currentPhase, int currentIndex) {
            phase = currentPhase;
            stepIndex = currentIndex;
        }

        private synchronized void completeStep(SqlExecutionPhase completedPhase, SqlExecutionStepResult result) {
            if (completedPhase == SqlExecutionPhase.WORK) {
                completed.add(result);
            }
            // 成功证据与下一位置一起发布，不把刚完成的 SQL 再记成失败。
            stepIndex = result.stepIndex() + 1;
        }

        private synchronized Throwable failure(Throwable error) {
            // 只在错误时复制证据；与步骤记录互斥，避免异步信号读取正在追加的 ArrayList。
            VirtualMachineError fatal = findVirtualMachineError(error);
            return fatal == null ? new SqlExecutionSequenceException(
                    phase, stepIndex, completed, RdbExceptionTranslator.translate(error)) : fatal;
        }

    }
}
