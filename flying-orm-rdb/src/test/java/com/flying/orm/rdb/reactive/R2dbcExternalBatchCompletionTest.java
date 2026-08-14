package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.observation.BatchExecutionObservation;
import com.flying.orm.rdb.observation.SqlExecutionResultKind;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import com.flying.orm.rdb.transaction.R2dbcTransactionCompletion;
import com.flying.orm.rdb.transaction.TransactionOutcome;
import io.r2dbc.spi.Connection;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscription;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 验证外部事务结束后，ENLISTED 能得到准确且只发送一次的最终批量观测。 */
class R2dbcExternalBatchCompletionTest {

    @Test
    void observesExternalRollbackWithoutReportingCommittedRows() {
        BatchExecutionObservation observation = complete(TransactionOutcome.ROLLED_BACK);

        assertEquals(BatchWriteResult.Status.ROLLED_BACK, observation.summaryStatus());
        assertEquals(0, observation.affectedRows());
        assertEquals(SqlExecutionResultKind.ROLLED_BACK, observation.resultKind());
    }

    @Test
    void observesUnknownExternalOutcomeWithoutGuessingCommit() {
        BatchExecutionObservation observation = complete(TransactionOutcome.UNKNOWN);

        assertEquals(BatchWriteResult.Status.UNKNOWN, observation.summaryStatus());
        assertEquals(0, observation.affectedRows());
        assertEquals(SqlExecutionResultKind.UNKNOWN, observation.resultKind());
    }

    /** 未提供事务完成通知时，回调的 JVM fatal 不能被降级为正常 ENLISTED 结果。 */
    @Test
    void propagatesFatalFromUnavailableCompletionCallback() {
        OutOfMemoryError fatal = new OutOfMemoryError("completion fatal");
        R2dbcExternalBatchCompletion support = new R2dbcExternalBatchCompletion(
                new R2dbcBatchResultAssembler(), ignored -> {
                });
        R2dbcBatchConnectionHandle handle = new R2dbcBatchConnectionHandle(
                R2dbcTransactionContext.external(connection(), "primary"));
        BatchWriteRequest request = new BatchWriteRequest(
                "insert into users(id) values(?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.empty(),
                BatchWriteOptions.atomic(2),
                com.flying.orm.rdb.batch.BatchRowCountPolicy.ANY,
                com.flying.orm.rdb.batch.BatchGeneratedKeys.none(),
                ignored -> rawError(fatal));

        assertSame(fatal, assertThrows(OutOfMemoryError.class, () -> support.enlist(
                handle, request, List.of(BatchChunkResult.committed(0, 0, 2, 2))).block()));
    }

    /** 外部事务注册器用普通异常包装 VME 时，不得回退为正常 ENLISTED。 */
    @Test
    void propagatesVirtualMachineErrorNestedInCompletionRegistrationFailure() {
        OutOfMemoryError fatal = new OutOfMemoryError("registration nested fatal");
        IllegalStateException wrapper = new IllegalStateException("registration wrapper", fatal);
        R2dbcExternalBatchCompletion support = new R2dbcExternalBatchCompletion(
                new R2dbcBatchResultAssembler(), ignored -> { });
        R2dbcBatchConnectionHandle handle = new R2dbcBatchConnectionHandle(
                R2dbcTransactionContext.external(connection(), "primary", listener -> { throw wrapper; }));
        BatchWriteRequest request = new BatchWriteRequest(
                "insert into users(id) values(?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.empty(),
                BatchWriteOptions.atomic(1));

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class, () -> support.enlist(
                handle, request, List.of(BatchChunkResult.committed(0, 0, 1, 1))).block());

        assertSame(fatal, observed);
    }

    private static BatchExecutionObservation complete(TransactionOutcome outcome) {
        RecordingCompletion completion = new RecordingCompletion();
        List<BatchExecutionObservation> observations = new ArrayList<>();
        R2dbcExternalBatchCompletion support = new R2dbcExternalBatchCompletion(
                new R2dbcBatchResultAssembler(), observations::add);
        R2dbcBatchConnectionHandle handle = new R2dbcBatchConnectionHandle(
                R2dbcTransactionContext.external(connection(), "primary", completion));
        BatchWriteRequest request = new BatchWriteRequest(
                "insert into users(id) values(?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.empty(),
                BatchWriteOptions.atomic(2));

        StepVerifier.create(support.enlist(
                            handle, request, List.of(BatchChunkResult.committed(0, 0, 2, 2))))
                    .assertNext(enlisted -> {
                        assertEquals(BatchWriteResult.Status.ENLISTED, enlisted.status());
                        assertEquals(0, enlisted.affectedRows());
                    })
                    .verifyComplete();

        completion.complete(outcome);
        completion.complete(outcome);
        assertEquals(1, observations.size(), "重复完成通知不能产生重复最终观测");
        return observations.getFirst();
    }

    private static final class RecordingCompletion implements R2dbcTransactionCompletion {
        private final AtomicReference<Listener> listener = new AtomicReference<>();

        @Override
        public boolean register(Listener listener) {
            return this.listener.compareAndSet(null, listener);
        }

        private void complete(TransactionOutcome outcome) {
            StepVerifier.create(Mono.from(listener.get().afterCompletion(outcome))).verifyComplete();
        }
    }

    @SuppressWarnings("unchecked")
    private static Connection connection() {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                                                    new Class<?>[]{Connection.class},
                                                    (proxy, method, args) -> defaultValue(method));
    }

    private static Object defaultValue(Method method) {
        if (Publisher.class.isAssignableFrom(method.getReturnType())) {
            return Mono.empty();
        }
        if (method.getReturnType() == boolean.class) {
            return false;
        }
        return null;
    }

    /** 直接用 onError 发出 fatal，避免 Reactor 的快捷路径绕过本类的异常恢复分支。 */
    private static <T> Publisher<T> rawError(Throwable error) {
        return subscriber -> subscriber.onSubscribe(new Subscription() {
            private boolean terminated;

            @Override
            public void request(long demand) {
                if (!terminated) {
                    terminated = true;
                    subscriber.onError(error);
                }
            }

            @Override
            public void cancel() {
                terminated = true;
            }
        });
    }
}
