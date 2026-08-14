package com.flying.orm.rdb.jdbc;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteCompletion;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchWriteResult;
import com.flying.orm.rdb.transaction.JdbcTransactionCompletion;
import com.flying.orm.rdb.transaction.JdbcTransactionContext;
import com.flying.orm.rdb.transaction.TransactionOutcome;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;

import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 验证 JDBC 外部事务完成通知缺失时不会订阅用户提供的 Publisher。 */
class JdbcExternalBatchCompletionTest {

    @Test
    void fallsBackSynchronouslyWithUnknownWithoutSubscribingArbitraryPublisher() {
        CountingCompletion completion = new CountingCompletion();
        BatchWriteRequest request = request(completion);

        BatchWriteResult result = enlist(JdbcTransactionContext.external(connection(), "primary"), request);

        assertEquals(BatchWriteResult.Status.ENLISTED, result.status());
        assertEquals(0, completion.publisherCalls.get());
        assertEquals(0, completion.publisherSubscriptions.get());
        assertEquals(1, completion.synchronousCalls.get());
        assertEquals(BatchWriteResult.Status.UNKNOWN, completion.synchronousResult.get().status());
    }

    @Test
    void keepsPublisherForRegisteredTransactionAdapterWithoutSynchronousFallback() {
        CountingCompletion completion = new CountingCompletion();
        RecordingTransactionCompletion transactionCompletion = new RecordingTransactionCompletion();
        BatchWriteRequest request = request(completion);

        enlist(JdbcTransactionContext.external(connection(), "primary", transactionCompletion), request);

        assertEquals(0, completion.synchronousCalls.get());
        JdbcTransactionCompletion.Listener listener = transactionCompletion.listener.get();
        assertNotNull(listener);
        Publisher<Void> publisher = listener.afterCompletion(TransactionOutcome.COMMITTED);
        assertNotNull(publisher);
        assertSame(completion.publisher, publisher);
        assertEquals(1, completion.publisherCalls.get());
        assertEquals(0, completion.publisherSubscriptions.get());
        assertEquals(0, completion.synchronousCalls.get());
    }

    /** 外部事务注册器用普通异常包装 VME 时，不得静默回退并继续返回 ENLISTED。 */
    @Test
    void propagatesVirtualMachineErrorNestedInCompletionRegistrationFailure() {
        OutOfMemoryError fatal = new OutOfMemoryError("registration nested fatal");
        IllegalStateException wrapper = new IllegalStateException("registration wrapper", fatal);
        CountingCompletion completion = new CountingCompletion();
        JdbcTransactionCompletion transactionCompletion = listener -> { throw wrapper; };

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class, () -> enlist(
                JdbcTransactionContext.external(connection(), "primary", transactionCompletion),
                request(completion)));

        assertSame(fatal, observed);
        assertEquals(0, completion.synchronousCalls.get());
    }

    /** 无注册能力时的同步兜底回调用普通异常包装 VME，也必须传播原致命错误。 */
    @Test
    void propagatesVirtualMachineErrorNestedInUnavailableCompletionFailure() {
        OutOfMemoryError fatal = new OutOfMemoryError("unavailable nested fatal");
        IllegalStateException wrapper = new IllegalStateException("unavailable wrapper", fatal);
        BatchWriteCompletion completion = new BatchWriteCompletion() {
            @Override
            public Publisher<Void> afterCompletion(BatchWriteResult result) {
                return subscriber -> { };
            }

            @Override
            public void afterCompletionUnavailable(BatchWriteResult result) {
                throw wrapper;
            }
        };

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class, () -> enlist(
                JdbcTransactionContext.external(connection(), "primary"), request(completion)));

        assertSame(fatal, observed);
    }

    private static BatchWriteResult enlist(JdbcTransactionContext transaction, BatchWriteRequest request) {
        JdbcBatchExecutionObservationSupport observation = JdbcBatchExecutionObservationSupport.create(null);
        return new JdbcExternalBatchCompletion().enlist(transaction,
                                                         request,
                                                         List.of(BatchChunkResult.committed(0, 0L, 1, 1L)),
                                                         observation.begin(request));
    }

    private static BatchWriteRequest request(BatchWriteCompletion completion) {
        return new BatchWriteRequest("insert into users(id) values(?)",
                                     1,
                                     List.of(String.class),
                                     SqlBindMarkerStyle.CANONICAL,
                                     subscriber -> { },
                                     BatchWriteOptions.atomic(1),
                                     com.flying.orm.rdb.batch.BatchRowCountPolicy.ANY,
                                     completion);
    }

    @SuppressWarnings("unchecked")
    private static Connection connection() {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                                                    new Class<?>[]{Connection.class},
                                                    (proxy, method, arguments) -> null);
    }

    private static final class CountingCompletion implements BatchWriteCompletion {
        private final AtomicInteger publisherCalls = new AtomicInteger();
        private final AtomicInteger publisherSubscriptions = new AtomicInteger();
        private final AtomicInteger synchronousCalls = new AtomicInteger();
        private final AtomicReference<BatchWriteResult> synchronousResult = new AtomicReference<>();
        private final Publisher<Void> publisher = subscriber -> publisherSubscriptions.incrementAndGet();

        @Override
        public Publisher<Void> afterCompletion(BatchWriteResult result) {
            publisherCalls.incrementAndGet();
            return publisher;
        }

        @Override
        public void afterCompletionUnavailable(BatchWriteResult result) {
            synchronousCalls.incrementAndGet();
            synchronousResult.set(result);
        }
    }

    private static final class RecordingTransactionCompletion implements JdbcTransactionCompletion {
        private final AtomicReference<Listener> listener = new AtomicReference<>();

        @Override
        public boolean register(Listener listener) {
            return this.listener.compareAndSet(null, listener);
        }
    }
}
