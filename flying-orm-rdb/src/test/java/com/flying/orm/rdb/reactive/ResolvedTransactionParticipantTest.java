package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipant;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipationException;
import io.r2dbc.spi.Connection;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

/**
 * 验证已绑定的事务上下文在一次订阅内不会被上层适配器再次查询或替换。
 *
 * @author wangr
 * @date 2026-08-09
 * @version v1.0
 */
class ResolvedTransactionParticipantTest {

    /** 已经由执行入口绑定的真实事务必须优先于委托适配器，避免同一订阅二次查询产生连接漂移。 */
    @Test
    void prefersRawBoundTransactionWithoutSubscribingDelegate() {
        R2dbcTransactionContext bound = R2dbcTransactionContext.external(connection(), "tenant-primary");
        AtomicInteger delegateSubscriptions = new AtomicInteger();
        R2dbcTransactionParticipant participant = ResolvedTransactionParticipant.wrap(() -> Mono.defer(() -> {
            delegateSubscriptions.incrementAndGet();
            return Mono.just(R2dbcTransactionContext.external(connection(), "tenant-replica"));
        }));

        StepVerifier.create(participant.currentTransaction()
                                     .contextWrite(context -> R2dbcTransactionParticipant.bind(context, bound)))
                    .expectNext(bound)
                    .verifyComplete();

        assertEquals(0, delegateSubscriptions.get());
    }

    /** 已绑定事务的固定路由仍须走统一校验，不能因缓存命中而允许改库。 */
    @Test
    void rejectsRequestedRouteThatConflictsWithRawBoundTransaction() {
        R2dbcTransactionContext bound = R2dbcTransactionContext.external(connection(), "tenant-primary");
        AtomicInteger delegateSubscriptions = new AtomicInteger();
        R2dbcTransactionParticipant participant = ResolvedTransactionParticipant.wrap(() -> Mono.defer(() -> {
            delegateSubscriptions.incrementAndGet();
            return Mono.empty();
        }));

        StepVerifier.create(participant.currentTransaction("tenant-replica")
                                     .contextWrite(context -> R2dbcTransactionParticipant.bind(context, bound)))
                    .expectErrorSatisfies(error -> {
                        R2dbcTransactionParticipationException rejected = assertInstanceOf(
                                R2dbcTransactionParticipationException.class, error);
                        assertEquals(R2dbcTransactionParticipationException.Reason.ROUTING_IDENTITY_CHANGED,
                                     rejected.reason());
                    })
                    .verify();

        assertEquals(0, delegateSubscriptions.get());
    }

    @SuppressWarnings("unchecked")
    private static Connection connection() {
        return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                                                    new Class<?>[]{Connection.class},
                                                    (proxy, method, arguments) -> defaultValue(method));
    }

    private static Object defaultValue(Method method) {
        if (method.getReturnType() == boolean.class) {
            return false;
        }
        if (method.getReturnType() == int.class) {
            return 0;
        }
        if (method.getReturnType() == long.class) {
            return 0L;
        }
        return null;
    }
}
