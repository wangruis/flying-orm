package com.flying.orm.rdb.transaction;

import io.r2dbc.spi.Connection;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 验证框架无关事务上下文只传播外部已绑定连接，并把物理路由身份固定在同一个不可变对象里。
 *
 * @author wangr
 * @date 2026-08-07
 * @version v1.0
 */
class R2dbcTransactionParticipantTest {

    /** 路由身份进入事务上下文时会去掉无意义空白，后续订阅只能读取这个固定值。 */
    @Test
    void keepsExternalConnectionAndNormalizedRoutingIdentity() {
        Connection connection = connection();

        R2dbcTransactionContext transaction = R2dbcTransactionContext.external(connection, "  primary  ");

        assertSame(connection, transaction.connection());
        assertEquals("primary", transaction.routingIdentity());
        assertEquals(R2dbcTransactionContext.ConnectionOwnership.EXTERNAL, transaction.ownership());
        assertEquals(R2dbcTransactionContext.TransactionOrigin.EXTERNAL_TRANSACTION, transaction.origin());
        assertFalse(transaction.completion().register(ignored -> Mono.empty()));
    }

    /** 完整适配器提供的完成注册器必须原样进入事务上下文，不能被默认实现覆盖。 */
    @Test
    void keepsProvidedTransactionCompletion() {
        R2dbcTransactionCompletion completion = ignored -> true;

        R2dbcTransactionContext transaction = R2dbcTransactionContext.external(
                connection(), "primary", completion);

        assertSame(completion, transaction.completion());
    }

    /** 没有物理路由身份就无法证明事务期间没有切库，因此在执行 SQL 前直接拒绝。 */
    @Test
    void rejectsBlankRoutingIdentity() {
        Connection connection = connection();

        assertThrows(IllegalArgumentException.class,
                     () -> R2dbcTransactionContext.external(connection, "   "));
    }

    /** 事务期间显式选择另一个数据库时必须在执行器拿到连接前给出稳定错误原因。 */
    @Test
    void rejectsRoutingIdentityChangedInsideTransaction() {
        R2dbcTransactionParticipant participant = () -> Mono.just(
                R2dbcTransactionContext.external(connection(), "tenant-db-a"));

        StepVerifier.create(participant.currentTransaction("tenant-db-b"))
                    .expectErrorSatisfies(error -> {
                        R2dbcTransactionParticipationException rejected = assertInstanceOf(
                                R2dbcTransactionParticipationException.class, error);
                        assertEquals(R2dbcTransactionParticipationException.Reason.ROUTING_IDENTITY_CHANGED,
                                     rejected.reason());
                    })
                    .verify();
    }

    /** Reactor 适配只在订阅上下文中读取事务，不使用线程变量，也不会把事务泄漏给无关订阅。 */
    @Test
    void readsOnlyTransactionBoundToCurrentReactorContext() {
        R2dbcTransactionContext transaction = R2dbcTransactionContext.external(connection(), "primary");
        R2dbcTransactionParticipant participant = R2dbcTransactionParticipant.reactorContext();

        StepVerifier.create(participant.currentTransaction()).verifyComplete();
        StepVerifier.create(participant.currentTransaction()
                                       .contextWrite(context -> R2dbcTransactionParticipant.bind(
                                               context, transaction)))
                    .expectNext(transaction)
                    .verifyComplete();
        assertSame(transaction,
                   R2dbcTransactionParticipant.bind(Context.empty(), transaction)
                                              .get(R2dbcTransactionContext.class));
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
        if (method.getReturnType() == int.class) {
            return 0;
        }
        if (method.getReturnType() == long.class) {
            return 0L;
        }
        return null;
    }
}
