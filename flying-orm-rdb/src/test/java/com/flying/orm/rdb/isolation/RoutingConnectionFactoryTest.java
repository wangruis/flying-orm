package com.flying.orm.rdb.isolation;

import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.lang.reflect.Proxy;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * 只验证路由、上下文隔离和连接归还清理，不连接真实数据库。
 *
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
class RoutingConnectionFactoryTest {

    @Test
    void canonicalConstructorRejectsSearchPathLists() {
        String unsafeSchema = "tenant_7, public -- must-not-leak";
        IllegalArgumentException schemaError = assertThrows(IllegalArgumentException.class,
                () -> new IsolationContext("tenant-7", null, unsafeSchema, Map.of()));
        assertFalse(schemaError.getMessage().contains(unsafeSchema));

        String unsafeSetting = "app.tenant-id--must-not-leak";
        IllegalArgumentException settingError = assertThrows(IllegalArgumentException.class,
                () -> new IsolationContext("tenant-7", null, null, Map.of(unsafeSetting, "tenant-7")));
        assertFalse(settingError.getMessage().contains(unsafeSetting));
    }

    @Test
    void routesDatabaseFromReactorContextAndResetsSessionBeforeClosing() {
        AtomicInteger defaultCreates = new AtomicInteger();
        AtomicInteger tenantCreates = new AtomicInteger();
        AtomicInteger initializes = new AtomicInteger();
        AtomicInteger resets = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        AtomicReference<IsolationContext> initialized = new AtomicReference<>();
        ConnectionFactory defaultFactory = factory(defaultCreates, closes);
        ConnectionFactory tenantFactory = factory(tenantCreates, closes);
        R2dbcSessionCustomizer customizer = new R2dbcSessionCustomizer() {
            @Override
            public Mono<Void> initialize(Connection connection, IsolationContext context) {
                initializes.incrementAndGet();
                initialized.set(context);
                return Mono.empty();
            }

            @Override
            public Mono<Void> reset(Connection connection, IsolationContext context) {
                resets.incrementAndGet();
                return Mono.empty();
            }
        };
        RoutingConnectionFactory routing = new RoutingConnectionFactory(
                defaultFactory,
                key -> "tenant-db".equals(key) ? tenantFactory : defaultFactory,
                customizer);
        IsolationContext context = IsolationContext.database("tenant-7", "tenant-db")
                                                   .withSchema("tenant_7")
                                                   .withRlsSettings(Map.of("app.tenant_id", "tenant-7"));

        Mono<Void> useConnection = Mono.usingWhen(Mono.from(routing.create()),
                                                   ignored -> Mono.empty(),
                                                   connection -> Mono.from(connection.close()));
        StepVerifier.create(IsolationContexts.with(useConnection, context)).verifyComplete();

        assertEquals(0, defaultCreates.get());
        assertEquals(1, tenantCreates.get());
        assertEquals(1, initializes.get());
        assertEquals(context, initialized.get());
        assertEquals(1, resets.get());
        assertEquals(1, closes.get());
    }

    /** 路由键属于调用上下文原始输入，路由失败时不能把它写进异常消息。 */
    @Test
    void doesNotEchoDatabaseRouteWhenRouterReturnsNull() {
        AtomicInteger defaultCreates = new AtomicInteger();
        String sensitiveRoute = "tenant-db-password=must-not-leak";
        RoutingConnectionFactory routing = new RoutingConnectionFactory(
                factory(defaultCreates, new AtomicInteger()),
                ignored -> null,
                R2dbcSessionCustomizer.none());

        StepVerifier.create(IsolationContexts.with(
                            Mono.from(routing.create()),
                            IsolationContext.database("tenant-7", sensitiveRoute)))
                    .expectErrorSatisfies(error -> assertFalse(error.getMessage().contains(sensitiveRoute)))
                    .verify();

        assertEquals(0, defaultCreates.get());
    }

    @Test
    void invalidatesPartiallyInitializedConnectionWithoutReusableClose() {
        AtomicInteger creates = new AtomicInteger();
        AtomicInteger resets = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        R2dbcSessionCustomizer customizer = new R2dbcSessionCustomizer() {
            @Override
            public Mono<Void> initialize(Connection connection, IsolationContext context) {
                return Mono.error(new IllegalStateException("RLS setup failed"));
            }

            @Override
            public Mono<Void> reset(Connection connection, IsolationContext context) {
                resets.incrementAndGet();
                return Mono.empty();
            }
        };
        ConnectionFactory delegate = factory(creates, closes);
        R2dbcConnectionInvalidator invalidator = invalidator(closes, invalidations, null);
        RoutingConnectionFactory routing = new RoutingConnectionFactory(
                delegate, key -> delegate, customizer, invalidator);

        StepVerifier.create(Mono.from(routing.create()))
                    .expectErrorMessage("RLS setup failed")
                    .verify();

        assertEquals(0, resets.get());
        assertEquals(0, closes.get());
        assertEquals(1, invalidations.get());
    }

    /** 会话初始化尚未完成时取消获取，已借出的物理连接也必须失效，不能悬空或回到可复用池。 */
    @Test
    void invalidatesConnectionWhenSessionInitializationIsCancelled() {
        AtomicInteger creates = new AtomicInteger();
        AtomicInteger initializationCancellations = new AtomicInteger();
        AtomicInteger reusableCloses = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        ConnectionFactory delegate = factory(creates, reusableCloses);
        R2dbcSessionCustomizer customizer = new R2dbcSessionCustomizer() {
            @Override
            public Mono<Void> initialize(Connection connection, IsolationContext context) {
                return Mono.<Void>never().doOnCancel(initializationCancellations::incrementAndGet);
            }

            @Override
            public Mono<Void> reset(Connection connection, IsolationContext context) {
                return Mono.empty();
            }
        };
        RoutingConnectionFactory routing = new RoutingConnectionFactory(
                delegate,
                key -> delegate,
                customizer,
                invalidator(reusableCloses, invalidations, null));

        StepVerifier.create(Mono.from(routing.create()))
                    .thenCancel()
                    .verify();

        assertEquals(1, creates.get());
        assertEquals(1, initializationCancellations.get());
        assertEquals(1, invalidations.get());
        assertEquals(0, reusableCloses.get());
    }

    /** close 只是返回 Publisher，真正的 reset 和归还连接必须等订阅以后再发生。 */
    @Test
    void defersCloseUntilPublisherIsSubscribed() {
        AtomicInteger creates = new AtomicInteger();
        AtomicInteger resets = new AtomicInteger();
        AtomicInteger closes = new AtomicInteger();
        ConnectionFactory delegate = factory(creates, closes);
        R2dbcSessionCustomizer customizer = new R2dbcSessionCustomizer() {
            @Override
            public Mono<Void> initialize(Connection connection, IsolationContext context) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> reset(Connection connection, IsolationContext context) {
                return Mono.defer(() -> {
                    resets.incrementAndGet();
                    return Mono.empty();
                });
            }
        };
        RoutingConnectionFactory routing = new RoutingConnectionFactory(delegate, key -> delegate, customizer);
        Connection connection = Mono.from(routing.create()).block();

        Publisher<Void> close = connection.close();
        assertEquals(0, resets.get());
        assertEquals(0, closes.get());

        StepVerifier.create(close).verifyComplete();
        StepVerifier.create(connection.close()).verifyComplete();
        assertEquals(1, resets.get());
        assertEquals(1, closes.get());
    }

    /** reset 在返回 Publisher 前同步失败时也必须失效连接，不能绕过已经装配的异步错误边界。 */
    @Test
    void invalidatesConnectionWhenSessionResetThrowsSynchronously() {
        IllegalStateException resetFailure = new IllegalStateException("reset failed");
        AtomicInteger reusableCloses = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        ConnectionFactory delegate = factory(new AtomicInteger(), reusableCloses);
        R2dbcSessionCustomizer customizer = new R2dbcSessionCustomizer() {
            @Override
            public Mono<Void> initialize(Connection connection, IsolationContext context) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> reset(Connection connection, IsolationContext context) {
                throw resetFailure;
            }
        };
        RoutingConnectionFactory routing = new RoutingConnectionFactory(
                delegate, key -> delegate, customizer,
                invalidator(reusableCloses, invalidations, null));

        Connection connection = Mono.from(routing.create()).block();
        StepVerifier.create(Mono.from(connection.close()))
                    .expectErrorSatisfies(error -> assertSame(resetFailure, error))
                    .verify();

        assertEquals(1, invalidations.get());
        assertEquals(0, reusableCloses.get());
    }

    /** 初始化是主操作，清理失败只能作为补充信息，不能把真正的失败原因盖掉。 */
    @Test
    void preservesInitializationFailureWhenConnectionInvalidationAlsoFails() {
        IllegalStateException initializationFailure = new IllegalStateException("RLS setup failed");
        IllegalStateException invalidationFailure = new IllegalStateException("physical invalidation failed");
        AtomicInteger reusableCloses = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        Connection connection = (Connection) Proxy.newProxyInstance(
                RoutingConnectionFactoryTest.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> {
                    if ("close".equals(method.getName())) {
                        reusableCloses.incrementAndGet();
                        return Mono.empty();
                    }
                    if ("isAutoCommit".equals(method.getName())) {
                        return true;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
        ConnectionFactory delegate = new ConnectionFactory() {
            @Override
            public Mono<? extends Connection> create() {
                return Mono.just(connection);
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "test";
            }
        };
        R2dbcSessionCustomizer customizer = new R2dbcSessionCustomizer() {
            @Override
            public Mono<Void> initialize(Connection ignored, IsolationContext context) {
                return Mono.error(initializationFailure);
            }

            @Override
            public Mono<Void> reset(Connection ignored, IsolationContext context) {
                return Mono.empty();
            }
        };
        RoutingConnectionFactory routing = new RoutingConnectionFactory(
                delegate,
                key -> delegate,
                customizer,
                invalidator(reusableCloses, invalidations, invalidationFailure));

        StepVerifier.create(Mono.from(routing.create()))
                    .expectErrorSatisfies(error -> {
                        assertSame(initializationFailure, error);
                        assertEquals(1, error.getSuppressed().length);
                        assertSame(invalidationFailure, error.getSuppressed()[0]);
                    })
                    .verify();

        assertEquals(1, invalidations.get());
        assertEquals(0, reusableCloses.get());
    }

    /**
     * 验证初始化失败后的失效清理如遇 JVM 致命错误，必须优先原样传播该致命错误。
     */
    @Test
    void propagatesInvalidationFatalAfterInitializationFailure() {
        IllegalStateException initializationFailure = new IllegalStateException("RLS setup failed");
        OutOfMemoryError invalidationFatal = new OutOfMemoryError("physical invalidation fatal");
        AtomicInteger creates = new AtomicInteger();
        AtomicInteger reusableCloses = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        ConnectionFactory delegate = factory(creates, reusableCloses);
        R2dbcSessionCustomizer customizer = new R2dbcSessionCustomizer() {
            @Override
            public Mono<Void> initialize(Connection connection, IsolationContext context) {
                return Mono.error(initializationFailure);
            }

            @Override
            public Mono<Void> reset(Connection connection, IsolationContext context) {
                return Mono.empty();
            }
        };
        RoutingConnectionFactory routing = new RoutingConnectionFactory(
                delegate,
                key -> delegate,
                customizer,
                invalidator(reusableCloses, invalidations, invalidationFatal));

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                                                  () -> Mono.from(routing.create()).block());

        assertSame(invalidationFatal, observed);
        assertEquals(1, observed.getSuppressed().length);
        assertSame(initializationFailure, observed.getSuppressed()[0]);
        assertEquals(1, invalidations.get());
        assertEquals(0, reusableCloses.get());
    }

    /**
     * 验证 reset 失败后的失效清理如遇 JVM 致命错误，不能回退成原普通 reset 失败。
     */
    @Test
    void propagatesInvalidationFatalAfterResetFailure() {
        IllegalStateException resetFailure = new IllegalStateException("session reset failed");
        OutOfMemoryError invalidationFatal = new OutOfMemoryError("physical invalidation fatal");
        AtomicInteger creates = new AtomicInteger();
        AtomicInteger reusableCloses = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        ConnectionFactory delegate = factory(creates, reusableCloses);
        R2dbcSessionCustomizer customizer = new R2dbcSessionCustomizer() {
            @Override
            public Mono<Void> initialize(Connection connection, IsolationContext context) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> reset(Connection connection, IsolationContext context) {
                return Mono.error(resetFailure);
            }
        };
        RoutingConnectionFactory routing = new RoutingConnectionFactory(
                delegate,
                key -> delegate,
                customizer,
                invalidator(reusableCloses, invalidations, invalidationFatal));
        Connection connection = Mono.from(routing.create()).block();

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                                                  () -> Mono.from(connection.close()).block());

        assertSame(invalidationFatal, observed);
        assertEquals(1, observed.getSuppressed().length);
        assertSame(resetFailure, observed.getSuppressed()[0]);
        assertEquals(1, invalidations.get());
        assertEquals(0, reusableCloses.get());
    }

    /**
     * 验证失效异常已引用主异常时不反向追加 suppressed，避免 Throwable 图形成环。
     */
    @Test
    void avoidsSuppressedCycleWhenInvalidationFailureReferencesPrimaryFailure() {
        IllegalStateException initializationFailure = new IllegalStateException("RLS setup failed");
        IllegalStateException invalidationFailure = new IllegalStateException(
                "physical invalidation failed", initializationFailure);
        AtomicInteger creates = new AtomicInteger();
        AtomicInteger reusableCloses = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        ConnectionFactory delegate = factory(creates, reusableCloses);
        R2dbcSessionCustomizer customizer = new R2dbcSessionCustomizer() {
            @Override
            public Mono<Void> initialize(Connection connection, IsolationContext context) {
                return Mono.error(initializationFailure);
            }

            @Override
            public Mono<Void> reset(Connection connection, IsolationContext context) {
                return Mono.empty();
            }
        };
        RoutingConnectionFactory routing = new RoutingConnectionFactory(
                delegate,
                key -> delegate,
                customizer,
                invalidator(reusableCloses, invalidations, invalidationFailure));

        IllegalStateException observed = assertThrows(IllegalStateException.class,
                                                       () -> Mono.from(routing.create()).block());

        assertSame(initializationFailure, observed);
        assertFalse(reaches(observed, invalidationFailure));
        assertEquals(1, invalidations.get());
        assertEquals(0, reusableCloses.get());
    }

    /** reset 失败后的连接状态不可证明安全，必须失效一次且绝不进入普通可复用 close。 */
    @Test
    void resetFailureInvalidatesExactlyOnceWithoutReusableClose() {
        IllegalStateException resetFailure = new IllegalStateException("session reset failed");
        AtomicInteger creates = new AtomicInteger();
        AtomicInteger reusableCloses = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        ConnectionFactory delegate = factory(creates, reusableCloses);
        R2dbcSessionCustomizer customizer = new R2dbcSessionCustomizer() {
            @Override
            public Mono<Void> initialize(Connection connection, IsolationContext context) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> reset(Connection connection, IsolationContext context) {
                return Mono.error(resetFailure);
            }
        };
        RoutingConnectionFactory routing = new RoutingConnectionFactory(
                delegate,
                key -> delegate,
                customizer,
                invalidator(reusableCloses, invalidations, null));
        Connection connection = Mono.from(routing.create()).block();

        StepVerifier.create(Mono.from(connection.close()))
                    .expectErrorSatisfies(error -> assertSame(resetFailure, error))
                    .verify();

        assertEquals(1, invalidations.get());
        assertEquals(0, reusableCloses.get());
    }

    /** reset 与物理失效同时失败时，reset 保持主错误，失效错误只作为抑制信息保留。 */
    @Test
    void preservesResetFailureWhenConnectionInvalidationAlsoFails() {
        IllegalStateException resetFailure = new IllegalStateException("session reset failed");
        IllegalStateException invalidationFailure = new IllegalStateException("physical invalidation failed");
        AtomicInteger creates = new AtomicInteger();
        AtomicInteger reusableCloses = new AtomicInteger();
        AtomicInteger invalidations = new AtomicInteger();
        ConnectionFactory delegate = factory(creates, reusableCloses);
        R2dbcSessionCustomizer customizer = new R2dbcSessionCustomizer() {
            @Override
            public Mono<Void> initialize(Connection connection, IsolationContext context) {
                return Mono.empty();
            }

            @Override
            public Mono<Void> reset(Connection connection, IsolationContext context) {
                return Mono.error(resetFailure);
            }
        };
        RoutingConnectionFactory routing = new RoutingConnectionFactory(
                delegate,
                key -> delegate,
                customizer,
                invalidator(reusableCloses, invalidations, invalidationFailure));
        Connection connection = Mono.from(routing.create()).block();

        StepVerifier.create(Mono.from(connection.close()))
                    .expectErrorSatisfies(error -> {
                        assertSame(resetFailure, error);
                        assertEquals(1, error.getSuppressed().length);
                        assertSame(invalidationFailure, error.getSuppressed()[0]);
                    })
                    .verify();

        assertEquals(1, invalidations.get());
        assertEquals(0, reusableCloses.get());
    }

    /** 默认失效器无法证明物理淘汰时必须报错，不能把普通 close 冒充淘汰。 */
    @Test
    void defaultInvalidatorFailsClosedWithoutCallingReusableClose() {
        AtomicInteger closes = new AtomicInteger();
        Connection connection = connection(closes);

        StepVerifier.create(Mono.from(R2dbcConnectionInvalidator.failClosed().invalidate(connection)))
                    .expectError(IllegalStateException.class)
                    .verify();

        assertEquals(0, closes.get());
    }

    /**
     * 按 cause 和 suppressed 边检查异常图可达性，用于证明不会把已指向主异常的清理异常反向接入。
     */
    private static boolean reaches(Throwable start, Throwable expected) {
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.addFirst(start);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (current == expected) {
                return true;
            }
            Throwable cause = current.getCause();
            if (cause != null) {
                pending.addFirst(cause);
            }
            for (Throwable suppressed : current.getSuppressed()) {
                pending.addFirst(suppressed);
            }
        }
        return false;
    }

    private static R2dbcConnectionInvalidator invalidator(AtomicInteger reusableCloses,
                                                           AtomicInteger invalidations,
                                                           Throwable invalidationFailure) {
        return new R2dbcConnectionInvalidator() {
            @Override
            public Publisher<Void> close(Connection connection) {
                reusableCloses.incrementAndGet();
                return Mono.empty();
            }

            @Override
            public Publisher<Void> invalidate(Connection connection) {
                invalidations.incrementAndGet();
                return invalidationFailure == null ? Mono.empty() : Mono.error(invalidationFailure);
            }
        };
    }

    private static Connection connection(AtomicInteger closes) {
        return (Connection) Proxy.newProxyInstance(
                RoutingConnectionFactoryTest.class.getClassLoader(),
                new Class<?>[]{Connection.class},
                (proxy, method, arguments) -> {
                    if ("close".equals(method.getName())) {
                        closes.incrementAndGet();
                        return Mono.empty();
                    }
                    if ("isAutoCommit".equals(method.getName())) {
                        return true;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static ConnectionFactory factory(AtomicInteger creates, AtomicInteger closes) {
        Connection connection = connection(closes);
        return new ConnectionFactory() {
            @Override
            public Mono<? extends Connection> create() {
                creates.incrementAndGet();
                return Mono.just(connection);
            }

            @Override
            public ConnectionFactoryMetadata getMetadata() {
                return () -> "test";
            }
        };
    }
}
