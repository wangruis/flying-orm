package com.flying.orm.rdb.isolation;

import io.r2dbc.spi.Batch;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.ConnectionFactoryMetadata;
import io.r2dbc.spi.ConnectionMetadata;
import io.r2dbc.spi.IsolationLevel;
import io.r2dbc.spi.Statement;
import io.r2dbc.spi.TransactionDefinition;
import io.r2dbc.spi.ValidationDepth;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 按 Reactor Context 选择数据库，并统一管理 schema/RLS 会话的连接工厂。
 *
 * <p>把它交给 {@code R2dbcSqlExecutor} 后，单条查询、批量写入、事务和回执恢复都会经过同一路由。
 * 连接包装器只转发 R2DBC 方法，唯一额外行为是在第一次 close 时先 reset 会话再归还连接池。</p>
 * @author wangr
 * @date 2026-08-03
 * @version v1.0
 */
public final class RoutingConnectionFactory implements ConnectionFactory {

    private final ConnectionFactory defaultFactory;
    private final DatabaseConnectionRouter databaseRouter;
    private final R2dbcSessionCustomizer sessionCustomizer;
    private final R2dbcConnectionInvalidator connectionInvalidator;

    /**
     * 使用 fail-closed 异常连接失效策略创建路由连接工厂。普通 reset 成功后仍正常归还连接；initialize、
     * reset 或普通关闭失败时，如果部署环境没有显式物理淘汰能力，原错误会保留失效失败作为抑制信息。
     *
     * @param defaultFactory    没有数据库路由键时使用的默认连接工厂
     * @param databaseRouter    数据库路由键到连接工厂的解析器
     * @param sessionCustomizer schema/RLS 会话初始化与 reset 适配器
     */
    public RoutingConnectionFactory(ConnectionFactory defaultFactory,
                                    DatabaseConnectionRouter databaseRouter,
                                    R2dbcSessionCustomizer sessionCustomizer) {
        this(defaultFactory,
             databaseRouter,
             sessionCustomizer,
             R2dbcConnectionInvalidator.failClosed());
    }

    /**
     * 使用显式连接失效能力创建路由连接工厂。连接池部署应通过
     * {@link R2dbcConnectionInvalidator#of(java.util.function.Function, java.util.function.Function)} 传入能证明
     * 物理淘汰的池原生入口；初始化或 reset 失败不会再走普通可复用 close。
     *
     * @param defaultFactory       没有数据库路由键时使用的默认连接工厂
     * @param databaseRouter       数据库路由键到连接工厂的解析器
     * @param sessionCustomizer    schema/RLS 会话初始化与 reset 适配器
     * @param connectionInvalidator 普通关闭与异常物理淘汰的明确边界
     */
    public RoutingConnectionFactory(ConnectionFactory defaultFactory,
                                    DatabaseConnectionRouter databaseRouter,
                                    R2dbcSessionCustomizer sessionCustomizer,
                                    R2dbcConnectionInvalidator connectionInvalidator) {
        this.defaultFactory = Objects.requireNonNull(defaultFactory, "default connection factory must not be null");
        this.databaseRouter = Objects.requireNonNull(databaseRouter, "database connection router must not be null");
        this.sessionCustomizer = Objects.requireNonNull(sessionCustomizer, "session customizer must not be null");
        this.connectionInvalidator = Objects.requireNonNull(connectionInvalidator,
                                                             "connection invalidator must not be null");
    }

    @Override
    public Publisher<? extends Connection> create() {
        return Mono.deferContextual(contextView -> {
            IsolationContext context = IsolationContexts.current(contextView);
            ConnectionFactory selected = context.databaseKey() == null
                    ? defaultFactory
                    : Objects.requireNonNull(databaseRouter.route(context.databaseKey()),
                                             "database router returned null");
            return Mono.from(selected.create())
                       .flatMap(connection -> Mono.usingWhen(
                               Mono.just(connection),
                               ignored -> Mono.from(sessionCustomizer.initialize(connection, context))
                                              .thenReturn(new ResettingConnection(connection,
                                                                                  context,
                                                                                  sessionCustomizer,
                                                                                  connectionInvalidator)),
                               ignored -> Mono.empty(),
                               (ignored, error) -> Mono.empty(),
                               ignored -> Mono.from(connectionInvalidator.invalidate(connection)))
                               .onErrorResume(error -> invalidateAndPreserve(
                                       connection, connectionInvalidator, error)));
        });
    }

    private static <T> Mono<T> invalidateAndPreserve(Connection connection,
                                                     R2dbcConnectionInvalidator invalidator,
                                                     Throwable primaryError) {
        return Mono.defer(() -> Mono.from(invalidator.invalidate(connection)))
                   .onErrorResume(invalidationError -> {
                       VirtualMachineError fatal = promoteVirtualMachineError(primaryError, invalidationError);
                       if (fatal != null) {
                           return Mono.error(fatal);
                       }
                       addSuppressedIfAcyclic(primaryError, invalidationError);
                       return Mono.empty();
                   })
                   .then(Mono.defer(() -> {
                       VirtualMachineError fatal = findVirtualMachineError(primaryError);
                       return Mono.error(fatal == null ? primaryError : fatal);
                   }));
    }

    /**
     * 在主操作与失效清理的异常图中选择必须原样向外传播的 JVM 致命错误；普通清理错误只作无环补充。
     */
    private static VirtualMachineError promoteVirtualMachineError(Throwable primaryError, Throwable cleanupError) {
        VirtualMachineError primaryFatal = findVirtualMachineError(primaryError);
        if (primaryFatal != null) {
            addSuppressedIfAcyclic(primaryFatal, cleanupError);
            return primaryFatal;
        }
        VirtualMachineError cleanupFatal = findVirtualMachineError(cleanupError);
        if (cleanupFatal != null) {
            addSuppressedIfAcyclic(cleanupFatal, primaryError);
        }
        return cleanupFatal;
    }

    /**
     * 按 identity 遍历 cause 与 suppressed，避免异常对象被重复引用或异常图异常时陷入循环。
     */
    private static VirtualMachineError findVirtualMachineError(Throwable error) {
        ArrayDeque<Throwable> pending = new ArrayDeque<>();
        Set<Throwable> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        pending.addFirst(error);
        while (!pending.isEmpty()) {
            Throwable current = pending.removeFirst();
            if (!visited.add(current)) {
                continue;
            }
            if (current instanceof VirtualMachineError fatal) {
                return fatal;
            }
            Throwable cause = current.getCause();
            if (cause != null) {
                pending.addFirst(cause);
            }
            for (Throwable suppressed : current.getSuppressed()) {
                pending.addFirst(suppressed);
            }
        }
        return null;
    }

    /**
     * 仅在两个异常图彼此不可达时保存清理上下文，禁止通过 suppressed 反向闭合 cause 图。
     */
    private static void addSuppressedIfAcyclic(Throwable primaryError, Throwable cleanupError) {
        if (primaryError != cleanupError
                && !reaches(primaryError, cleanupError)
                && !reaches(cleanupError, primaryError)) {
            primaryError.addSuppressed(cleanupError);
        }
    }

    /**
     * 判断异常图能否从起点到达目标；该方法只服务清理错误的无环合并，不修改任一异常对象。
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

    @Override
    public ConnectionFactoryMetadata getMetadata() {
        // 执行器会在装配时读取一次元数据来选择 bind marker，因此默认库和所有路由库必须使用同一种方言。
        return defaultFactory.getMetadata();
    }

    private static final class ResettingConnection implements Connection {

        private final Connection delegate;
        private final Mono<Void> close;

        private ResettingConnection(Connection delegate,
                                    IsolationContext context,
                                    R2dbcSessionCustomizer customizer,
                                    R2dbcConnectionInvalidator invalidator) {
            this.delegate = delegate;
            AtomicBoolean terminal = new AtomicBoolean();
            /*
             * R2DBC 的 close() 返回冷 Publisher，调用方法本身不等于已经关闭。整个流程包在 defer 里，
             * 避免调用方只拿到 Publisher 却没有订阅时就把连接误标成已关闭。并发 close 共享同一次
             * reset + close；最后一个订阅者取消时 usingWhen 会先物理失效连接，不能留下永久后台 reset。
             */
            Mono<Void> operation = Mono.defer(() -> Mono.from(customizer.reset(delegate, context)))
                    .onErrorResume(resetError -> invalidateAndPreserve(delegate, invalidator, resetError))
                    .then(Mono.defer(() -> Mono.from(invalidator.close(delegate)))
                              .onErrorResume(closeError -> invalidateAndPreserve(
                                      delegate, invalidator, closeError)))
                    .doOnSuccess(ignored -> terminal.set(true))
                    .doOnError(ignored -> terminal.set(true));
            Mono<Void> shared = Mono.usingWhen(
                    Mono.just(delegate),
                    ignored -> operation,
                    ignored -> Mono.empty(),
                    (ignored, error) -> Mono.empty(),
                    ignored -> {
                        terminal.set(true);
                        return Mono.defer(() -> Mono.from(invalidator.invalidate(delegate)));
                    })
                    .flux()
                    .replay(1)
                    .refCount(1)
                    .then();
            this.close = Mono.defer(() -> terminal.get() ? Mono.empty() : shared);
        }

        @Override
        public Publisher<Void> close() {
            return close;
        }

        @Override public Publisher<Void> beginTransaction() { return delegate.beginTransaction(); }
        @Override public Publisher<Void> beginTransaction(TransactionDefinition definition) { return delegate.beginTransaction(definition); }
        @Override public Publisher<Void> commitTransaction() { return delegate.commitTransaction(); }
        @Override public Batch createBatch() { return delegate.createBatch(); }
        @Override public Publisher<Void> createSavepoint(String name) { return delegate.createSavepoint(name); }
        @Override public Statement createStatement(String sql) { return delegate.createStatement(sql); }
        @Override public boolean isAutoCommit() { return delegate.isAutoCommit(); }
        @Override public ConnectionMetadata getMetadata() { return delegate.getMetadata(); }
        @Override public IsolationLevel getTransactionIsolationLevel() { return delegate.getTransactionIsolationLevel(); }
        @Override public Publisher<Void> releaseSavepoint(String name) { return delegate.releaseSavepoint(name); }
        @Override public Publisher<Void> rollbackTransaction() { return delegate.rollbackTransaction(); }
        @Override public Publisher<Void> rollbackTransactionToSavepoint(String name) { return delegate.rollbackTransactionToSavepoint(name); }
        @Override public Publisher<Void> setAutoCommit(boolean autoCommit) { return delegate.setAutoCommit(autoCommit); }
        @Override public Publisher<Void> setLockWaitTimeout(Duration timeout) { return delegate.setLockWaitTimeout(timeout); }
        @Override public Publisher<Void> setStatementTimeout(Duration timeout) { return delegate.setStatementTimeout(timeout); }
        @Override public Publisher<Void> setTransactionIsolationLevel(IsolationLevel isolationLevel) { return delegate.setTransactionIsolationLevel(isolationLevel); }
        @Override public Publisher<Boolean> validate(ValidationDepth depth) { return delegate.validate(depth); }
    }
}
