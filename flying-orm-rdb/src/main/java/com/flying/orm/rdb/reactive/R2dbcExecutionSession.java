package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.exception.RdbExceptionTranslator;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.isolation.IsolationContexts;
import com.flying.orm.rdb.isolation.R2dbcConnectionInvalidator;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipant;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

/**
 * 统一维护一条 SQL 执行链路里的连接、Statement 和收尾动作。
 * 查询、更新和同连接序列都从这里获取并释放连接，避免各入口对超时、取消和清理失败作出不同判断。
 * 实例只保存不可变依赖，可以被执行器并发复用；每次订阅的连接和行映射状态彼此隔离。
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class R2dbcExecutionSession {

    private final ConnectionFactory connectionFactory;

    private final R2dbcBindMarkers bindMarkers;

    private final SqlExecutionObserver observer;

    private final R2dbcConnectionInvalidator connectionInvalidator;

    private final R2dbcTransactionParticipant transactionParticipant;

    R2dbcExecutionSession(ConnectionFactory connectionFactory,
                          R2dbcBindMarkers bindMarkers,
                          SqlExecutionObserver observer,
                          R2dbcConnectionInvalidator connectionInvalidator,
                          R2dbcTransactionParticipant transactionParticipant) {
        this.connectionFactory = Objects.requireNonNull(connectionFactory,
                                                        "connection factory must not be null");
        this.bindMarkers = Objects.requireNonNull(bindMarkers, "bind marker adapter must not be null");
        this.observer = Objects.requireNonNull(observer, "sql execution observer must not be null");
        this.connectionInvalidator = Objects.requireNonNull(connectionInvalidator,
                                                             "connection invalidator must not be null");
        this.transactionParticipant = Objects.requireNonNull(transactionParticipant,
                                                              "transaction participant must not be null");
    }

    <T> Flux<T> withStatement(SqlRequest request,
                              SqlExecutionOptions options,
                              SqlExecutionOperation operationType,
                              java.util.function.BiFunction<Statement,
                                      R2dbcLargeObjectScope, Publisher<T>> operation,
                              Function<Flux<T>, Flux<T>> operationProtection) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options,
                                                              "sql execution options must not be null");
        Function<Flux<T>, Flux<T>> safeProtection = Objects.requireNonNull(
                operationProtection, "sql operation protection must not be null");
        return Flux.usingWhen(acquireConnection(safeOptions),
                              lease -> {
                                  Statement statement = prepareStatement(lease.connection(),
                                                                       safeRequest.sql(),
                                                                       safeRequest.parameters().size(),
                                                                       safeRequest.bindMarkerStyle(),
                                                                       safeRequest.parameters(),
                                                                       safeOptions.fetchSize());
                                  return safeProtection.apply(Flux.from(
                                          operation.apply(statement, lease.largeObjects())));
                              },
                              lease -> closeAfterResult(lease, operationType, safeOptions, true),
                              (lease, error) -> closeAfterError(
                                      lease, operationType, safeOptions, error),
                              // 查询取消不产生提交不确定性，但仍须先释放未消费的 LOB。
                              lease -> closeAfterResult(lease, operationType, safeOptions, true));
    }
    <T> Mono<T> withStatementMono(SqlRequest request,
                                  SqlExecutionOptions options,
                                  SqlExecutionOperation operationType,
                                  java.util.function.BiFunction<Statement,
                                          R2dbcLargeObjectScope, Mono<T>> operation) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options,
                                                              "sql execution options must not be null");
        return Mono.usingWhen(acquireConnection(safeOptions),
                             lease -> {
                                 Statement statement = prepareStatement(lease.connection(),
                                                                      safeRequest.sql(),
                                                                      safeRequest.parameters().size(),
                                                                      safeRequest.bindMarkerStyle(),
                                                                      safeRequest.parameters());
                                 return protectMono(
                                         operation.apply(statement, lease.largeObjects()), safeOptions);
                             },
                             lease -> closeAfterResult(lease, operationType, safeOptions, true),
                             (lease, error) -> closeAfterError(lease, operationType, safeOptions, error),
                             lease -> closeAfterResult(lease, operationType, safeOptions, false));
    }
    Flux<DynamicRow> protectRows(Flux<DynamicRow> source, String sql, SqlExecutionOptions options) {
        return ReactiveSqlExecutionProtection.protectRows(source, sql, options, BatchMemoryBudget::estimateRowBytes);
    }
    <T> Mono<T> protectMono(Mono<T> source, SqlExecutionOptions options) {
        return ReactiveSqlExecutionProtection.protectMono(source, options);
    }
    Mono<ConnectionLease> acquireConnection(SqlExecutionOptions options) {
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options,
                                                             "sql execution options must not be null");
        // 外部上下文中的 routingIdentity 已在事务开始时固定；校验后直接借用主库连接，不再经过路由工厂。
        return Mono.deferContextual(context -> transactionParticipant.currentTransaction(
                           IsolationContexts.currentDatabaseKey(context))
                   .map(transaction -> new ConnectionLease(transaction.connection(), true,
                                                           new R2dbcLargeObjectScope()))
                   .switchIfEmpty(acquireOwnedConnection(safeOptions)));
    }
    private Mono<ConnectionLease> acquireOwnedConnection(SqlExecutionOptions safeOptions) {
        // create() 也放进 defer：外部事务已给出连接时，备用连接工厂不应被调用。
        Mono<Connection> connection = Mono.defer(() -> Mono.from(connectionFactory.create()));
        if (safeOptions.connectionAcquireTimeout().isZero()) {
            return connection.map(owned -> new ConnectionLease(
                    owned, false, new R2dbcLargeObjectScope()));
        }
        return connection.timeout(safeOptions.connectionAcquireTimeout())
                         .onErrorMap(TimeoutException.class,
                                     error -> new com.flying.orm.rdb.exception.RdbConnectionAcquireTimeoutException(
                                             safeOptions.connectionAcquireTimeout(),
                                             error))
                         .map(owned -> new ConnectionLease(
                                 owned, false, new R2dbcLargeObjectScope()));
    }

    Statement prepareStatement(Connection connection,
                             String sql,
                             int parameterCount,
                             SqlBindMarkerStyle bindMarkerStyle,
                             List<Object> parameters) {
        return prepareStatement(connection, sql, parameterCount, bindMarkerStyle, parameters, 0);
    }
    private Statement prepareStatement(Connection connection,
                                       String sql,
                                       int parameterCount,
                                       SqlBindMarkerStyle bindMarkerStyle,
                                       List<Object> parameters,
                                       int fetchSize) {
        Statement statement = connection.createStatement(sqlForDriver(sql, parameterCount, bindMarkerStyle));
        if (fetchSize > 0) {
            statement.fetchSize(fetchSize);
        }
        return bind(statement, parameters);
    }

    Mono<Void> closeAfterResult(ConnectionLease lease,
                               SqlExecutionOperation operation,
                               SqlExecutionOptions options,
                               boolean outcomeConfirmed) {
        ConnectionLease safeLease = Objects.requireNonNull(lease, "connection lease must not be null");
        if (safeLease.external()) {
            return safeLease.largeObjects().complete();
        }
        Connection connection = safeLease.connection();
        Duration timeout = options.cleanupTimeout();
        Mono<Void> cleanup = Mono.defer(() -> Mono.from(outcomeConfirmed
                ? connectionInvalidator.close(connection)
                : connectionInvalidator.invalidate(connection)));
        if (!timeout.isZero()) {
            cleanup = cleanup.timeout(timeout);
        }
        Mono<Void> connectionCleanup = cleanup;
        Mono<Boolean> largeObjects = safeLease.largeObjects().complete().thenReturn(true)
                .onErrorResume(error -> invalidateAfterCleanupFailure(
                        safeLease, operation, ResourceCleanupObservation.Phase.CONNECTION_INVALIDATE,
                        options, outcomeConfirmed, error).thenReturn(false));
        if (!outcomeConfirmed) {
            return largeObjects.flatMap(reusable -> reusable ? connectionCleanup.onErrorResume(error -> {
                observer.onResourceCleanup(new ResourceCleanupObservation(
                        operation,
                        ResourceCleanupObservation.Phase.CONNECTION_INVALIDATE,
                        false,
                        error));
                VirtualMachineError fatal = ReactiveSqlExecutionProtection.findVirtualMachineError(error);
                return fatal == null ? Mono.empty() : Mono.error(fatal);
            }) : Mono.empty());
        }
        return largeObjects.flatMap(reusable -> reusable
                ? connectionCleanup.onErrorResume(error -> invalidateAfterCleanupFailure(safeLease,
                        operation, ResourceCleanupObservation.Phase.CONNECTION_CLOSE,
                        options, outcomeConfirmed, error))
                : Mono.empty());
    }

    private Mono<Void> closeAfterError(ConnectionLease lease,
                                       SqlExecutionOperation operation,
                                       SqlExecutionOptions options,
                                       Throwable error) {
        Mono<Void> largeObjectCleanup = lease.largeObjects().error(error)
                .onErrorResume(cleanupError -> {
                    VirtualMachineError fatal = ReactiveSqlExecutionProtection.promoteVirtualMachineError(
                            error, cleanupError);
                    if (fatal == null) {
                        return Mono.empty();
                    }
                    return invalidateAfterCleanupFailure(
                            lease, operation, ResourceCleanupObservation.Phase.CONNECTION_INVALIDATE,
                            options, false, fatal).then(Mono.error(fatal));
                });
        return largeObjectCleanup.then(Mono.defer(() -> {
            RuntimeException translated = RdbExceptionTranslator.translate(error);
            boolean reusable = translated instanceof RdbException rdbException
                    && reusableAfterError(rdbException.kind());
            return closeAfterResult(lease, operation, options, reusable);
        }));
    }
    private static boolean reusableAfterError(RdbErrorKind kind) {
        return switch (kind) {
            // 服务端已明确拒绝语句时，普通自动提交连接仍可安全 reset 后归池。
            case DUPLICATE_KEY, CONSTRAINT, BAD_SQL, DEADLOCK, LOCK_TIMEOUT -> true;
            // 连接、超时、取消或未知错误不能证明驱动会话已经干净。
            case CONNECTION, TIMEOUT, CANCELLED, UNKNOWN -> false;
        };
    }
    Mono<Void> invalidateAfterCleanupFailure(ConnectionLease lease,
                                            SqlExecutionOperation operation,
                                            ResourceCleanupObservation.Phase phase,
                                            SqlExecutionOptions options,
                                            boolean outcomeConfirmed,
                                            Throwable primaryError) {
        ConnectionLease safeLease = Objects.requireNonNull(lease, "connection lease must not be null");
        if (safeLease.external()) {
            return Mono.empty();
        }
        Connection connection = safeLease.connection();
        Duration timeout = options.cleanupTimeout();
        Mono<Void> invalidation = Mono.defer(() -> Mono.from(connectionInvalidator.invalidate(connection)));
        if (!timeout.isZero()) {
            invalidation = invalidation.timeout(timeout);
        }
        return invalidation.onErrorResume(invalidationError -> {
            VirtualMachineError fatal = ReactiveSqlExecutionProtection.promoteVirtualMachineError(
                    primaryError, invalidationError);
            if (fatal != null) {
                observer.onResourceCleanup(new ResourceCleanupObservation(
                        operation, phase, outcomeConfirmed, fatal));
                return Mono.error(fatal);
            }
            ReactiveSqlExecutionProtection.addSuppressedIfAcyclic(primaryError, invalidationError);
            return Mono.empty();
        }).then(Mono.defer(() -> {
            VirtualMachineError fatal = ReactiveSqlExecutionProtection.findVirtualMachineError(primaryError);
            Throwable observationError = fatal == null ? primaryError : fatal;
            observer.onResourceCleanup(new ResourceCleanupObservation(
                    operation, phase, outcomeConfirmed, observationError));
            return fatal == null ? Mono.empty() : Mono.error(fatal);
        }));
    }

    static Publisher<DynamicRow> mapRows(Result result,
                                         SqlExecutionOptions options,
                                         R2dbcLargeObjectScope cleanupScope) {
        return R2dbcLargeObjectRows.map(result, options, cleanupScope);
    }

    String sqlForDriver(String sql, int parameterCount, SqlBindMarkerStyle bindMarkerStyle) {
        return bindMarkers.adapt(sql, parameterCount, bindMarkerStyle);
    }

    private Statement bind(Statement statement,
                          List<Object> parameters) {
        return bind(statement, parameters, null);
    }

    private Statement bind(Statement statement,
                          List<Object> parameters,
                          List<Class<?>> parameterTypes) {
        for (int i = 0; i < parameters.size(); i++) {
            Object value = parameters.get(i);
            if (value == null) {
                Class<?> parameterType = parameterTypes == null ? Object.class : parameterTypes.get(i);
                statement.bindNull(i, parameterType);
            } else {
                statement.bind(i, R2dbcParameterValues.forBinding(value));
            }
        }
        return statement;
    }

    /** 每次订阅独享的连接借用凭据，不需要共享 Map 或锁判断关闭责任。 */
    record ConnectionLease(Connection connection,
                           boolean external,
                           R2dbcLargeObjectScope largeObjects) {

        ConnectionLease {
            connection = Objects.requireNonNull(connection, "leased connection must not be null");
            largeObjects = Objects.requireNonNull(largeObjects, "large object cleanup scope must not be null");
        }
    }
}
