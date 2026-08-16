package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.isolation.IsolationContexts;
import com.flying.orm.rdb.isolation.R2dbcConnectionInvalidator;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.observation.SqlTransactionSource;
import com.flying.orm.rdb.result.DynamicRow;
import com.flying.orm.rdb.transaction.R2dbcTransactionContext;
import com.flying.orm.rdb.transaction.R2dbcTransactionParticipant;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.ConnectionFactory;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

import java.util.List;
import java.util.Objects;
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

    private final R2dbcConnectionLeaseCleanup leaseCleanup;

    private final R2dbcTransactionParticipant transactionParticipant;

    R2dbcExecutionSession(ConnectionFactory connectionFactory,
                          R2dbcBindMarkers bindMarkers,
                          SqlExecutionObserver observer,
                          R2dbcConnectionInvalidator connectionInvalidator,
                          R2dbcTransactionParticipant transactionParticipant) {
        this.connectionFactory = Objects.requireNonNull(connectionFactory,
                                                        "connection factory must not be null");
        this.bindMarkers = Objects.requireNonNull(bindMarkers, "bind marker adapter must not be null");
        this.leaseCleanup = new R2dbcConnectionLeaseCleanup(observer, connectionInvalidator);
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
        return Flux.usingWhen(acquireConnection(),
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
                              lease -> leaseCleanup.closeAfterResult(lease, operationType, safeOptions, true),
                              (lease, error) -> leaseCleanup.closeAfterError(
                                      lease, operationType, safeOptions, error),
                              lease -> leaseCleanup.cancelAfterResult(lease, operationType, safeOptions));
    }
    <T> Mono<T> withStatementMono(SqlRequest request,
                                  SqlExecutionOptions options,
                                  SqlExecutionOperation operationType,
                                  java.util.function.BiFunction<Statement,
                                          R2dbcLargeObjectScope, Mono<T>> operation) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options,
                                                              "sql execution options must not be null");
        return Mono.usingWhen(acquireConnection(),
                             lease -> {
                                 Statement statement = prepareStatement(lease.connection(),
                                                                      safeRequest.sql(),
                                                                      safeRequest.parameters().size(),
                                                                      safeRequest.bindMarkerStyle(),
                                                                      safeRequest.parameters());
                                 return protectMono(
                                         operation.apply(statement, lease.largeObjects()), safeOptions);
                             },
                             lease -> leaseCleanup.closeAfterResult(lease, operationType, safeOptions, true),
                             (lease, error) -> leaseCleanup.closeAfterError(lease, operationType, safeOptions, error),
                             lease -> leaseCleanup.cancelAfterResult(lease, operationType, safeOptions));
    }
    Flux<DynamicRow> protectRows(Flux<DynamicRow> source, String sql, SqlExecutionOptions options) {
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options,
                                                                  "sql execution options must not be null");
        return Flux.deferContextual(context -> {
            R2dbcSqlDeadline deadline = R2dbcSqlDeadline.currentOrStart(context, safeOptions);
            Flux<DynamicRow> boundedRows = ReactiveSqlExecutionProtection.protectRows(
                    source,
                    sql,
                    safeOptions.withTimeout(java.time.Duration.ZERO),
                    BatchMemoryBudget::estimateRowBytes);
            return deadline.protectExecution(boundedRows);
        });
    }
    <T> Mono<T> protectMono(Mono<T> source, SqlExecutionOptions options) {
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options,
                                                                  "sql execution options must not be null");
        return Mono.deferContextual(context -> R2dbcSqlDeadline.currentOrStart(context, safeOptions)
                                                               .protectExecution(source));
    }
    Mono<ConnectionLease> acquireConnection() {
        return Mono.deferContextual(context -> {
            ReactiveTransactionSourceResolver.Resolution bound = context.getOrDefault(
                    ReactiveTransactionSourceResolver.Resolution.class, null);
            if (bound != null) {
                return bound.transaction() == null
                        ? acquireOwnedConnection()
                        : Mono.just(externalLease(bound.transaction()));
            }
            return currentTransaction(context)
                    .map(this::externalLease)
                    .switchIfEmpty(acquireOwnedConnection());
        });
    }

    Mono<ReactiveTransactionSourceResolver.Resolution> resolveTransaction() {
        return Mono.deferContextual(context -> context
                .<ReactiveTransactionSourceResolver.Resolution>getOrEmpty(
                        ReactiveTransactionSourceResolver.Resolution.class)
                .map(Mono::just)
                .orElseGet(() -> {
                    return currentTransaction(context)
                                   .map(transaction -> new ReactiveTransactionSourceResolver.Resolution(
                                           SqlTransactionSource.EXTERNAL, transaction))
                                   .defaultIfEmpty(new ReactiveTransactionSourceResolver.Resolution(
                                           SqlTransactionSource.AUTO_COMMIT, null));
                }));
    }

    /** 外部上下文中的路由身份已在事务开始时固定，执行会话只借用该连接。 */
    private ConnectionLease externalLease(R2dbcTransactionContext transaction) {
        return new ConnectionLease(transaction.connection(), true, new R2dbcLargeObjectScope());
    }

    /** 事务参与者解析由上层事务管理器控制，执行会话不叠加本地截止时间。 */
    private Mono<R2dbcTransactionContext> currentTransaction(ContextView context) {
        String databaseKey = IsolationContexts.currentDatabaseKey(context);
        return Mono.defer(() -> Objects.requireNonNull(
                transactionParticipant.currentTransaction(databaseKey),
                "current transaction publisher must not be null"));
    }

    private Mono<ConnectionLease> acquireOwnedConnection() {
        // create() 也放进 defer：外部事务已给出连接时，备用连接工厂不应被调用。
        return Mono.defer(() -> Mono.from(connectionFactory.create()))
                   .map(owned -> new ConnectionLease(owned, false, new R2dbcLargeObjectScope()));
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
        return leaseCleanup.closeAfterResult(lease, operation, options, outcomeConfirmed);
    }

    Mono<Void> closeAfterResult(ConnectionLease lease,
                                SqlExecutionOperation operation,
                                SqlExecutionOptions options,
                                boolean outcomeConfirmed,
                                R2dbcCleanupDeadline deadline) {
        return leaseCleanup.closeAfterResult(lease, operation, options, outcomeConfirmed, deadline);
    }
    Mono<Void> invalidateAfterCleanupFailure(ConnectionLease lease,
                                            SqlExecutionOperation operation,
                                            ResourceCleanupObservation.Phase phase,
                                            SqlExecutionOptions options,
                                            boolean outcomeConfirmed,
                                            Throwable primaryError) {
        return leaseCleanup.invalidateAfterCleanupFailure(
                lease, operation, phase, options, outcomeConfirmed, primaryError);
    }

    Mono<Void> invalidateAfterCleanupFailure(ConnectionLease lease,
                                             SqlExecutionOperation operation,
                                             ResourceCleanupObservation.Phase phase,
                                             SqlExecutionOptions options,
                                             boolean outcomeConfirmed,
                                             Throwable primaryError,
                                             R2dbcCleanupDeadline deadline) {
        return leaseCleanup.invalidateAfterCleanupFailure(
                lease, operation, phase, options, outcomeConfirmed, primaryError, deadline);
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
