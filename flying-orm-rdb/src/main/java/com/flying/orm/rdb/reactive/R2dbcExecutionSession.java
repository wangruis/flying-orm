package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.internal.binding.SqlNullParameter;
import com.flying.orm.rdb.observation.ResourceCleanupObservation;
import com.flying.orm.rdb.observation.SqlExecutionObserver;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Parameters;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.SignalType;
import static com.flying.orm.core.internal.error.ThrowableGraph.addSuppressedIfAcyclic;
import static com.flying.orm.core.internal.error.ThrowableGraph.promoteVirtualMachineError;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * 统一维护一条 SQL 执行链路里的连接、Statement 和收尾动作。
 * 查询、更新和同连接序列都从这里获取并释放连接，统一处理取消和清理失败。
 * 实例只保存不可变依赖，可以被执行器并发复用；每次订阅的连接和行映射状态彼此隔离。
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class R2dbcExecutionSession {

    private final R2dbcConnectionAccess access;
    private final Connection boundConnection;
    private final R2dbcBindMarkers bindMarkers;
    private final SqlExecutionObserver observer;

    R2dbcExecutionSession(R2dbcConnectionAccess access, R2dbcBindMarkers bindMarkers,
                           SqlExecutionObserver observer, Connection boundConnection) {
        this.access = Objects.requireNonNull(access, "connection access must not be null");
        this.bindMarkers = Objects.requireNonNull(bindMarkers, "bind markers must not be null");
        this.observer = Objects.requireNonNull(observer, "observer must not be null");
        this.boundConnection = boundConnection;
    }

    Mono<Resources> acquireConnection(SqlRequest request) {
        return Mono.defer(() -> boundConnection == null
                ? Mono.from(Objects.requireNonNull(access.getConnection(request),
                        "connection publisher must not be null"))
                    .switchIfEmpty(Mono.error(new IllegalStateException("connection publisher completed empty")))
                    .map(connection -> new Resources(connection, request))
                : Mono.just(new Resources(boundConnection, request)));
    }

    <T> Mono<T> withConnection(SqlRequest request, Function<Resources, Mono<T>> work,
                               SqlExecutionOperation operation) {
        return Mono.usingWhen(acquireConnection(request),
                resource -> Mono.defer(() -> work.apply(resource)),
                resource -> release(resource, operation, SignalType.ON_COMPLETE, null),
                (resource, error) -> release(resource, operation, SignalType.ON_ERROR, error),
                resource -> release(resource, operation, SignalType.CANCEL, null))
                .onErrorMap(R2dbcExecutionSession::unwrapCleanupFailure);
    }

    /** Dispose statement-owned LOBs before invoking the upper release port, once per acquisition. */
    Mono<Void> release(Resources resource, SqlExecutionOperation operation,
                       SignalType signal, Throwable primary) {
        return resource.cleanupLargeObjects(signal, primary)
                .materialize().flatMap(cleanup -> {
                    Throwable lobError = cleanup.getThrowable();
                    Mono<Void> released = boundConnection == null
                            ? Mono.defer(() -> Mono.from(Objects.requireNonNull(
                                    access.releaseConnection(signal, resource.connection, resource.request),
                                    "release publisher must not be null")))
                            : Mono.empty();
                    return released.materialize().flatMap(release -> {
                        Throwable releaseError = release.getThrowable();
                        Throwable error = primary;
                        if (lobError != null) error = merge(error, lobError);
                        if (releaseError != null) error = merge(error, releaseError);
                        try {
                            if (lobError != null) observeCleanup(operation,
                                    ResourceCleanupObservation.Phase.LOB_CLEANUP, signal, lobError);
                            if (releaseError != null) observeCleanup(operation,
                                    ResourceCleanupObservation.Phase.CONNECTION_RELEASE, signal, releaseError);
                        } catch (Throwable observationFailure) {
                            error = merge(error, observationFailure);
                        }
                        return error == null || error == primary
                                ? Mono.empty() : Mono.error(error);
                    });
                });
    }

    static Mono<Void> cleanupLargeObjects(R2dbcLargeObjectScope scope,
                                          SignalType signal, Throwable primary) {
        if (scope == null) return Mono.empty();
        Mono<Void> cleanup = signal == SignalType.CANCEL ? scope.cancel()
                : primary == null ? scope.complete() : scope.error(primary);
        return cleanup.then(Mono.defer(() -> scope.cleanupFailure() == null
                ? Mono.empty() : Mono.error(scope.cleanupFailure())));
    }

    private void observeCleanup(SqlExecutionOperation operation,
                                ResourceCleanupObservation.Phase phase,
                                SignalType signal, Throwable error) {
        if (observer.enabled()) observer.onResourceCleanup(new ResourceCleanupObservation(
                operation, phase, signal == SignalType.ON_COMPLETE, error));
    }

    static Throwable merge(Throwable primary, Throwable secondary) {
        if (primary == null) return secondary;
        VirtualMachineError fatal = promoteVirtualMachineError(primary, secondary);
        if (fatal != null) return fatal;
        addSuppressedIfAcyclic(primary, secondary);
        return primary;
    }

    static Throwable unwrapCleanupFailure(Throwable error) {
        return error.getClass() == RuntimeException.class
                && error.getMessage() != null && error.getMessage().startsWith("Async resource cleanup failed")
                && error.getCause() != null ? error.getCause() : error;
    }

    <T> Flux<T> withStatement(SqlRequest request,
                              SqlExecutionOptions options,
                              SqlExecutionOperation operationType,
                              java.util.function.BiFunction<Statement,
                                      Supplier<R2dbcLargeObjectScope>, Publisher<T>> operation,
                              Function<Flux<T>, Flux<T>> operationProtection) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        return withPreparedStatement(safeRequest,
                                     snapshotExecutionParameters(safeRequest),
                                     options,
                                     operationType,
                                     operation,
                                     operationProtection);
    }

    <T> Flux<T> withPreparedStatement(SqlRequest request,
                                      List<Object> executionParameters,
                                      SqlExecutionOptions options,
                                      SqlExecutionOperation operationType,
                                      java.util.function.BiFunction<Statement,
                                              Supplier<R2dbcLargeObjectScope>, Publisher<T>> operation,
                                      Function<Flux<T>, Flux<T>> operationProtection) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        List<Object> safeParameters = Objects.requireNonNull(
                executionParameters, "execution parameters must not be null");
        Function<Flux<T>, Flux<T>> safeProtection = Objects.requireNonNull(
                operationProtection, "sql operation protection must not be null");
        return Flux.usingWhen(acquireConnection(safeRequest),
                              lease -> {
                                  Statement statement = prepareStatement(lease.connection(),
                                                                       safeRequest,
                                                                       safeParameters,
                                                                       options.fetchSize());
                                  return safeProtection.apply(Flux.from(
                                          operation.apply(statement, lease::largeObjects)));
                              },
                              lease -> release(lease, operationType, SignalType.ON_COMPLETE, null),
                              (lease, error) -> release(lease, operationType, SignalType.ON_ERROR, error),
                              lease -> release(lease, operationType, SignalType.CANCEL, null))
                .onErrorMap(R2dbcExecutionSession::unwrapCleanupFailure);
    }
    /** 只拥有 Statement/连接的资源边界；结果与失败证据由操作提供。 */
    <T> Mono<T> withPreparedStatementResource(SqlRequest request,
                                          List<Object> executionParameters,
                                          SqlExecutionOperation operationType,
                                          java.util.function.BiFunction<Statement,
                                                  Supplier<R2dbcLargeObjectScope>, Mono<T>> operation) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        List<Object> safeParameters = Objects.requireNonNull(
                executionParameters, "execution parameters must not be null");
        return Mono.usingWhen(acquireConnection(safeRequest),
                             lease -> {
                                 Statement statement = prepareStatement(lease.connection(),
                                                                      safeRequest,
                                                                      safeParameters,
                                                                      0);
                                 return operation.apply(statement, lease::largeObjects);
                             },
                             lease -> release(lease, operationType, SignalType.ON_COMPLETE, null),
                             (lease, error) -> release(lease, operationType, SignalType.ON_ERROR, error),
                             lease -> release(lease, operationType, SignalType.CANCEL, null))
                .onErrorMap(R2dbcExecutionSession::unwrapCleanupFailure);
    }
    Flux<DynamicRow> protectRows(Flux<DynamicRow> source, String sql, SqlExecutionOptions options) {
        return ReactiveSqlExecutionProtection.protectRows(
                source, sql, options, BatchMemoryBudget::estimateRowBytes);
    }

    /** SqlRequest 已拥有普通参数；这里只冻结 core 无法读取的 R2DBC 包装器载荷。 */
    static List<Object> snapshotExecutionParameters(SqlRequest request) {
        List<Object> source = OwnedBindableValues.ownedValues(
                Objects.requireNonNull(request, "sql request must not be null").parameters());
        List<Object> snapshot = null;
        for (int index = 0; index < source.size(); index++) {
            Object value = source.get(index);
            Object owned = R2dbcParameterValues.snapshotForExecution(value);
            if (snapshot != null) {
                snapshot.add(owned);
            } else if (owned != value) {
                snapshot = new ArrayList<>(source.size());
                snapshot.addAll(source.subList(0, index));
                snapshot.add(owned);
            }
        }
        return snapshot == null ? source : Collections.unmodifiableList(snapshot);
    }
    Statement prepareStatement(Connection connection,
                               SqlRequest request,
                               List<Object> parameters) {
        return prepareStatement(connection, request, parameters, 0);
    }

    private Statement prepareStatement(Connection connection,
                                       SqlRequest request,
                                       List<Object> parameters,
                                       int fetchSize) {
        SqlRequest safeRequest = Objects.requireNonNull(request, "sql request must not be null");
        Statement statement = connection.createStatement(bindMarkers.adapt(safeRequest));
        if (fetchSize > 0) {
            statement.fetchSize(fetchSize);
        }
        return bind(statement, parameters);
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

    static Publisher<DynamicRow> mapRows(Result result,
                                         SqlExecutionOptions options,
                                         R2dbcLargeObjectScope cleanupScope) {
        return R2dbcLargeObjectRows.map(result, options, cleanupScope);
    }

    static Publisher<DynamicRow> mapRows(Result result,
                                         SqlExecutionOptions options,
                                         Supplier<R2dbcLargeObjectScope> cleanupScope) {
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
            if (value instanceof SqlNullParameter typedNull) {
                statement.bindNull(i, typedNull.javaType());
            } else if (value == null) {
                Class<?> parameterType = parameterTypes == null ? Object.class : parameterTypes.get(i);
                if (parameterType == Object.class) {
                    statement.bind(i, Parameters.in(Object.class));
                } else {
                    statement.bindNull(i, parameterType);
                }
            } else {
                statement.bind(i, R2dbcParameterValues.forBinding(value));
            }
        }
        return statement;
    }

    /** Only the connection supplied for this operation and its owned LOB resources. */
    static final class Resources {
        private final Connection connection;
        private final SqlRequest request;
        private volatile R2dbcLargeObjectScope scope;
        private Mono<Void> lobCleanup;

        Resources(Connection connection, SqlRequest request) {
            this.connection = Objects.requireNonNull(connection, "connection must not be null");
            this.request = Objects.requireNonNull(request, "request must not be null");
        }

        Connection connection() { return connection; }

        R2dbcLargeObjectScope largeObjectsIfCreated() { return scope; }

        /** A release subscriber joins any already-started LOB cleanup before returning the connection. */
        synchronized Mono<Void> cleanupLargeObjects(SignalType signal, Throwable primary) {
            if (lobCleanup != null) return lobCleanup;
            R2dbcLargeObjectScope current = signal == SignalType.CANCEL ? largeObjects() : scope;
            if (current == null) return Mono.empty();
            lobCleanup = R2dbcExecutionSession.cleanupLargeObjects(current, signal, primary).cache();
            return lobCleanup;
        }

        synchronized void clearCleanedLargeObjects() {
            scope = null;
            lobCleanup = null;
        }

        R2dbcLargeObjectScope largeObjects() {
            R2dbcLargeObjectScope current = scope;
            if (current != null) return current;
            synchronized (this) {
                if (scope == null) scope = new R2dbcLargeObjectScope();
                return scope;
            }
        }
    }
}
