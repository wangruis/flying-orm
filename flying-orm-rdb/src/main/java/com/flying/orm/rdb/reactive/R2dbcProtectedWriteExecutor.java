package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.execution.GeneratedKeyReadException;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.observation.SqlExecutionOperation;
import com.flying.orm.rdb.result.DynamicRow;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 在一条原生 R2DBC 连接上执行业务写入与 CONTAINS 侧索引维护。
 *
 * <p>整个工作单元保持冷订阅；自有连接由同一资源域 begin/commit/rollback，外部事务只借用连接。
 * 取消进入事务清理；提交或回滚无法确认时返回 UNKNOWN，并按标准连接边界归还租约。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class R2dbcProtectedWriteExecutor {

    private final R2dbcBatchConnectionLifecycle connections;
    private final R2dbcExecutionSession session;
    private final R2dbcProtectedBatchSideIndex sideIndex;

    R2dbcProtectedWriteExecutor(R2dbcBatchConnectionLifecycle connections,
                                R2dbcExecutionSession session,
                                R2dbcBindMarkers bindMarkers) {
        this.connections = Objects.requireNonNull(connections, "R2DBC connection lifecycle must not be null");
        this.session = Objects.requireNonNull(session, "R2DBC execution session must not be null");
        this.sideIndex = new R2dbcProtectedBatchSideIndex(bindMarkers);
    }

    Mono<SqlWriteResult> execute(ProtectedWriteWork work, SqlExecutionOptions options) {
        ProtectedWriteWork safeWork = Objects.requireNonNull(work, "protected write work must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        BatchWriteOptions transactionOptions = BatchWriteOptions.defaults()
                .withTimeout(safeOptions.timeout());
        Mono<SqlWriteResult> source = Mono.usingWhen(
                connections.acquire(transactionOptions, safeOptions.cleanupTimeout()),
                resource -> execute(resource, safeWork, safeOptions),
                resource -> connections.closeAfterOutcome(resource, SqlExecutionOperation.UPDATE),
                (resource, ignored) -> connections.closeAfterOutcome(resource, SqlExecutionOperation.UPDATE),
                resource -> connections.cancel(resource, "protected", SqlExecutionOperation.UPDATE));
        return source.onErrorMap(ReactiveSqlExecutionProtection::translate);
    }

    private Mono<SqlWriteResult> execute(R2dbcBatchConnectionHandle resource,
                                         ProtectedWriteWork work,
                                         SqlExecutionOptions options) {
        R2dbcGeneratedKeyWriter.Accumulator keys = work.requiresGeneratedKeys()
                ? new R2dbcGeneratedKeyWriter.Accumulator(options, resource::largeObjects) : null;
        Mono<SqlWriteResult> transaction = connections.begin(resource)
                .then(readOwners(resource.connection(), work, options, resource.largeObjects()))
                .flatMap(owners -> writeForOwners(
                        resource.connection(), work, owners, keys)
                        .flatMap(result -> {
                            if (keys != null) {
                                keys.completeHandoff();
                            }
                            work.requireStableOwnerSet(owners, result);
                            return replaceTokens(resource.connection(), work, owners, result)
                                    .thenReturn(result);
                        }))
                .flatMap(result -> connections.commit(resource).thenReturn(result));
        // 超时必须发生在事务资源域内，BEGIN/COMMIT 回执丢失才能按状态升级为 UNKNOWN。
        Mono<SqlWriteResult> execution = session.protectMono(transaction, options);
        if (keys != null) {
            // 先保存截止前已观察到的写入，再由原事务状态裁决回滚或外部参与事实。
            execution = execution.onErrorMap(keys::wrapFailure);
        }
        return execution.onErrorResume(error -> recover(resource, error));
    }

    private Mono<List<Map<String, Object>>> readOwners(Connection connection,
                                                         ProtectedWriteWork work,
                                                         SqlExecutionOptions options,
                                                         R2dbcLargeObjectScope largeObjects) {
        if (work.kind() != ProtectedWriteWork.Kind.UPDATE) {
            return Mono.just(List.of(work.knownOwner()));
        }
        SqlRequest request = work.ownerQuery();
        // 外层事务已统一限制执行时间，这里只保留 owner 结果预算，避免为同次更新再创建超时任务。
        SqlExecutionOptions ownerReadOptions = ProtectedWriteWork.ownerReadOptions(
                options.timeout().isZero() ? options : options.withTimeout(Duration.ZERO));
        Statement statement = session.prepareStatement(
                connection, request, request.parameters());
        Flux<DynamicRow> rows = Flux.from(statement.execute())
                   .concatMap(result -> R2dbcExecutionSession.mapRows(
                           result, ownerReadOptions, largeObjects), 1);
        return session.protectRows(rows, request.sql(), ownerReadOptions)
                   .map(work::ownerFrom)
                   .collectList();
    }

    private Mono<SqlWriteResult> writeForOwners(Connection connection,
                                                 ProtectedWriteWork work,
                                                 List<Map<String, Object>> owners,
                                                 R2dbcGeneratedKeyWriter.Accumulator keys) {
        if (work.kind() == ProtectedWriteWork.Kind.UPDATE && owners.isEmpty()) {
            return Mono.just(new SqlWriteResult(0L, List.of()));
        }
        SqlRequest request = work.kind() == ProtectedWriteWork.Kind.UPDATE
                ? work.writeRequestForOwners(owners) : work.writeRequest();
        return write(connection, work, request, keys);
    }

    private Mono<SqlWriteResult> write(Connection connection,
                                       ProtectedWriteWork work,
                                       SqlRequest request,
                                       R2dbcGeneratedKeyWriter.Accumulator keys) {
        Statement statement = session.prepareStatement(
                connection, request, request.parameters());
        if (work.requiresGeneratedKeys()) {
            return keys.collect(statement, work.generatedOwnerField());
        }
        return Flux.from(statement.execute())
                   .flatMap(Result::getRowsUpdated)
                   .reduce(0L, R2dbcExecutionCounts::add)
                   .map(rows -> new SqlWriteResult(rows, List.of()));
    }

    private Mono<Void> replaceTokens(Connection connection,
                                     ProtectedWriteWork work,
                                     List<Map<String, Object>> owners,
                                     SqlWriteResult result) {
        if (result.affectedRows() == 0L) {
            return Mono.empty();
        }
        if (work.kind() != ProtectedWriteWork.Kind.INSERT) {
            return sideIndex.replaceOwners(connection, work, owners);
        }
        Map<String, Object> owner = work.resolveInsertOwner(result);
        return sideIndex.insertOwners(connection, work, List.of(owner));
    }

    private Mono<SqlWriteResult> recover(R2dbcBatchConnectionHandle resource, Throwable error) {
        if (connections.isExternal(resource)) {
            return Mono.error(error);
        }
        return switch (resource.state()) {
            case ACTIVE -> connections.rollback(resource)
                    .onErrorResume(rollbackFailure -> Mono.error(rollbackUnknown(error, rollbackFailure)))
                    .then(Mono.defer(() -> Mono.error(confirmedRollbackFailure(error))));
            case NEW, COMMITTING, ROLLING_BACK -> Mono.error(unknown(error, null));
            case COMMITTED, ROLLED_BACK -> Mono.error(error);
        };
    }

    private static Throwable confirmedRollbackFailure(Throwable error) {
        return error instanceof GeneratedKeyReadException keyFailure ? keyFailure.getCause() : error;
    }

    private static Throwable rollbackUnknown(Throwable error, Throwable rollbackFailure) {
        RdbException uncertainty = unknown(error, rollbackFailure);
        return error instanceof GeneratedKeyReadException keyFailure
                ? new GeneratedKeyReadException(keyFailure.affectedRows(), uncertainty)
                : uncertainty;
    }

    private static RdbException unknown(Throwable primary, Throwable cleanup) {
        Throwable cause = cleanup == null ? primary : cleanup;
        RdbException unknown = new RdbException(RdbErrorKind.UNKNOWN, "protected write outcome is unknown",
                                                null, null, cause);
        if (cleanup != null && primary != cleanup) {
            // unknown 尚未发布，附加业务失败不会形成回指环，同时保留回滚失败中的致命错误图。
            unknown.addSuppressed(primary);
        }
        return unknown;
    }
}
