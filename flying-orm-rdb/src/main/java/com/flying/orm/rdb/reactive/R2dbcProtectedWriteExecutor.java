package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * 在一条原生 R2DBC 连接上执行业务写入与 CONTAINS 侧索引维护。
 *
 * <p>整个工作单元保持冷订阅；自有连接由同一资源域 begin/commit/rollback，外部事务只借用连接。
 * 取消进入事务清理，提交或回滚无法确认时物理失效连接。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class R2dbcProtectedWriteExecutor {

    private final R2dbcBatchConnectionLifecycle connections;
    private final R2dbcExecutionSession session;

    R2dbcProtectedWriteExecutor(R2dbcBatchConnectionLifecycle connections,
                                R2dbcExecutionSession session) {
        this.connections = Objects.requireNonNull(connections, "R2DBC connection lifecycle must not be null");
        this.session = Objects.requireNonNull(session, "R2DBC execution session must not be null");
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
        Mono<SqlWriteResult> transaction = connections.begin(resource)
                .then(readOwners(resource.connection(), work, options, resource.largeObjects()))
                .flatMap(owners -> writeForOwners(
                        resource.connection(), work, owners, options, resource.largeObjects())
                        .flatMap(result -> requireStableOwnerSet(work, owners, result)
                                .then(replaceTokens(resource.connection(), work, owners, result, options))
                                .thenReturn(result)))
                .flatMap(result -> connections.commit(resource).thenReturn(result));
        // 超时必须发生在事务资源域内，BEGIN/COMMIT 回执丢失才能按状态升级为 UNKNOWN。
        return session.protectMono(transaction, options)
                      .onErrorResume(error -> recover(resource, error));
    }

    private Mono<List<Map<String, Object>>> readOwners(Connection connection,
                                                         ProtectedWriteWork work,
                                                         SqlExecutionOptions options,
                                                         R2dbcLargeObjectScope largeObjects) {
        if (work.kind() != ProtectedWriteWork.Kind.UPDATE) {
            return Mono.just(List.of(new LinkedHashMap<>(work.knownOwner())));
        }
        SqlRequest request = work.ownerQuery();
        Statement statement = session.prepareStatement(
                connection, request.sql(), request.parameters().size(),
                request.bindMarkerStyle(), request.parameters());
        Flux<DynamicRow> rows = Flux.from(statement.execute())
                   .concatMap(result -> R2dbcExecutionSession.mapRows(result, options, largeObjects), 1);
        return session.protectRows(rows, request.sql(), options)
                   .map(row -> owner(work.ownerFields(), row))
                   .collectList();
    }

    private Mono<SqlWriteResult> writeForOwners(Connection connection,
                                                 ProtectedWriteWork work,
                                                 List<Map<String, Object>> owners,
                                                 SqlExecutionOptions options,
                                                 R2dbcLargeObjectScope largeObjects) {
        if (work.kind() == ProtectedWriteWork.Kind.UPDATE && owners.isEmpty()) {
            return Mono.just(new SqlWriteResult(0L, List.of()));
        }
        SqlRequest request = work.kind() == ProtectedWriteWork.Kind.UPDATE
                ? work.writeRequestForOwners(owners) : work.writeRequest();
        return write(connection, work, request, options, largeObjects);
    }

    private Mono<SqlWriteResult> write(Connection connection,
                                       ProtectedWriteWork work,
                                       SqlRequest request,
                                       SqlExecutionOptions options,
                                       R2dbcLargeObjectScope largeObjects) {
        Statement statement = session.prepareStatement(
                connection, request.sql(), request.parameters().size(),
                request.bindMarkerStyle(), request.parameters());
        if (work.requiresGeneratedKeys()) {
            return R2dbcGeneratedKeyWriter.collect(
                    statement, options, largeObjects, generatedKeyColumn(work));
        }
        return Flux.from(statement.execute())
                   .flatMap(Result::getRowsUpdated)
                   .reduce(0L, R2dbcExecutionCounts::add)
                   .map(rows -> new SqlWriteResult(rows, List.of()));
    }

    private static String generatedKeyColumn(ProtectedWriteWork work) {
        Map<String, Object> knownOwner = work.knownOwner();
        List<String> missing = work.ownerFields().stream()
                                   .filter(field -> !knownOwner.containsKey(field) || knownOwner.get(field) == null)
                                   .toList();
        if (missing.size() != 1) {
            throw new IllegalArgumentException("protected insert requires exactly one database-generated owner field");
        }
        return missing.getFirst();
    }

    private static Mono<Void> requireStableOwnerSet(ProtectedWriteWork work,
                                                     List<Map<String, Object>> owners,
                                                     SqlWriteResult result) {
        return work.kind() == ProtectedWriteWork.Kind.UPDATE && result.affectedRows() != owners.size()
                ? Mono.error(new IllegalStateException("protected update row set changed concurrently"))
                : Mono.empty();
    }

    private Mono<Void> replaceTokens(Connection connection,
                                     ProtectedWriteWork work,
                                     List<Map<String, Object>> owners,
                                     SqlWriteResult result,
                                     SqlExecutionOptions options) {
        if (result.affectedRows() == 0L) {
            return Mono.empty();
        }
        List<Map<String, Object>> resolved = work.kind() == ProtectedWriteWork.Kind.INSERT
                ? List.of(resolveInsertOwner(work, result))
                : owners;
        return Flux.fromIterable(resolved)
                   .concatMap(owner -> Flux.fromIterable(work.fields())
                           .concatMap(field -> replaceField(connection, work, owner, field, options), 1), 1)
                   .then();
    }

    private Mono<Void> replaceField(Connection connection,
                                    ProtectedWriteWork work,
                                    Map<String, Object> owner,
                                    ProtectedWriteWork.FieldTokens field,
                                    SqlExecutionOptions options) {
        Mono<Void> delete = work.kind() == ProtectedWriteWork.Kind.INSERT
                ? Mono.empty()
                : update(connection, work.deleteSql(), ownerValues(work, owner, field, null), options).then();
        return delete.thenMany(Flux.fromIterable(field.tokens())
                                   .concatMap(token -> insertToken(
                                           connection, work.insertSql(),
                                           ownerValues(work, owner, field, token), options), 1))
                     .then();
    }

    /** 新令牌必须实际落库一行；零行或多行都不能与业务密文一起提交。 */
    private Mono<Long> insertToken(Connection connection,
                                   String sql,
                                   List<Object> parameters,
                                   SqlExecutionOptions options) {
        return update(connection, sql, parameters, options)
                .flatMap(rows -> rows == 1L
                        ? Mono.just(rows)
                        : Mono.error(new IllegalStateException(
                                "protected side index insert must affect one row")));
    }

    private Mono<Long> update(Connection connection,
                              String sql,
                              List<Object> parameters,
                              SqlExecutionOptions options) {
        Statement statement = session.prepareStatement(
                connection, sql, parameters.size(),
                com.flying.orm.core.sql.render.SqlBindMarkerStyle.CANONICAL, parameters);
        return Flux.from(statement.execute())
                   .flatMap(Result::getRowsUpdated)
                   .reduce(0L, R2dbcExecutionCounts::add);
    }

    private Mono<SqlWriteResult> recover(R2dbcBatchConnectionHandle resource, Throwable error) {
        if (connections.isExternal(resource)) {
            return Mono.error(error);
        }
        return switch (resource.state()) {
            case ACTIVE -> connections.rollback(resource)
                    .onErrorResume(rollbackFailure -> Mono.error(unknown(error, rollbackFailure)))
                    .then(Mono.error(error));
            case NEW, COMMITTING -> Mono.error(unknown(error, null));
            case COMMITTED, ROLLED_BACK -> Mono.error(error);
        };
    }

    private static Map<String, Object> resolveInsertOwner(ProtectedWriteWork work, SqlWriteResult result) {
        Map<String, Object> owner = new LinkedHashMap<>(work.knownOwner());
        List<String> missing = work.ownerFields().stream()
                .filter(field -> !owner.containsKey(field) || owner.get(field) == null)
                .toList();
        if (missing.isEmpty()) {
            return Map.copyOf(owner);
        }
        if (result.generatedKeys().size() != 1 || missing.size() != 1) {
            throw new IllegalStateException("protected insert did not return one complete owner key");
        }
        DynamicRow generated = result.generatedKeys().getFirst();
        Object value = generated.containsKey(missing.getFirst())
                ? generated.get(missing.getFirst())
                : generated.value(0);
        owner.put(missing.getFirst(), Objects.requireNonNull(
                value, "protected insert generated owner key must not be null"));
        return Map.copyOf(owner);
    }

    private static List<Object> ownerValues(ProtectedWriteWork work,
                                            Map<String, Object> owner,
                                            ProtectedWriteWork.FieldTokens field,
                                            byte[] token) {
        List<Object> values = new ArrayList<>(work.ownerFields().size() + 2);
        work.ownerFields().forEach(name -> values.add(Objects.requireNonNull(
                owner.get(name), "protected write owner value must not be null")));
        values.add(field.fieldTag());
        if (token != null) {
            values.add(token);
        }
        return values;
    }

    private static Map<String, Object> owner(List<String> fields, DynamicRow row) {
        Map<String, Object> owner = new LinkedHashMap<>();
        for (int index = 0; index < fields.size(); index++) {
            owner.put(fields.get(index), row.value(index));
        }
        return Map.copyOf(owner);
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
