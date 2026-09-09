package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
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

import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Executes protected main-table and side-index work in the caller's external transaction. */
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
        return connections.acquireExternal().flatMap(resource -> Mono.usingWhen(
                Mono.just(resource),
                current -> execute(current, safeWork, safeOptions),
                current -> connections.closeAfterOutcome(current, SqlExecutionOperation.UPDATE),
                (current, ignored) -> connections.closeAfterOutcome(current, SqlExecutionOperation.UPDATE),
                current -> connections.cancel(current, "protected", SqlExecutionOperation.UPDATE))
                .onErrorMap(ReactiveSqlExecutionProtection::translate));
    }

    private Mono<SqlWriteResult> execute(R2dbcBatchConnectionHandle resource,
                                         ProtectedWriteWork work,
                                         SqlExecutionOptions options) {
        R2dbcGeneratedKeyWriter.Accumulator keys = work.requiresGeneratedKeys()
                ? new R2dbcGeneratedKeyWriter.Accumulator(options, resource::largeObjects) : null;
        Mono<SqlWriteResult> execution = readOwners(
                resource.connection(), work, options, resource.largeObjects())
                .flatMap(owners -> writeForOwners(resource.connection(), work, owners, keys)
                        .flatMap(result -> {
                            if (keys != null) {
                                keys.completeHandoff();
                            }
                            work.requireStableOwnerSet(owners, result);
                            return replaceTokens(resource.connection(), work, owners, result)
                                    .thenReturn(result);
                        }));
        return keys == null ? execution : execution.onErrorMap(keys::wrapFailure);
    }

    private Mono<List<Map<String, Object>>> readOwners(Connection connection,
                                                         ProtectedWriteWork work,
                                                         SqlExecutionOptions options,
                                                         R2dbcLargeObjectScope largeObjects) {
        if (work.kind() != ProtectedWriteWork.Kind.UPDATE) {
            return Mono.just(List.of(work.knownOwner()));
        }
        SqlRequest request = work.ownerQuery();
        SqlExecutionOptions ownerReadOptions = ProtectedWriteWork.ownerReadOptions(options);
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
}
