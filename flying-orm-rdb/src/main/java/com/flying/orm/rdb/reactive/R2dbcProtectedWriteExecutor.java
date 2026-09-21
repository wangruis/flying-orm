package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.internal.value.OwnedBindableValues;
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
import java.util.function.LongConsumer;

/** Executes protected main-table and side-index work on the connection supplied by the caller. */
final class R2dbcProtectedWriteExecutor {

    private final R2dbcExecutionSession session;
    private final R2dbcProtectedBatchSideIndex sideIndex;

    R2dbcProtectedWriteExecutor(R2dbcExecutionSession session,
                                R2dbcBindMarkers bindMarkers) {
        this.session = Objects.requireNonNull(session, "R2DBC execution session must not be null");
        this.sideIndex = new R2dbcProtectedBatchSideIndex(bindMarkers);
    }

    Mono<SqlWriteResult> execute(ProtectedWriteWork work, SqlExecutionOptions options) {
        return execute(work, options, null);
    }

    Mono<SqlWriteResult> execute(ProtectedWriteWork work,
                                 SqlExecutionOptions options,
                                 LongConsumer confirmedRows) {
        ProtectedWriteWork safeWork = Objects.requireNonNull(work, "protected write work must not be null");
        SqlExecutionOptions safeOptions = Objects.requireNonNull(options, "sql execution options must not be null");
        SqlRequest first = safeWork.kind() == ProtectedWriteWork.Kind.UPDATE
                ? safeWork.ownerQuery() : safeWork.writeRequest();
        return session.withConnection(first,
                resource -> execute(resource, safeWork, safeOptions, confirmedRows),
                SqlExecutionOperation.UPDATE).onErrorMap(ReactiveSqlExecutionProtection::translate);
    }

    private Mono<SqlWriteResult> execute(R2dbcExecutionSession.Resources resource,
                                         ProtectedWriteWork work,
                                         SqlExecutionOptions options,
                                         LongConsumer confirmedRows) {
        R2dbcGeneratedKeyWriter.Accumulator keys = work.requiresGeneratedKeys()
                ? new R2dbcGeneratedKeyWriter.Accumulator(options, resource::largeObjects, confirmedRows) : null;
        Mono<SqlWriteResult> execution = readOwners(
                resource.connection(), work, options, resource.largeObjects())
                .flatMap(owners -> writeForOwners(resource.connection(), work, owners, keys, confirmedRows)
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
                connection, request, OwnedBindableValues.ownedValues(request.parameters()));
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
                                                 R2dbcGeneratedKeyWriter.Accumulator keys,
                                                 LongConsumer confirmedRows) {
        if (work.kind() == ProtectedWriteWork.Kind.UPDATE && owners.isEmpty()) {
            return Mono.just(new SqlWriteResult(0L, List.of()));
        }
        SqlRequest request = work.kind() == ProtectedWriteWork.Kind.UPDATE
                ? work.writeRequestForOwners(owners) : work.writeRequest();
        return write(connection, work, request, keys, confirmedRows);
    }

    private Mono<SqlWriteResult> write(Connection connection,
                                       ProtectedWriteWork work,
                                       SqlRequest request,
                                       R2dbcGeneratedKeyWriter.Accumulator keys,
                                       LongConsumer confirmedRows) {
        Statement statement = session.prepareStatement(
                connection, request, OwnedBindableValues.ownedValues(request.parameters()));
        if (work.requiresGeneratedKeys()) {
            return keys.collect(statement, work.generatedOwnerField());
        }
        return R2dbcExecutionCounts.sum(
                Flux.from(statement.execute()).flatMap(Result::getRowsUpdated), confirmedRows)
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
