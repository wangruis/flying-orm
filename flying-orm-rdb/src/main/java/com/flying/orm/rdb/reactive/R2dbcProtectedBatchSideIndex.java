package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
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
 * 在 R2DBC 批量分片的同一事务连接内读取 owner 并维护 CONTAINS 侧索引。
 *
 * <p>业务写入前为 UPDATE 锁定 owner 快照；业务写入确认后、事务提交前替换令牌。任何一步失败都继续进入
 * 批量回滚和连接失效状态机，不能形成提交后的补偿写。</p>
 *
 * @author wangr
 * @date 2026-08-10
 * @version v1.0
 */
final class R2dbcProtectedBatchSideIndex {

    private final R2dbcBindMarkers bindMarkers;

    R2dbcProtectedBatchSideIndex(R2dbcBindMarkers bindMarkers) {
        this.bindMarkers = Objects.requireNonNull(bindMarkers, "r2dbc bind markers must not be null");
    }

    Mono<Prepared> prepare(Connection connection,
                           BatchWriteRequest request,
                           R2dbcBatchWriterChunks.BatchChunk chunk,
                           R2dbcLargeObjectScope largeObjects) {
        SqlExecutionOptions options = largeObjectOptions(request);
        return Flux.fromIterable(chunk.protectedRows())
                   .concatMap(row -> prepareRow(connection, row.work(), options, largeObjects), 1)
                   .collectList()
                   .map(Prepared::new);
    }

    boolean hasOwnerRestrictedUpdates(Prepared prepared) {
        return prepared.rows().stream().map(RowState::work)
                .filter(Objects::nonNull)
                .anyMatch(work -> work.kind() == ProtectedWriteWork.Kind.UPDATE);
    }

    /**
     * owner 预读与业务更新之间可能发生并发行漂移；受保护 UPDATE 必须按行附加预读主键限制。
     * 未命中 owner 的行返回零影响，由上层 EXACTLY_ONE 状态机形成冲突并回滚当前事务。
     */
    Mono<List<Long>> executeOwnerRestrictedUpdates(Connection connection,
                                                   BatchWriteRequest request,
                                                   R2dbcBatchWriterChunks.BatchChunk chunk,
                                                   Prepared prepared) {
        return Flux.range(0, chunk.rows().size())
                   .concatMap(index -> executeOwnerRestrictedUpdate(
                           connection, request, chunk.rows().get(index), prepared.rows().get(index)), 1)
                   .collectList();
    }

    private Mono<Long> executeOwnerRestrictedUpdate(Connection connection,
                                                    BatchWriteRequest request,
                                                    Object[] parameters,
                                                    RowState state) {
        ProtectedWriteWork work = state.work();
        if (work == null) {
            return executeBusiness(connection, request, request.sql(), request.bindMarkerStyle(),
                                   java.util.Arrays.asList(parameters));
        }
        if (work.kind() != ProtectedWriteWork.Kind.UPDATE) {
            return Mono.error(new IllegalArgumentException(
                    "protected batch work kind does not match update request"));
        }
        if (state.owners().isEmpty()) {
            return Mono.just(0L);
        }
        SqlRequest restricted = work.writeRequestForOwners(state.owners());
        return executeBusiness(connection, request, restricted.sql(), restricted.bindMarkerStyle(),
                               restricted.parameters());
    }

    private Mono<Long> executeBusiness(Connection connection,
                                       BatchWriteRequest request,
                                       String sql,
                                       SqlBindMarkerStyle style,
                                       List<Object> parameters) {
        return Mono.defer(() -> {
            Statement statement = connection.createStatement(bindMarkers.adapt(sql, parameters.size(), style));
            for (int index = 0; index < parameters.size(); index++) {
                Object value = parameters.get(index);
                if (value == null) {
                    if (index >= request.parameterCount()) {
                        throw new IllegalArgumentException("protected write owner value must not be null");
                    }
                    statement.bindNull(index, request.parameterTypes().get(index));
                } else {
                    statement.bind(index, R2dbcParameterValues.forBinding(value));
                }
            }
            return Flux.from(statement.execute())
                       .concatMap(Result::getRowsUpdated)
                       .reduce(0L, R2dbcExecutionCounts::add);
        });
    }

    Mono<Void> complete(Connection connection, Prepared prepared, BatchChunkResult result) {
        if (result.status() != BatchChunkResult.Status.COMMITTED) {
            return Mono.empty();
        }
        return Flux.fromIterable(prepared.rows())
                   .concatMap(state -> replace(connection, state, new SqlWriteResult(1L, List.of())), 1)
                   .then();
    }

    Mono<Void> completeGeneratedRow(Connection connection,
                                    RowState state,
                                    R2dbcBatchGeneratedKeyWriter.GeneratedWrite write) {
        if (state.work() == null || write.affectedRows() == 0L) {
            return Mono.empty();
        }
        return replace(connection, state,
                       new SqlWriteResult(write.affectedRows(), List.of(write.generatedKey())));
    }

    private Mono<RowState> prepareRow(Connection connection,
                                      ProtectedWriteWork work,
                                      SqlExecutionOptions options,
                                      R2dbcLargeObjectScope largeObjects) {
        if (work == null || work.kind() != ProtectedWriteWork.Kind.UPDATE) {
            return Mono.just(new RowState(work, List.of()));
        }
        SqlRequest query = Objects.requireNonNull(work.ownerQuery(),
                                                  "protected batch owner query must not be null");
        Statement statement = statement(connection, query.sql(), query.bindMarkerStyle(), query.parameters());
        return Flux.from(statement.execute())
                   .concatMap(result -> R2dbcExecutionSession.mapRows(result, options, largeObjects), 1)
                   .map(row -> owner(work.ownerFields(), row))
                   .take(2)
                   .collectList()
                   .flatMap(owners -> owners.size() > 1
                           ? Mono.error(new IllegalStateException(
                                   "protected batch update owner query returned multiple rows"))
                           : Mono.just(new RowState(work, owners)));
    }

    private static SqlExecutionOptions largeObjectOptions(BatchWriteRequest request) {
        long limit = request.options().maxBufferedBytes();
        return SqlExecutionOptions.safeDefaults()
                                  .withTimeout(request.options().timeout())
                                  .withMaxResultBytes(limit)
                                  .withMaxLargeObjectBytes(limit)
                                  .withMaxLargeObjectChars(limit);
    }

    private Mono<Void> replace(Connection connection, RowState state, SqlWriteResult result) {
        ProtectedWriteWork work = state.work();
        if (work == null || result.affectedRows() == 0L) {
            return Mono.empty();
        }
        List<Map<String, Object>> owners = switch (work.kind()) {
            case INSERT -> List.of(resolveInsertOwner(work, result));
            case UPSERT -> List.of(work.knownOwner());
            case UPDATE -> state.owners();
        };
        if (owners.isEmpty()) {
            return Mono.error(new IllegalStateException("protected batch update owner was not found"));
        }
        return Flux.fromIterable(owners)
                   .concatMap(owner -> Flux.fromIterable(work.fields())
                           .concatMap(field -> replaceField(connection, work, owner, field), 1), 1)
                   .then();
    }

    private Mono<Void> replaceField(Connection connection,
                                    ProtectedWriteWork work,
                                    Map<String, Object> owner,
                                    ProtectedWriteWork.FieldTokens field) {
        Mono<Void> delete = work.kind() == ProtectedWriteWork.Kind.INSERT
                ? Mono.empty()
                : update(connection, work.deleteSql(), values(work, owner, field, null)).then();
        return delete.thenMany(Flux.fromIterable(field.tokens())
                                   .concatMap(token -> insertToken(
                                           connection, work.insertSql(), values(work, owner, field, token)), 1))
                     .then();
    }

    /** 新令牌必须实际落库一行；零行或多行都必须让当前批量事务失败。 */
    private Mono<Long> insertToken(Connection connection, String sql, List<Object> parameters) {
        return update(connection, sql, parameters)
                .flatMap(rows -> rows == 1L
                        ? Mono.just(rows)
                        : Mono.error(new IllegalStateException(
                                "protected side index insert must affect one row")));
    }

    private Mono<Long> update(Connection connection, String sql, List<Object> parameters) {
        Statement statement = statement(connection, sql, SqlBindMarkerStyle.CANONICAL, parameters);
        return Flux.from(statement.execute())
                   .flatMap(Result::getRowsUpdated)
                   .reduce(0L, R2dbcExecutionCounts::add);
    }

    private Statement statement(Connection connection,
                                String sql,
                                SqlBindMarkerStyle style,
                                List<Object> parameters) {
        Statement statement = connection.createStatement(bindMarkers.adapt(sql, parameters.size(), style));
        for (int index = 0; index < parameters.size(); index++) {
            Object value = parameters.get(index);
            if (value == null) {
                statement.bindNull(index, Object.class);
            } else {
                statement.bind(index, R2dbcParameterValues.forBinding(value));
            }
        }
        return statement;
    }

    private static Map<String, Object> resolveInsertOwner(ProtectedWriteWork work, SqlWriteResult result) {
        Map<String, Object> owner = new LinkedHashMap<>(work.knownOwner());
        List<String> missing = work.ownerFields().stream()
                                   .filter(field -> !owner.containsKey(field) || owner.get(field) == null)
                                   .toList();
        if (missing.isEmpty()) {
            return Map.copyOf(owner);
        }
        if (missing.size() != 1 || result.generatedKeys().size() != 1) {
            throw new IllegalStateException("protected batch insert did not return one complete owner key");
        }
        DynamicRow key = result.generatedKeys().getFirst();
        Object value = key.containsKey(missing.getFirst()) ? key.get(missing.getFirst()) : key.value(0);
        owner.put(missing.getFirst(), Objects.requireNonNull(
                value, "protected batch generated owner key must not be null"));
        return Map.copyOf(owner);
    }

    private static Map<String, Object> owner(List<String> fields, DynamicRow row) {
        Map<String, Object> owner = new LinkedHashMap<>();
        for (int index = 0; index < fields.size(); index++) {
            owner.put(fields.get(index), row.value(index));
        }
        return Map.copyOf(owner);
    }

    private static List<Object> values(ProtectedWriteWork work,
                                       Map<String, Object> owner,
                                       ProtectedWriteWork.FieldTokens field,
                                       byte[] token) {
        List<Object> values = new ArrayList<>(work.ownerFields().size() + 2);
        work.ownerFields().forEach(name -> values.add(Objects.requireNonNull(
                owner.get(name), "protected batch owner value must not be null")));
        values.add(field.fieldTag());
        if (token != null) {
            values.add(token);
        }
        return values;
    }

    record Prepared(List<RowState> rows) {
        Prepared {
            rows = List.copyOf(rows);
        }
    }

    record RowState(ProtectedWriteWork work, List<Map<String, Object>> owners) {
        RowState {
            owners = List.copyOf(owners);
        }
    }
}
