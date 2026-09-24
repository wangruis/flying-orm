package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.core.internal.value.OwnedBindableValues;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchMemoryBudget;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.internal.protection.ProtectedOwnerBatchPlan;
import com.flying.orm.rdb.internal.protection.ProtectedReplacementBatchPlan;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.AbstractList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.LongConsumer;
import java.util.function.Supplier;

/**
 * 在 R2DBC 批量缓冲的同一连接内读取 owner 并维护 CONTAINS 侧索引。
 *
 * <p>业务写入前为 UPDATE 锁定 owner 快照；业务写入确认后替换令牌。任何一步失败都继续进入
 * 普通失败处理与上层连接释放。</p>
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
                           Supplier<R2dbcLargeObjectScope> largeObjects, R2dbcBatchEvidenceCounts evidence) {
        return Mono.defer(() -> {
            if (chunk.rows().stream().noneMatch(row -> row.work() != null)) {
                return Mono.just(new Prepared(List.of(), request.options().maxBufferedBytes()));
            }
            SqlExecutionOptions options = ProtectedWriteWork.ownerReadOptions(
                    largeObjectOptions(request));
            List<RowState> states = new java.util.ArrayList<>(chunk.rows().size());
            for (ProtectedBatchRows.RowView row : chunk.rows()) {
                states.add(new RowState(row.work(), List.of()));
            }
            Iterable<ProtectedOwnerBatchPlan> plans = ProtectedOwnerBatchPlan.plans(
                    chunk.rows(), request.options().maxBufferedBytes());
            return Flux.fromIterable(plans)
                       .concatMap(plan -> readOwners(
                               connection, plan, states, options, largeObjects, evidence), 1)
                       .then(Mono.fromSupplier(() -> new Prepared(
                               states, request.options().maxBufferedBytes())));
        });
    }

    boolean hasOwnerRestrictedUpdates(Prepared prepared) {
        return prepared.rows().stream().map(RowState::work)
                .filter(Objects::nonNull)
                .anyMatch(work -> work.kind() == ProtectedWriteWork.Kind.UPDATE);
    }

    /**
     * owner 预读与业务更新之间可能发生并发行漂移；受保护 UPDATE 必须按行附加预读主键限制。
     * 未命中 owner 的行返回零影响，由上层 EXACTLY_ONE 事实中形成冲突。
     */
    Mono<Void> executeOwnerRestrictedUpdates(Connection connection,
                                                   BatchWriteRequest request,
                                                   R2dbcBatchWriterChunks.BatchChunk chunk,
                                                   Prepared prepared,
                                                   String transportSql,
                                                   LongConsumer completedRow, R2dbcBatchEvidenceCounts evidence) {
        return Flux.range(0, chunk.rows().size())
                   .concatMap(index -> executeOwnerRestrictedUpdate(
                           connection, request, chunk.rows().get(index), prepared.rows().get(index),
                           transportSql, evidence).doOnNext(count -> {
                               completedRow.accept(count);
                               if (chunk.rows().get(index).work() == null) evidence.rowReady();
                           }), 1)
                   .then();
    }

    private Mono<Long> executeOwnerRestrictedUpdate(Connection connection,
                                                     BatchWriteRequest request,
                                                     ProtectedBatchRows.RowView row,
                                                     RowState state,
                                                     String transportSql, R2dbcBatchEvidenceCounts evidence) {
        ProtectedWriteWork work = state.work();
        if (work == null) {
            return executeBusiness(connection, request, transportSql,
                                   java.util.Arrays.asList(row.row()).subList(0, row.parameterCount()),
                                   evidence);
        }
        if (work.kind() != ProtectedWriteWork.Kind.UPDATE) {
            return Mono.error(new IllegalArgumentException(
                    "protected batch work kind does not match update request"));
        }
        if (state.owners().isEmpty()) {
            evidence.recordRowCount(0L);
            return Mono.just(0L);
        }
        SqlRequest restricted = work.writeRequestForOwners(state.owners());
        return executeBusiness(connection, request,
                               bindMarkers.adapt(restricted.sql(), restricted.parameters().size(),
                                                 restricted.bindMarkerStyle()),
                               OwnedBindableValues.ownedValues(restricted.parameters()), evidence);
    }

    private Mono<Long> executeBusiness(Connection connection,
                                       BatchWriteRequest request,
                                       String sql, List<Object> parameters,
                                       R2dbcBatchEvidenceCounts evidence) {
        return Mono.defer(() -> {
            Statement statement = connection.createStatement(sql);
            for (int index = 0; index < parameters.size(); index++) {
                Object value = parameters.get(index);
                if (value == null) {
                    if (index >= request.parameterCount()) {
                        throw new IllegalArgumentException("protected write owner value must not be null");
                    }
                    statement.bindNull(index, request.parameterTypes().get(index));
                } else {
                    statement.bind(index, R2dbcParameterValues.forOwnedBinding(value));
                }
            }
            if (evidence != null) {
                evidence.startBusinessExecution();
            }
        return Flux.from(statement.execute())
                   .concatMap(Result::getRowsUpdated)
                   .doOnNext(count -> {
                       if (evidence != null) {
                           evidence.recordRowCount(count);
                       }
                   })
                   .reduce(0L, (left, right) -> right < 0 ? left : R2dbcExecutionCounts.add(left, right));
        });
    }

    Mono<Void> complete(Connection connection, Prepared prepared) {
        if (prepared.rows().isEmpty()) {
            return Mono.empty();
        }
        InsertBatchShape shape = insertBatchShape(prepared);
        if (shape != null) {
            return completeInsertChunk(connection, prepared, shape);
        }
        return completeReplacements(connection, prepared.rows(), prepared.maxBufferedBytes());
    }

    Mono<Void> replaceOwners(Connection connection, ProtectedWriteWork work,
                              List<Map<String, Object>> owners) {
        // 普通写入没有批量字节预算，仍按既有操作数和参数数量上限逐段执行。
        return completeReplacements(connection, List.of(new RowState(work, owners)), Long.MAX_VALUE);
    }

    Mono<Void> insertOwners(Connection connection, ProtectedWriteWork work,
                            List<Map<String, Object>> owners) {
        List<ProtectedReplacementBatchPlan.Insertion> insertions = new ArrayList<>();
        for (Map<String, Object> owner : owners) {
            addInsertions(insertions, work, owner);
        }
        return insertions(connection, insertions, Long.MAX_VALUE);
    }

    GeneratedTokenBatch generatedTokenBatch(Prepared prepared) {
        return insertBatchShape(prepared) == null ? null : new GeneratedTokenBatch(prepared.maxBufferedBytes());
    }

    Mono<Void> completeGeneratedRows(Connection connection, GeneratedTokenBatch batch) {
        return insertions(connection, batch.insertions, batch.maxBufferedBytes);
    }

    private Mono<Void> completeReplacements(Connection connection, List<RowState> rows, long maxBufferedBytes) {
        return Flux.fromIterable(ProtectedReplacementBatchPlan.segments(rows, maxBufferedBytes))
                   .concatMap(segment -> completeSegment(connection, segment, maxBufferedBytes), 0)
                   .then();
    }

    Mono<Void> completeGeneratedRow(Connection connection, RowState state,
                                    R2dbcBatchGeneratedKeyWriter.GeneratedWrite write, long maxBufferedBytes) {
        if (state.work() == null || write.affectedRows() == 0L) {
            return Mono.empty();
        }
        return replace(connection, state,
                       new SqlWriteResult(write.affectedRows(), List.of(write.generatedKey())), maxBufferedBytes);
    }

    private Mono<Void> readOwners(Connection connection,
                                  ProtectedOwnerBatchPlan plan,
                                  List<RowState> states,
                                  SqlExecutionOptions options,
                                  Supplier<R2dbcLargeObjectScope> largeObjects, R2dbcBatchEvidenceCounts evidence) {
        Statement statement = statement(
                connection, plan.sql(), SqlBindMarkerStyle.CANONICAL, plan.parameters());
        boolean[] matched = new boolean[plan.size()];
        if (evidence != null) {
            evidence.markDatabaseWorkAttempted();
        }
        return Flux.from(statement.execute())
                   .concatMap(result -> R2dbcExecutionSession.mapRows(result, options, largeObjects), 1)
                   .doOnNext(row -> {
                       ProtectedOwnerBatchPlan.Match match = plan.decode(row);
                       if (matched[match.slot()]) {
                           throw new IllegalStateException(
                                   "protected batch update owner query returned multiple rows");
                       }
                       matched[match.slot()] = true;
                       states.set(match.rowIndex(), new RowState(
                               states.get(match.rowIndex()).work(), List.of(match.owner())));
                   })
                   .then();
    }

    private static SqlExecutionOptions largeObjectOptions(BatchWriteRequest request) {
        long limit = request.options().maxBufferedBytes();
        return new SqlExecutionOptions(0, limit, limit, limit, 0);
    }

    private Mono<Void> replace(Connection connection, RowState state, SqlWriteResult result, long maxBufferedBytes) {
        ProtectedWriteWork work = state.work();
        if (work == null || result.affectedRows() == 0L) {
            return Mono.empty();
        }
        List<Map<String, Object>> owners = switch (work.kind()) {
            case INSERT -> List.of(work.resolveInsertOwner(result));
            case UPSERT -> List.of(work.knownOwner());
            case UPDATE -> state.owners();
        };
        if (owners.isEmpty()) {
            return Mono.error(new IllegalStateException("protected batch update owner was not found"));
        }
        return Flux.fromIterable(owners)
                   .concatMap(owner -> Flux.fromIterable(work.fields())
                           .concatMap(field -> replaceField(connection, work, owner, field, maxBufferedBytes), 0), 0)
                   .then();
    }

    private Mono<Void> completeSegment(Connection connection, ProtectedReplacementBatchPlan.Segment segment,
                                       long maxBufferedBytes) {
        Mono<Void> deletes;
        if (segment.deleteParameterSets().isEmpty()) {
            deletes = Mono.empty();
        } else {
            int parameterCount = segment.deleteParameterSets().getFirst().size();
            String deleteSql = bindMarkers.adapt(
                    segment.deleteSql(), parameterCount, SqlBindMarkerStyle.CANONICAL);
            deletes = R2dbcProtectedSideIndexDml.deleteParameterSets(
                    connection, deleteSql, segment.deleteParameterSets());
        }
        return deletes.then(insertions(connection, segment.insertions(), maxBufferedBytes));
    }

    private Mono<Void> insertions(Connection connection, List<ProtectedReplacementBatchPlan.Insertion> insertions,
                                  long maxBufferedBytes) {
        if (insertions.isEmpty()) {
            return Mono.empty();
        }
        ProtectedWriteWork work = insertions.getFirst().work();
        String insertSql = bindMarkers.adapt(
                work.insertSql(), work.ownerFields().size() + 2, SqlBindMarkerStyle.CANONICAL);
        return insertTokenBatches(connection, insertSql, insertions, maxBufferedBytes);
    }

    private Mono<Void> insertTokenBatches(Connection connection, String sql,
                                          Iterable<ProtectedReplacementBatchPlan.Insertion> insertions,
                                          long maxBufferedBytes) {
        Iterable<List<List<Object>>> batches = () -> new TokenBatchIterator(insertions.iterator(), maxBufferedBytes);
        return Flux.fromIterable(batches)
                   .concatMap(parameters -> R2dbcProtectedSideIndexDml.insertParameterSets(
                           connection, sql, parameters), 0)
                   .then();
    }

    private static void addInsertions(List<ProtectedReplacementBatchPlan.Insertion> insertions,
                                      ProtectedWriteWork work,
                                      Map<String, Object> owner) {
        for (ProtectedWriteWork.FieldTokens field : work.fields()) {
            if (field.tokenCount() > 0) {
                insertions.add(new ProtectedReplacementBatchPlan.Insertion(work, owner, field));
            }
        }
    }

    private InsertBatchShape insertBatchShape(Prepared prepared) {
        InsertBatchShape shape = null;
        for (RowState state : prepared.rows()) {
            ProtectedWriteWork work = state.work();
            if (work == null) {
                continue;
            }
            if (work.kind() != ProtectedWriteWork.Kind.INSERT) {
                return null;
            }
            InsertBatchShape next = new InsertBatchShape(
                    work.insertSql(), work.ownerFields().size() + 2);
            if (shape != null && !shape.equals(next)) {
                return null;
            }
            shape = next;
        }
        return shape;
    }

    private Mono<Void> completeInsertChunk(Connection connection,
                                           Prepared prepared,
                                           InsertBatchShape shape) {
        String sql = bindMarkers.adapt(
                shape.sql(), shape.parameterCount(), SqlBindMarkerStyle.CANONICAL);
        Iterable<ProtectedReplacementBatchPlan.Insertion> insertions = () -> prepared.rows().stream()
                .filter(state -> state.work() != null)
                .flatMap(state -> {
                    ProtectedWriteWork work = state.work();
                    Map<String, Object> owner = work.resolveInsertOwner(new SqlWriteResult(1L, List.of()));
                    return work.fields().stream().map(field ->
                            new ProtectedReplacementBatchPlan.Insertion(work, owner, field));
                }).iterator();
        return insertTokenBatches(connection, sql, insertions, prepared.maxBufferedBytes());
    }

    private Mono<Void> replaceField(Connection connection,
                                    ProtectedWriteWork work,
                                    Map<String, Object> owner,
                                    ProtectedWriteWork.FieldTokens field,
                                    long maxBufferedBytes) {
        Mono<Void> delete = work.kind() == ProtectedWriteWork.Kind.INSERT
                ? Mono.empty()
                : update(connection, work.deleteSql(), work.sideIndexParameters(owner, field, null)).then();
        return delete.then(insertions(connection,
                List.of(new ProtectedReplacementBatchPlan.Insertion(work, owner, field)), maxBufferedBytes));
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

    static final class GeneratedTokenBatch {
        private final List<ProtectedReplacementBatchPlan.Insertion> insertions = new ArrayList<>();
        private final long maxBufferedBytes;

        private GeneratedTokenBatch(long maxBufferedBytes) {
            this.maxBufferedBytes = maxBufferedBytes;
        }

        void add(RowState state, R2dbcBatchGeneratedKeyWriter.GeneratedWrite write) {
            if (state.work() == null || write.affectedRows() <= 0L) {
                return;
            }
            ProtectedWriteWork work = state.work();
            Map<String, Object> owner = work.resolveInsertOwner(
                    new SqlWriteResult(write.affectedRows(), List.of(write.generatedKey())));
            addInsertions(insertions, work, owner);
        }
    }

    /** Each subscription plans only the requested batch, checking a binding view before materializing it. */
    private static final class TokenBatchIterator implements Iterator<List<List<Object>>> {
        private final Iterator<ProtectedReplacementBatchPlan.Insertion> insertions;
        private final long maxBufferedBytes;
        private ProtectedReplacementBatchPlan.Insertion current;
        private int tokenIndex;

        private TokenBatchIterator(Iterator<ProtectedReplacementBatchPlan.Insertion> insertions,
                                   long maxBufferedBytes) {
            this.insertions = insertions;
            this.maxBufferedBytes = maxBufferedBytes;
        }

        @Override
        public boolean hasNext() {
            return advance() != null;
        }

        private ProtectedReplacementBatchPlan.Insertion advance() {
            while (current == null || tokenIndex >= current.field().tokenCount()) {
                if (!insertions.hasNext()) return null;
                current = insertions.next();
                tokenIndex = 0;
            }
            return current;
        }

        @Override
        public List<List<Object>> next() {
            ProtectedReplacementBatchPlan.Insertion insertion = advance();
            if (insertion == null) throw new NoSuchElementException();
            List<List<Object>> batch = new ArrayList<>();
            long bufferedBytes = 0L;
            long parameters = 0L;
            do {
                int parameterCount = insertion.work().ownerFields().size() + 2;
                if (!batch.isEmpty() && (batch.size() >= R2dbcProtectedSideIndexDml.MAX_TOKEN_BATCH_SIZE
                        || parameters + parameterCount > ProtectedReplacementBatchPlan.MAX_PARAMETERS)) {
                    break;
                }
                List<Object> view = parameterView(insertion, tokenIndex);
                long bytes = BatchMemoryBudget.estimateValueBytes(view);
                if (bytes == Long.MAX_VALUE || bytes > maxBufferedBytes) {
                    throw new IllegalArgumentException("protected side-index token parameters exceed batch safety limits");
                }
                if (!batch.isEmpty() && bytes > maxBufferedBytes - bufferedBytes) {
                    break;
                }
                batch.add(insertion.work().sideIndexParameters(insertion.owner(), insertion.field(), tokenIndex++));
                bufferedBytes += bytes;
                parameters += parameterCount;
                insertion = advance();
            } while (insertion != null);
            return batch;
        }

        private static List<Object> parameterView(ProtectedReplacementBatchPlan.Insertion insertion, int tokenIndex) {
            List<String> owners = insertion.work().ownerFields();
            return new AbstractList<>() {
                @Override
                public Object get(int index) {
                    Objects.checkIndex(index, size());
                    if (index < owners.size()) {
                        return Objects.requireNonNull(insertion.owner().get(owners.get(index)),
                                "protected write owner value must not be null");
                    }
                    return index == owners.size() ? insertion.field().fieldTag() : insertion.field().ownedToken(tokenIndex);
                }

                @Override
                public int size() {
                    return owners.size() + 2;
                }
            };
        }
    }

    record Prepared(List<RowState> rows, long maxBufferedBytes) {
        Prepared(List<RowState> rows) {
            this(rows, com.flying.orm.rdb.batch.BatchWriteOptions.DEFAULT_MAX_BUFFERED_BYTES);
        }

        Prepared {
            rows = List.copyOf(rows);
            if (maxBufferedBytes <= 0L) {
                throw new IllegalArgumentException("protected batch byte limit must be greater than zero");
            }
        }
    }

    record RowState(ProtectedWriteWork work, List<Map<String, Object>> owners)
            implements ProtectedReplacementBatchPlan.Row {
        RowState {
            owners = List.copyOf(owners);
        }
    }

    private record InsertBatchShape(String sql, int parameterCount) { }
}
