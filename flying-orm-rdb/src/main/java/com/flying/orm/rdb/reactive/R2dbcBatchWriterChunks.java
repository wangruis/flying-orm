package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.internal.protection.ProtectedReplacementBatchPlan.Prepared;
import com.flying.orm.rdb.internal.plan.SqlExecutionStatements;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.Collections;
import java.util.BitSet;
import java.util.List;
import java.util.function.Supplier;
import java.util.function.IntFunction;

/** Executes one bounded ordinary input window on a supplied connection. */
final class R2dbcBatchWriterChunks {
    private final R2dbcBatchGeneratedKeyWriter keys = new R2dbcBatchGeneratedKeyWriter();
    private final R2dbcProtectedBatchSideIndex sideIndex;

    R2dbcBatchWriterChunks(R2dbcBindMarkers markers) {
        sideIndex = new R2dbcProtectedBatchSideIndex(markers);
    }

    Mono<Void> execute(Connection connection, BatchWriteRequest request, BatchChunk window,
                       String sql, IntFunction<String> insertSql, String upsertSql, Supplier<R2dbcLargeObjectScope> lobs,
                       R2dbcBatchEvidenceCounts facts) {
        return sideIndex.prepare(connection, request, window, lobs)
                .flatMap(prepared -> {
                    if (request.generatedKeys().required()) {
                        return generated(connection, request, window, sql, lobs, facts, prepared);
                    }
                    // 单行窗口的计数已能唯一对应输入，不需要再返回序号。
                    String returnedRowsSql = window.rows().size() == 1 || prepared.rows().isEmpty()
                            ? null : upsertSql;
                    boolean exactIgnoredRows = !prepared.rows().isEmpty()
                            && SqlExecutionStatements.ignoresConflicts(request.statement());
                    Mono<Void> main;
                    if (prepared.hasOwnerRestrictedUpdates()) {
                        main = sideIndex.executeOwnerRestrictedUpdates(connection, request, window,
                                prepared, sql, facts::completeRow, facts);
                    } else if (request.rowCountPolicy() == BatchRowCountPolicy.EXACTLY_ONE
                            || returnedRowsSql == null && exactIgnoredRows
                            // 零参数语句没有可重复的绑定组，不能依赖 Statement.add() 执行多行。
                            || request.parameterCount() == 0
                            || containsLargeObject(window)
                            // PostgreSQL 协议最多 65535 个参数；没有额外序号位置时沿用逐行计数。
                            || returnedRowsSql != null && request.parameterCount() >= 65_535) {
                        main = Flux.fromIterable(window.rows()).concatMap(row ->
                                executeOne(connection, request, row, sql, facts)
                                        .doOnNext(count -> {
                                            if (exactIgnoredRows) facts.requireKnownRowCount();
                                            facts.completeRow(count);
                                            if (row.work() == null) facts.rowReady();
                                        }), 0).then();
                    } else {
                        main = executeDriverBatch(connection, request, window, sql, insertSql, returnedRowsSql, facts);
                    }
                    return main.then(Mono.defer(() -> facts.hasConflicts()
                            ? Mono.error(new IllegalStateException("batch row count did not match exactly-one policy"))
                            : facts.hasNoAffectedRows() ? Mono.empty()
                                    : sideIndex.complete(connection, prepared, facts.zeroRows())));
                });
    }

    private Mono<Void> generated(Connection connection, BatchWriteRequest request, BatchChunk window,
                                  String sql, Supplier<R2dbcLargeObjectScope> lobs,
                                  R2dbcBatchEvidenceCounts facts,
                                  Prepared prepared) {
        return Mono.defer(() -> {
            R2dbcProtectedBatchSideIndex.GeneratedTokenBatch tokens = sideIndex.generatedTokenBatch(prepared);
            var options = R2dbcBatchGeneratedKeyWriter.largeObjectOptions(request);
            return Flux.range(0, window.rows().size()).concatMap(index ->
                    keys.write(connection, request, sql, window.rows().get(index),
                            window.startOffset() + index, lobs, options, facts)
                            .flatMap(write -> {
                                request.generatedKeys().accept(window.startOffset() + index, write.generatedKey());
                                if (tokens != null) {
                                    tokens.add(prepared.rows().get(index), write);
                                    if (window.rows().get(index).work() == null) facts.rowReady();
                                    return Mono.<Void>empty();
                                }
                                Mono<Void> auxiliary = prepared.rows().isEmpty() ? Mono.empty()
                                        : sideIndex.completeGeneratedRow(connection, prepared.rows().get(index), write,
                                                prepared.maxBufferedBytes());
                                // SQL success alone is not POST eligibility: keys and row-local work must finish.
                                return auxiliary.doOnSuccess(ignored -> facts.rowReady());
                            }), 0).then(Mono.defer(() -> facts.hasConflicts()
                                    ? Mono.error(new IllegalStateException("batch row count did not match exactly-one policy"))
                                    : tokens == null ? Mono.empty() : sideIndex.completeGeneratedRows(connection, tokens)));
        });
    }

    private Mono<Void> executeDriverBatch(Connection connection, BatchWriteRequest request,
                                          BatchChunk window, String sql, IntFunction<String> insertSql,
                                          String returnedRowsSql, R2dbcBatchEvidenceCounts facts) {
        return Mono.defer(() -> {
            String valuesSql = insertSql.apply(window.rows().size());
            Statement statement = connection.createStatement(returnedRowsSql != null ? returnedRowsSql
                    : valuesSql == null ? sql : valuesSql);
            for (int index = 0; index < window.rows().size(); index++) {
                R2dbcParameterValues.bindBatch(statement, request, window.rows().get(index),
                        valuesSql == null ? 0 : index * request.parameterCount());
                if (returnedRowsSql != null) statement.bind(request.parameterCount(), index);
                if (valuesSql == null && index + 1 < window.rows().size()) statement.add();
            }
            facts.startBusinessExecution();
            Mono<Void> execution;
            if (returnedRowsSql == null) {
                execution = Flux.from(statement.execute()).concatMap(Result::getRowsUpdated, 1)
                        .doOnNext(facts::record)
                        .then(Mono.fromRunnable(() -> facts.completeBusinessRows(window.rows().size())));
            } else {
                BitSet written = new BitSet(window.rows().size());
                execution = Flux.from(statement.execute()).concatMap(result -> result.map((row, metadata) -> {
                    Integer index = row.get(0, Integer.class);
                    if (index == null || index < 0 || index >= window.rows().size() || written.get(index)) {
                        throw new IllegalStateException("upsert returned an invalid batch input index");
                    }
                    written.set(index);
                    return index;
                }), 1).then(Mono.fromRunnable(() -> facts.completeReturnedRows(written)));
            }
            return execution
                    .then(Mono.fromRunnable(() -> {
                        if (facts.tracksReadyRows()) {
                            for (int index = 0; index < window.rows().size(); index++) {
                                if (window.rows().get(index).work() == null) facts.rowReady(index);
                            }
                        }
                    }));
        });
    }

    private Mono<Long> executeOne(Connection connection, BatchWriteRequest request,
                                  ProtectedBatchRows.RowView row, String sql, R2dbcBatchEvidenceCounts facts) {
        return Mono.defer(() -> {
            Statement statement = connection.createStatement(sql);
            R2dbcParameterValues.bindBatch(statement, request, row, 0);
            facts.startBusinessExecution();
            return Flux.from(statement.execute()).concatMap(Result::getRowsUpdated, 1)
                    .doOnNext(facts::record)
                    .reduce(0L, (left, right) -> right < 0 ? left : Math.addExact(left, right));
        });
    }

    private static boolean containsLargeObject(BatchChunk window) {
        for (ProtectedBatchRows.RowView row : window.rows()) {
            for (int index = 0; index < row.parameterCount(); index++) {
                if (R2dbcParameterValues.isLargeObject(row.row()[index])) return true;
            }
        }
        return false;
    }

    record BatchChunk(long startOffset, List<ProtectedBatchRows.RowView> rows) {
        // Chunker transfers its private list; only the read-only view is published.
        BatchChunk { rows = Collections.unmodifiableList(rows); }
    }
}
