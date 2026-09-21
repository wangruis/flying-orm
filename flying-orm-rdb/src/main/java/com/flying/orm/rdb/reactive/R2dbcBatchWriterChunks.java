package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.util.Collections;
import java.util.List;
import java.util.function.Supplier;

/** Executes one bounded ordinary input window on a supplied connection. */
final class R2dbcBatchWriterChunks {
    private final R2dbcBatchGeneratedKeyWriter keys = new R2dbcBatchGeneratedKeyWriter();
    private final R2dbcProtectedBatchSideIndex sideIndex;

    R2dbcBatchWriterChunks(R2dbcBindMarkers markers) {
        sideIndex = new R2dbcProtectedBatchSideIndex(markers);
    }

    Mono<Void> execute(Connection connection, BatchWriteRequest request, BatchChunk window,
                       String sql, Supplier<R2dbcLargeObjectScope> lobs,
                       R2dbcBatchEvidenceCounts facts) {
        return sideIndex.prepare(connection, request, window, lobs, facts)
                .flatMap(prepared -> {
                    if (request.generatedKeys().required()) {
                        return generated(connection, request, window, sql, lobs, facts, prepared);
                    }
                    Mono<Void> main;
                    if (sideIndex.hasOwnerRestrictedUpdates(prepared)) {
                        main = sideIndex.executeOwnerRestrictedUpdates(connection, request, window,
                                prepared, sql, facts::completeRow, facts);
                    } else if (request.rowCountPolicy() == BatchRowCountPolicy.EXACTLY_ONE
                            || containsLargeObject(window)) {
                        main = Flux.fromIterable(window.rows()).concatMap(row ->
                                executeOne(connection, request, row, sql, facts)
                                        .doOnNext(count -> {
                                            facts.completeRow(count);
                                            if (row.work() == null) facts.rowReady();
                                        }), 0).then();
                    } else {
                        main = executeDriverBatch(connection, request, window, sql, facts);
                    }
                    return main.then(Mono.defer(() -> facts.hasConflicts()
                            ? Mono.error(new IllegalStateException("batch row count did not match exactly-one policy"))
                            : sideIndex.complete(connection, prepared)));
                });
    }

    private Mono<Void> generated(Connection connection, BatchWriteRequest request, BatchChunk window,
                                  String sql, Supplier<R2dbcLargeObjectScope> lobs,
                                  R2dbcBatchEvidenceCounts facts,
                                  R2dbcProtectedBatchSideIndex.Prepared prepared) {
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
                                        : sideIndex.completeGeneratedRow(connection, prepared.rows().get(index), write);
                                // SQL success alone is not POST eligibility: keys and row-local work must finish.
                                return auxiliary.doOnSuccess(ignored -> facts.rowReady());
                            }), 0).then(Mono.defer(() -> facts.hasConflicts()
                                    ? Mono.error(new IllegalStateException("batch row count did not match exactly-one policy"))
                                    : tokens == null ? Mono.empty() : sideIndex.completeGeneratedRows(connection, tokens)));
        });
    }

    private Mono<Void> executeDriverBatch(Connection connection, BatchWriteRequest request,
                                          BatchChunk window, String sql, R2dbcBatchEvidenceCounts facts) {
        return Mono.defer(() -> {
            Statement statement = connection.createStatement(sql);
            for (int index = 0; index < window.rows().size(); index++) {
                bind(statement, request, window.rows().get(index));
                if (index + 1 < window.rows().size()) statement.add();
            }
            facts.startBusinessExecution();
            return Flux.from(statement.execute()).concatMap(Result::getRowsUpdated, 1)
                    .doOnNext(facts::record)
                    .then(Mono.fromRunnable(() -> {
                        facts.completeBusinessRows(window.rows().size());
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
            bind(statement, request, row);
            facts.startBusinessExecution();
            return Flux.from(statement.execute()).concatMap(Result::getRowsUpdated, 1)
                    .doOnNext(facts::recordRowCount)
                    .reduce(0L, (left, right) -> right < 0 ? left : Math.addExact(left, right));
        });
    }

    private static void bind(Statement statement, BatchWriteRequest request, ProtectedBatchRows.RowView row) {
        for (int index = 0; index < row.parameterCount(); index++) {
            Object value = row.row()[index];
            if (value == null) statement.bindNull(index, request.parameterTypes().get(index));
            else statement.bind(index, R2dbcParameterValues.forOwnedBinding(value));
        }
    }

    private static boolean containsLargeObject(BatchChunk window) {
        for (ProtectedBatchRows.RowView row : window.rows()) {
            for (int index = 0; index < row.parameterCount(); index++) {
                if (R2dbcParameterValues.isLargeObject(row.row()[index])) return true;
            }
        }
        return false;
    }

    record BatchChunk(int chunkIndex, long startOffset, List<ProtectedBatchRows.RowView> rows,
                      long estimatedBytes) {
        // Chunker transfers its private list; only the read-only view is published.
        BatchChunk { rows = Collections.unmodifiableList(rows); }
    }
}
