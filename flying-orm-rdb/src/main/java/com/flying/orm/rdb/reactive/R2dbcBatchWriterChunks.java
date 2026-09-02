package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchRowConflict;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.exception.RdbExceptionTranslator;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongConsumer;

import static java.util.Objects.requireNonNull;

/**
 * R2DBC 批量写入中与单个分片有关的工作：从输入流切分数据、控制内存、绑定参数并执行 SQL。
 *
 * <p>这个类型不保存某次请求的行数据。每次订阅都会从 {@link BatchWriteRequest#rows()} 重新拉取输入，
 * 因而重试或重复订阅不会串行数据；真正的事务边界、ATOMIC/INDEPENDENT 结果汇总仍由写入器负责。</p>
 *
 * <p>分片前先给每一行编号，编号就是结果里的全局偏移。即使 INDEPENDENT 模式下各分片完成顺序不同，
 * 失败行、乐观锁冲突行和恢复信息也仍能准确指回原始输入。</p>
 *
 * @author wangr
 * @date 2026-08-06
 * @version v1.0
 */
final class R2dbcBatchWriterChunks {

    private final R2dbcBatchGeneratedKeyWriter generatedKeyWriter;
    private final R2dbcProtectedBatchSideIndex protectedSideIndex;

    R2dbcBatchWriterChunks(R2dbcBindMarkers bindMarkers) {
        R2dbcBindMarkers safeMarkers = requireNonNull(bindMarkers, "r2dbc bind markers must not be null");
        this.generatedKeyWriter = new R2dbcBatchGeneratedKeyWriter();
        this.protectedSideIndex = new R2dbcProtectedBatchSideIndex(safeMarkers);
    }

    /** Splits owned input rows without mixing chunk accounting into SQL execution. */
    Flux<BatchChunk> chunks(BatchWriteRequest request) {
        return R2dbcBatchChunker.chunks(request);
    }

    /** Splits rows while reporting how many inputs have been snapshotted at the execution boundary. */
    Flux<BatchChunk> chunks(BatchWriteRequest request, LongConsumer acceptedRows) {
        return R2dbcBatchChunker.chunks(request, acceptedRows);
    }

    /** 正常完成或失败会清除当前片；截止时间取消时保留它，供结果报告恢复已接收行数。 */
    static <T> Mono<T> trackActiveChunk(AtomicReference<BatchChunk> activeChunk,
                                        BatchChunk chunk,
                                        Mono<T> execution) {
        return Mono.defer(() -> {
            activeChunk.set(chunk);
            return execution
                    .doOnSuccess(ignored -> activeChunk.compareAndSet(chunk, null))
                    .doOnError(ignored -> activeChunk.compareAndSet(chunk, null));
        });
    }

    static Throwable timeoutFailure(AtomicReference<BatchChunk> activeChunk, TimeoutException error) {
        BatchChunk chunk = activeChunk.get();
        return chunk == null ? error : new R2dbcBatchChunkWriteFailure(chunk, error);
    }

    /**
     * 在调用方已选定的连接和事务中执行一个分片。
     *
     * <p>严格影响行数策略必须逐行执行，才能把冲突对应到准确的输入偏移；含 LOB 的分片同样逐行执行，
     * 避开部分驱动在 {@link Statement#add()} 路径上丢失大字段语义的问题。其余请求走驱动批处理路径。</p>
     *
     * @param resource 当前事务连接及其资源作用域
     * @param request    批量请求
     * @param chunk      待执行分片
     * @param transportSql 已在订阅边界按真实驱动编译的主业务 SQL
     * @return 已提交 SQL 语句但尚未代表外层事务提交成功的分片结果
     */
    Mono<BatchChunkResult> executeChunk(R2dbcBatchConnectionHandle resource,
                                        BatchWriteRequest request,
                                        BatchChunk chunk,
                                        String transportSql) {
        Connection connection = resource.connection();
        return protectedSideIndex.prepare(connection, request, chunk, resource::largeObjects)
                .flatMap(prepared -> executeBusinessChunk(
                        resource, request, chunk, prepared, transportSql))
                .onErrorMap(error -> chunkFailure(chunk, error));
    }

    private static Throwable chunkFailure(BatchChunk chunk, Throwable error) {
        if (error instanceof R2dbcBatchChunkWriteFailure
                || error instanceof R2dbcBatchChunkConflictFailure
                || error instanceof Error) {
            return error;
        }
        return new R2dbcBatchChunkWriteFailure(chunk, RdbExceptionTranslator.translate(error));
    }

    private Mono<BatchChunkResult> executeBusinessChunk(R2dbcBatchConnectionHandle resource,
                                                         BatchWriteRequest request,
                                                         BatchChunk chunk,
                                                         R2dbcProtectedBatchSideIndex.Prepared prepared,
                                                         String transportSql) {
        Connection connection = resource.connection();
        if (request.generatedKeys().required()) {
            return executeGeneratedKeyChunk(resource, request, chunk, prepared, transportSql);
        }
        Mono<BatchChunkResult> business;
        if (protectedSideIndex.hasOwnerRestrictedUpdates(prepared)) {
            business = executeOwnerRestrictedUpdateChunk(
                    connection, request, chunk, prepared, transportSql);
        } else if (request.rowCountPolicy() == BatchRowCountPolicy.EXACTLY_ONE) {
            business = executeExactlyOneChunk(connection, request, chunk, transportSql);
        } else if (containsLargeObject(chunk)) {
            business = executeLargeObjectChunk(connection, request, chunk, transportSql);
        } else {
            business = executeDriverBatch(connection, request, chunk, transportSql);
        }
        return business.flatMap(result -> protectedSideIndex.complete(connection, prepared, result)
                                                               .thenReturn(result));
    }

    /**
     * 数据库生成主键必须保持输入顺序逐行执行。每行拿到键后立刻回填，既不会错配实体，也不会积压整批键。
     */
    private Mono<BatchChunkResult> executeGeneratedKeyChunk(R2dbcBatchConnectionHandle resource,
                                                             BatchWriteRequest request,
                                                             BatchChunk chunk,
                                                             R2dbcProtectedBatchSideIndex.Prepared prepared,
                                                             String transportSql) {
        Connection connection = resource.connection();
        return Mono.defer(() -> {
            R2dbcProtectedBatchSideIndex.GeneratedTokenBatch generatedTokens =
                    protectedSideIndex.generatedTokenBatch(prepared);
            return Flux.range(0, chunk.rows().size())
                    .concatMap(index -> generatedKeyWriter.write(
                                    connection,
                                    request,
                                    transportSql,
                                    chunk.rows().get(index),
                                    chunk.startOffset() + index,
                                    resource::largeObjects)
                            .flatMap(write -> {
                                Mono<Void> sideIndex;
                                if (generatedTokens != null) {
                                    sideIndex = Mono.fromRunnable(
                                            () -> generatedTokens.add(
                                                    prepared.rows().get(index), write));
                                } else {
                                    sideIndex = prepared.rows().isEmpty()
                                            ? Mono.empty()
                                            : protectedSideIndex.completeGeneratedRow(
                                                    connection,
                                                    prepared.rows().get(index),
                                                    write);
                                }
                                return sideIndex.thenReturn(
                                        new IndexedRowCount(index, write.affectedRows()));
                            }))
                    .collectList()
                    .flatMap(counts -> {
                        Mono<Void> inserts = generatedTokens == null
                                ? Mono.empty()
                                : protectedSideIndex.completeGeneratedRows(
                                        connection, generatedTokens);
                        return inserts.then(Mono.defer(() ->
                                resultForRowCounts(request, chunk, counts)));
                    })
                    .onErrorMap(error -> error instanceof R2dbcBatchChunkConflictFailure
                            || error instanceof R2dbcBatchChunkWriteFailure
                            ? error
                            : new R2dbcBatchChunkWriteFailure(
                                    chunk, RdbExceptionTranslator.translate(error)));
        });
    }

    /**
     * LOB 在部分 R2DBC 驱动的批处理实现中并不可靠，检测到后改走同连接的逐行绑定。
     */
    private boolean containsLargeObject(BatchChunk chunk) {
        for (ProtectedBatchRows.RowView row : chunk.rows()) {
            for (int index = 0; index < row.parameterCount(); index++) {
                if (R2dbcParameterValues.isLargeObject(row.row()[index])) {
                    return true;
                }
            }
        }
        return false;
    }
    private Mono<BatchChunkResult> executeLargeObjectChunk(Connection connection,
                                                          BatchWriteRequest request,
                                                          BatchChunk chunk,
                                                          String transportSql) {
        return Flux.fromIterable(chunk.rows())
                .concatMap(parameters -> executeSingleRow(
                        connection, request, parameters, transportSql))
                .reduce(0L, R2dbcExecutionCounts::add)
                .map(affectedRows -> BatchChunkResult.committed(chunk.chunkIndex(),
                                                                chunk.startOffset(),
                                                                chunk.rows().size(),
                                                                affectedRows))
                .onErrorMap(error -> new R2dbcBatchChunkWriteFailure(chunk,
                                                                     RdbExceptionTranslator.translate(error)));
    }
    private Mono<BatchChunkResult> executeDriverBatch(Connection connection,
                                                      BatchWriteRequest request,
                                                      BatchChunk chunk,
                                                      String transportSql) {
        return Mono.defer(() -> {
            Statement statement = connection.createStatement(transportSql);
            for (int i = 0; i < chunk.rows().size(); i++) {
                bind(statement, request, chunk.rows().get(i));
                if (i < chunk.rows().size() - 1) {
                    statement.add();
                }
            }
            return Flux.from(statement.execute())
                    .flatMap(Result::getRowsUpdated)
                    .reduce(0L, R2dbcExecutionCounts::add)
                    .map(affectedRows -> BatchChunkResult.committed(chunk.chunkIndex(),
                                                                     chunk.startOffset(),
                                                                     chunk.rows().size(),
                                                                     affectedRows))
                    .onErrorMap(error -> new R2dbcBatchChunkWriteFailure(chunk,
                                                                         RdbExceptionTranslator.translate(error)));
        }).onErrorMap(error -> error instanceof R2dbcBatchChunkWriteFailure
                ? error
                : new R2dbcBatchChunkWriteFailure(chunk, RdbExceptionTranslator.translate(error)));
    }

    private Mono<BatchChunkResult> executeExactlyOneChunk(Connection connection,
                                                          BatchWriteRequest request,
                                                          BatchChunk chunk,
                                                          String transportSql) {
        return Flux.range(0, chunk.rows().size())
                .concatMap(index -> executeSingleRow(
                                connection, request, chunk.rows().get(index), transportSql)
                        .map(affectedRows -> new IndexedRowCount(index, affectedRows)))
                .collectList()
                .flatMap(counts -> resultForRowCounts(request, chunk, counts))
                .onErrorMap(error -> {
                    if (error instanceof R2dbcBatchChunkWriteFailure
                            || error instanceof R2dbcBatchChunkConflictFailure) {
                        return error;
                    }
                    return new R2dbcBatchChunkWriteFailure(chunk, RdbExceptionTranslator.translate(error));
                });
    }

    private Mono<BatchChunkResult> executeOwnerRestrictedUpdateChunk(
            Connection connection,
            BatchWriteRequest request,
            BatchChunk chunk,
            R2dbcProtectedBatchSideIndex.Prepared prepared,
            String transportSql) {
        return protectedSideIndex.executeOwnerRestrictedUpdates(
                        connection, request, chunk, prepared, transportSql)
                .map(counts -> java.util.stream.IntStream.range(0, counts.size())
                        .mapToObj(index -> new IndexedRowCount(index, counts.get(index))).toList())
                .flatMap(counts -> resultForRowCounts(request, chunk, counts))
                .onErrorMap(error -> {
                    if (error instanceof R2dbcBatchChunkWriteFailure
                            || error instanceof R2dbcBatchChunkConflictFailure) {
                        return error;
                    }
                    return new R2dbcBatchChunkWriteFailure(chunk, RdbExceptionTranslator.translate(error));
                });
    }

    private static Mono<BatchChunkResult> resultForRowCounts(BatchWriteRequest request,
                                                              BatchChunk chunk,
                                                              List<IndexedRowCount> counts) {
        if (request.rowCountPolicy() == BatchRowCountPolicy.EXACTLY_ONE) {
            List<BatchRowConflict> conflicts = counts.stream()
                    .filter(count -> count.affectedRows() != 1)
                    .map(count -> BatchRowConflict.exactlyOne(
                            chunk.startOffset() + count.index(),
                            count.affectedRows()))
                    .toList();
            if (!conflicts.isEmpty()) {
                return Mono.error(new R2dbcBatchChunkConflictFailure(chunk, conflicts));
            }
        }
        long affectedRows = R2dbcExecutionCounts.sum(counts.stream().mapToLong(IndexedRowCount::affectedRows));
        return Mono.just(BatchChunkResult.committed(chunk.chunkIndex(),
                                                   chunk.startOffset(),
                                                   chunk.rows().size(),
                                                   affectedRows));
    }

    /**
     * 单行执行是 LOB 和严格行数策略共用的底座，仍使用参数绑定，不能退回拼接 SQL。
     */
    private Mono<Long> executeSingleRow(Connection connection,
                                        BatchWriteRequest request,
                                        ProtectedBatchRows.RowView row,
                                        String transportSql) {
        return Mono.defer(() -> {
            Statement statement = connection.createStatement(transportSql);
            bind(statement, request, row);
            return Flux.from(statement.execute())
                    .concatMap(Result::getRowsUpdated)
                    .reduce(0L, R2dbcExecutionCounts::add);
        });
    }

    /**
     * 每行绑定前先校验参数布局，避免错位值被驱动当成一条合法 SQL 执行。
     */
    private void bind(Statement statement, BatchWriteRequest request, ProtectedBatchRows.RowView row) {
        for (int i = 0; i < row.parameterCount(); i++) {
            Object value = row.row()[i];
            if (value == null) {
                statement.bindNull(i, request.parameterTypes().get(i));
            } else {
                statement.bind(i, R2dbcParameterValues.forOwnedBinding(value));
            }
        }
    }

    /**
     * 一个待执行分片的最小不可变描述。行数组在接收 {@code onNext} 时已经复制，此处只冻结列表结构。
     */
    record BatchChunk(int chunkIndex,
                      long startOffset,
                      List<ProtectedBatchRows.RowView> rows,
                      long estimatedBytes) {
        BatchChunk {
            rows = List.copyOf(rows);
        }
    }

    private record IndexedRowCount(int index, long affectedRows) {
    }
}
