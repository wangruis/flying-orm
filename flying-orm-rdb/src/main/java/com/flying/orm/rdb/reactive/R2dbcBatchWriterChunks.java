package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.batch.BatchRowConflict;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.exception.RdbExceptionTranslator;
import com.flying.orm.rdb.execution.BatchRowSnapshotter;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import io.r2dbc.spi.Connection;
import io.r2dbc.spi.Result;
import io.r2dbc.spi.Statement;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

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
    private final R2dbcBindMarkers bindMarkers;
    private final R2dbcBatchGeneratedKeyWriter generatedKeyWriter;
    private final R2dbcProtectedBatchSideIndex protectedSideIndex;
    R2dbcBatchWriterChunks(R2dbcBindMarkers bindMarkers) {
        this.bindMarkers = requireNonNull(bindMarkers, "r2dbc bind markers must not be null");
        this.generatedKeyWriter = new R2dbcBatchGeneratedKeyWriter(bindMarkers);
        this.protectedSideIndex = new R2dbcProtectedBatchSideIndex(bindMarkers);
    }

    /**
     * 以请求配置的 chunkSize 切分输入，同时在真正执行前拦住分片数量和在途内存超限的请求。
     *
     * <p>每个并发分片只分到总缓冲预算的一份，配合上游按需消费和 INDEPENDENT 的
     * {@code flatMap(..., prefetch=1)}，可以避免大批量数据先全部堆进内存。</p>
     *
     * @param request 批量请求
     * @return 保留原始全局偏移的分片流
     */
    Flux<BatchChunk> chunks(BatchWriteRequest request) {
        return Flux.defer(() -> {
            AtomicInteger chunkIndex = new AtomicInteger();
            AtomicInteger rowsInChunk = new AtomicInteger();
            AtomicLong bufferedBytes = new AtomicLong();
            long perChunkLimit = Math.max(1L,
                                          request.options().maxBufferedBytes() / request.options().concurrency());
            return rows(request)
                       .index()
                       .map(tuple -> IndexedRow.from(
                               tuple.getT1(), tuple.getT2(), request.parameterCount(), perChunkLimit))
                       // buffer 之前逐行检查，不能先积满一个超大分片才发现内存已经越界。
                       .bufferUntil(row -> reachesChunkBoundary(row,
                                                                 rowsInChunk,
                                                                 bufferedBytes,
                                                                 request.options().chunkSize(),
                                                                 perChunkLimit))
                       .map(rows -> {
                    int currentChunk = chunkIndex.getAndIncrement();
                    if (currentChunk >= request.options().maxResultChunks()) {
                        throw new BatchMemoryLimitExceededException(
                                "result chunks",
                                request.options().maxResultChunks(),
                                (long) currentChunk + 1);
                    }
                    List<Object[]> parameters = rows.stream().map(IndexedRow::parameters).toList();
                    List<Object[]> receiptParameters = rows.stream()
                            .map(row -> row.receiptParameters() == null
                                    ? row.parameters() : row.receiptParameters())
                            .toList();
                    List<ProtectedRow> protectedRows = rows.stream()
                            .map(row -> new ProtectedRow(row.work()))
                            .toList();
                    long estimatedBytes = rows.stream()
                            .mapToLong(IndexedRow::estimatedBytes)
                            .reduce(0L, R2dbcBatchWriterChunks::saturatedAdd);
                    return new BatchChunk(currentChunk, rows.getFirst().offset(), parameters, receiptParameters,
                                          protectedRows, estimatedBytes);
                       });
        });
    }

    private static boolean reachesChunkBoundary(IndexedRow row,
                                                AtomicInteger rowsInChunk,
                                                AtomicLong bufferedBytes,
                                                int chunkSize,
                                                long perChunkLimit) {
        long estimatedBytes = saturatedAdd(bufferedBytes.get(), row.estimatedBytes());
        if (estimatedBytes == Long.MAX_VALUE || estimatedBytes > perChunkLimit) {
            throw new BatchMemoryLimitExceededException(
                    "buffered bytes per concurrent chunk", perChunkLimit, estimatedBytes);
        }
        bufferedBytes.set(estimatedBytes);
        if (rowsInChunk.incrementAndGet() < chunkSize) {
            return false;
        }
        rowsInChunk.set(0);
        bufferedBytes.set(0L);
        return true;
    }

    /**
     * 在调用方已选定的连接和事务中执行一个分片。
     *
     * <p>严格影响行数策略必须逐行执行，才能把冲突对应到准确的输入偏移；含 LOB 的分片同样逐行执行，
     * 避开部分驱动在 {@link Statement#add()} 路径上丢失大字段语义的问题。其余请求走驱动批处理路径。</p>
     *
     * @param connection 当前事务连接
     * @param request    批量请求
     * @param chunk      待执行分片
     * @return 已提交 SQL 语句但尚未代表外层事务提交成功的分片结果
     */
    Mono<BatchChunkResult> executeChunk(R2dbcBatchConnectionHandle resource,
                                        BatchWriteRequest request,
                                        BatchChunk chunk) {
        Connection connection = resource.connection();
        return protectedSideIndex.prepare(connection, request, chunk, resource.largeObjects())
                .flatMap(prepared -> executeBusinessChunk(resource, request, chunk, prepared));
    }

    private Mono<BatchChunkResult> executeBusinessChunk(R2dbcBatchConnectionHandle resource,
                                                         BatchWriteRequest request,
                                                         BatchChunk chunk,
                                                         R2dbcProtectedBatchSideIndex.Prepared prepared) {
        Connection connection = resource.connection();
        if (request.generatedKeys().required()) {
            return executeGeneratedKeyChunk(resource, request, chunk, prepared);
        }
        Mono<BatchChunkResult> business;
        if (protectedSideIndex.hasOwnerRestrictedUpdates(prepared)) {
            business = executeOwnerRestrictedUpdateChunk(connection, request, chunk, prepared);
        } else if (request.rowCountPolicy() == BatchRowCountPolicy.EXACTLY_ONE) {
            business = executeExactlyOneChunk(connection, request, chunk);
        } else if (containsLargeObject(chunk)) {
            business = executeLargeObjectChunk(connection, request, chunk);
        } else {
            business = executeDriverBatch(connection, request, chunk);
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
                                                             R2dbcProtectedBatchSideIndex.Prepared prepared) {
        Connection connection = resource.connection();
        return Flux.range(0, chunk.rows().size())
                .concatMap(index -> generatedKeyWriter.write(connection,
                                                              request,
                                                              chunk.rows().get(index),
                                                              chunk.startOffset() + index,
                                                              resource.largeObjects())
                        .flatMap(write -> protectedSideIndex.completeGeneratedRow(
                                        connection, prepared.rows().get(index), write)
                                .thenReturn(new IndexedRowCount(index, write.affectedRows()))))
                .collectList()
                .flatMap(counts -> resultForRowCounts(request, chunk, counts))
                .onErrorMap(error -> error instanceof R2dbcBatchChunkConflictFailure
                        || error instanceof R2dbcBatchChunkWriteFailure
                        ? error
                        : new R2dbcBatchChunkWriteFailure(chunk, RdbExceptionTranslator.translate(error)));
    }

    /**
     * 边消费上游边检查输入行数。超过限制时立即让 Reactor 取消上游，不会为了计数而读取整批数据。
     */
    private Flux<Object[]> rows(BatchWriteRequest request) {
        long maxRows = request.options().maxRows();
        return Flux.from(request.rows())
                .index()
                .handle((tuple, sink) -> {
                    if (maxRows > 0 && tuple.getT1() >= maxRows) {
                        sink.error(new R2dbcBatchRowLimitExceededException(tuple.getT1(), maxRows));
                    } else {
                        sink.next(tuple.getT2());
                    }
                });
    }
    private static long saturatedAdd(long left, long right) {
        return Long.MAX_VALUE - left < right ? Long.MAX_VALUE : left + right;
    }

    /**
     * LOB 在部分 R2DBC 驱动的批处理实现中并不可靠，检测到后改走同连接的逐行绑定。
     */
    private boolean containsLargeObject(BatchChunk chunk) {
        for (Object[] row : chunk.rows()) {
            for (Object value : row) {
                if (R2dbcParameterValues.isLargeObject(value)) {
                    return true;
                }
            }
        }
        return false;
    }
    private Mono<BatchChunkResult> executeLargeObjectChunk(Connection connection,
                                                          BatchWriteRequest request,
                                                          BatchChunk chunk) {
        return Flux.fromIterable(chunk.rows())
                .concatMap(parameters -> executeSingleRow(connection, request, parameters))
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
                                                      BatchChunk chunk) {
        return Mono.defer(() -> {
            Statement statement = connection.createStatement(sqlForDriver(request.sql(),
                                                                         request.parameterCount(),
                                                                         request.bindMarkerStyle()));
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
                                                          BatchChunk chunk) {
        return Flux.range(0, chunk.rows().size())
                .concatMap(index -> executeSingleRow(connection, request, chunk.rows().get(index))
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
            R2dbcProtectedBatchSideIndex.Prepared prepared) {
        return protectedSideIndex.executeOwnerRestrictedUpdates(connection, request, chunk, prepared)
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
    private Mono<Long> executeSingleRow(Connection connection, BatchWriteRequest request, Object[] parameters) {
        return Mono.defer(() -> {
            Statement statement = connection.createStatement(sqlForDriver(request.sql(),
                                                                        request.parameterCount(),
                                                                        request.bindMarkerStyle()));
            bind(statement, request, parameters);
            return Flux.from(statement.execute())
                    .concatMap(Result::getRowsUpdated)
                    .reduce(0L, R2dbcExecutionCounts::add);
        });
    }

    /**
     * 每行绑定前先校验参数布局，避免错位值被驱动当成一条合法 SQL 执行。
     */
    private void bind(Statement statement, BatchWriteRequest request, Object[] parameters) {
        if (parameters.length != request.parameterCount()) {
            throw new IllegalArgumentException("batch row parameter count does not match request parameter count");
        }
        for (int i = 0; i < parameters.length; i++) {
            Object value = parameters[i];
            if (value == null) {
                statement.bindNull(i, request.parameterTypes().get(i));
            } else {
                statement.bind(i, R2dbcParameterValues.forBinding(value));
            }
        }
    }

    /**
     * 把统一 SQL 计划转换为当前 R2DBC 驱动能识别的绑定标记，不改变参数顺序。
     */
    private String sqlForDriver(String sql, int parameterCount, SqlBindMarkerStyle bindMarkerStyle) {
        return bindMarkers.adapt(sql, parameterCount, bindMarkerStyle);
    }

    /**
     * 一个待执行分片的最小不可变描述。行数组在接收 {@code onNext} 时已经复制，此处只冻结列表结构。
     */
    record BatchChunk(int chunkIndex,
                      long startOffset,
                      List<Object[]> rows,
                      List<Object[]> receiptRows,
                      List<ProtectedRow> protectedRows,
                      long estimatedBytes) {
        BatchChunk {
            rows = List.copyOf(rows);
            receiptRows = List.copyOf(receiptRows);
            protectedRows = List.copyOf(protectedRows);
            if (rows.size() != receiptRows.size() || rows.size() != protectedRows.size()) {
                throw new IllegalArgumentException("protected batch row metadata must match batch rows");
            }
        }
    }

    private record IndexedRow(long offset,
                              Object[] parameters,
                              Object[] receiptParameters,
                              ProtectedWriteWork work,
        long estimatedBytes) {
        private static IndexedRow from(long offset,
                                       Object[] row,
                                       int parameterCount,
                                       long perChunkLimit) {
            BatchRowSnapshotter.Snapshot owned = BatchRowSnapshotter.snapshotAndEstimate(
                    row, parameterCount, perChunkLimit, "buffered bytes per concurrent chunk");
            Object[] snapshot = owned.row();
            return new IndexedRow(offset,
                                  ProtectedBatchRows.parameters(snapshot, parameterCount),
                                  ProtectedBatchRows.receiptParameters(snapshot, parameterCount),
                                  ProtectedBatchRows.work(snapshot, parameterCount),
                                  owned.estimatedBytes());
        }

        private IndexedRow {
            // from 已在 onNext 当下取得独立所有权；这里不能再次复制普通批量行。
            parameters = requireNonNull(parameters, "batch row must not be null");
        }
    }
    record ProtectedRow(ProtectedWriteWork work) {
    }
    private record IndexedRowCount(int index, long affectedRows) {
    }
}
