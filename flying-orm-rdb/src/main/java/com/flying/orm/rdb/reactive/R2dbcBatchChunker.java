package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.batch.BatchWriteException;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.BatchRowSnapshotter;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongConsumer;

import static java.util.Objects.requireNonNull;

/**
 * Streams owned input rows into bounded R2DBC batch chunks.
 *
 * <p>This class owns only input numbering, snapshots and chunk memory limits. SQL execution stays
 * in {@link R2dbcBatchWriterChunks}, so batching policy and driver interaction remain separate.</p>
 *
 * @author wangr
 * @date 2026-08-24
 * @version v3.0
 */
final class R2dbcBatchChunker {

    private R2dbcBatchChunker() {
    }

    static Flux<R2dbcBatchWriterChunks.BatchChunk> chunks(BatchWriteRequest request) {
        return chunks(request, ignored -> {
        });
    }

    static Flux<R2dbcBatchWriterChunks.BatchChunk> chunks(BatchWriteRequest request,
                                                           LongConsumer acceptedRows) {
        LongConsumer safeAcceptedRows = requireNonNull(acceptedRows, "accepted row tracker must not be null");
        return Flux.defer(() -> {
            AtomicInteger chunkIndex = new AtomicInteger();
            long perChunkLimit = request.options().maxBufferedBytes() / request.options().concurrency();
            long maxRowBytes = request.options().maxRowBytes();
            ChunkAccumulator accumulator = new ChunkAccumulator(
                    request.options().chunkSize(), perChunkLimit, maxRowBytes);
            Flux<R2dbcBatchWriterChunks.BatchChunk> completed = rows(request)
                    .map(tuple -> {
                        IndexedRow row = IndexedRow.from(
                                tuple.getT1(), tuple.getT2(), request.parameterCount(),
                                maxRowBytes);
                        safeAcceptedRows.accept(tuple.getT1() + 1L);
                        return row;
                    })
                    .handle((row, sink) -> {
                        List<IndexedRow> chunk = accumulator.accept(row);
                        if (chunk != null) {
                            sink.next(toChunk(request, chunkIndex.getAndIncrement(), chunk));
                        }
                    });
            return completed.concatWith(Flux.defer(() -> {
                List<IndexedRow> last = accumulator.drain();
                return last == null
                        ? Flux.empty()
                        : Flux.just(toChunk(request, chunkIndex.getAndIncrement(), last));
            }));
        });
    }

    private static R2dbcBatchWriterChunks.BatchChunk toChunk(BatchWriteRequest request,
                                                               int chunkIndex,
                                                               List<IndexedRow> rows) {
        if (chunkIndex >= request.options().maxResultChunks()) {
            throw new BatchMemoryLimitExceededException(
                    "result chunks", request.options().maxResultChunks(), (long) chunkIndex + 1);
        }
        List<ProtectedBatchRows.RowView> rowViews = new ArrayList<>(rows.size());
        long estimatedBytes = 0L;
        for (IndexedRow row : rows) {
            ProtectedBatchRows.RowView rowView = row.rowView();
            rowViews.add(rowView);
            estimatedBytes += rowView.estimatedBytes();
        }
        return new R2dbcBatchWriterChunks.BatchChunk(
                chunkIndex, rows.getFirst().offset(), rowViews, estimatedBytes);
    }

    /** Stops consuming as soon as the configured row limit is exceeded. */
    private static Flux<Tuple2<Long, Object[]>> rows(BatchWriteRequest request) {
        long maxRows = request.options().maxRows();
        return Flux.from(request.rows())
                .onErrorMap(BatchWriteException.class, R2dbcBatchRowSourceFailure::new)
                .index()
                .handle((tuple, sink) -> {
                    if (maxRows > 0 && tuple.getT1() >= maxRows) {
                        sink.error(new R2dbcBatchRowLimitExceededException(tuple.getT1(), maxRows));
                    } else {
                        sink.next(tuple);
                    }
                });
    }

    private record IndexedRow(long offset, ProtectedBatchRows.RowView rowView) {

        private static IndexedRow from(long offset,
                                       Object[] row,
                                       int parameterCount,
                                       long maxRowBytes) {
            ProtectedBatchRows.RowView owned = BatchRowSnapshotter.snapshotView(
                    row, parameterCount, maxRowBytes, "row bytes");
            return new IndexedRow(offset, owned);
        }

        private IndexedRow {
            rowView = requireNonNull(rowView, "batch row view must not be null");
        }
    }

    /** 行数满或无法再为最大合法行预留空间时立即发布，不接纳下一分片的跨界行。 */
    private static final class ChunkAccumulator {
        private final int chunkSize;
        private final long drainThreshold;
        private List<IndexedRow> rows;
        private long bytes;

        private ChunkAccumulator(int chunkSize, long byteLimit, long maxRowBytes) {
            this.chunkSize = chunkSize;
            this.drainThreshold = byteLimit - maxRowBytes;
            this.rows = new ArrayList<>(Math.min(chunkSize, 16));
        }

        private List<IndexedRow> accept(IndexedRow row) {
            rows.add(row);
            bytes += row.rowView().estimatedBytes();
            return rows.size() == chunkSize || bytes > drainThreshold ? drain() : null;
        }

        private List<IndexedRow> drain() {
            if (rows.isEmpty()) {
                return null;
            }
            List<IndexedRow> completed = rows;
            rows = new ArrayList<>(Math.min(chunkSize, 16));
            bytes = 0L;
            return completed;
        }
    }
}
