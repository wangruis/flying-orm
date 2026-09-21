package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.execution.BatchRowSnapshotter;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import reactor.core.publisher.Flux;

import java.util.ArrayList;
import java.util.List;
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
        return chunks(request, rows(request, acceptedRows));
    }

    static Flux<Object[]> rows(BatchWriteRequest request, LongConsumer acceptedRows) {
        LongConsumer safeAcceptedRows = requireNonNull(acceptedRows, "accepted row tracker must not be null");
        long maxRows = request.options().maxRows();
        return Flux.from(request.rows())
                .onErrorMap(BatchExecutionEvidenceException.class, R2dbcBatchRowSourceFailure::new)
                .index()
                .handle((tuple, sink) -> {
                    if (maxRows > 0 && tuple.getT1() >= maxRows) {
                        sink.error(new R2dbcBatchRowLimitExceededException(tuple.getT1(), maxRows));
                    } else {
                        Object[] stableRow = R2dbcParameterValues.stabilizeBatchParameters(
                                tuple.getT2(), request.parameterCount());
                        safeAcceptedRows.accept(1L);
                        sink.next(stableRow);
                    }
                });
    }

    static Flux<R2dbcBatchWriterChunks.BatchChunk> chunks(BatchWriteRequest request,
                                                           Flux<Object[]> preparedRows) {
        Flux<Object[]> safeRows = requireNonNull(preparedRows, "prepared rows must not be null");
        return Flux.defer(() -> {
            long perChunkLimit = request.options().maxBufferedBytes();
            long maxRowBytes = request.options().maxRowBytes();
            ChunkAccumulator accumulator = new ChunkAccumulator(
                    request.options().bufferSize(), perChunkLimit, maxRowBytes);
            Flux<R2dbcBatchWriterChunks.BatchChunk> completed = safeRows.index()
                    .map(tuple -> {
                        IndexedRow row = IndexedRow.from(
                                tuple.getT1(), tuple.getT2(), request.parameterCount(),
                                maxRowBytes);
                        return row;
                    })
                    .handle((row, sink) -> {
                        List<IndexedRow> chunk = accumulator.accept(row);
                        if (chunk != null) {
                            sink.next(toChunk(request, accumulator.nextChunkIndex(), chunk));
                        }
                    });
            return completed.concatWith(Flux.defer(() -> {
                List<IndexedRow> last = accumulator.drain();
                return last == null
                        ? Flux.empty()
                        : Flux.just(toChunk(request, accumulator.nextChunkIndex(), last));
            }));
        });
    }

    private static R2dbcBatchWriterChunks.BatchChunk toChunk(BatchWriteRequest request,
                                                               int chunkIndex,
                                                               List<IndexedRow> rows) {
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

    /** 行数满或无法再为最大合法行预留空间时立即发布，不接纳下一缓冲的跨界行。 */
    private static final class ChunkAccumulator {
        private final int chunkSize;
        private final long drainThreshold;
        private List<IndexedRow> rows;
        private long bytes;
        private int chunkIndex;

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

        private int nextChunkIndex() {
            return chunkIndex++;
        }
    }
}
