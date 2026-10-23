package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;
import com.flying.orm.rdb.batch.BatchWriteRequest;
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

    static Flux<R2dbcBatchWriterChunks.BatchChunk> chunks(BatchWriteRequest request,
                                                         LongConsumer acceptedRows) {
        LongConsumer safeAcceptedRows = requireNonNull(acceptedRows, "accepted row tracker must not be null");
        return Flux.defer(() -> {
            ChunkAccumulator accumulator = new ChunkAccumulator(request);
            Flux<R2dbcBatchWriterChunks.BatchChunk> completed = Flux.from(request.rows())
                    .onErrorMap(BatchExecutionEvidenceException.class, R2dbcBatchRowSourceFailure::new)
                    .handle((row, sink) -> {
                        R2dbcBatchWriterChunks.BatchChunk chunk = accumulator.accept(row);
                        safeAcceptedRows.accept(1L);
                        if (chunk != null) {
                            sink.next(chunk);
                        }
                    });
            return completed.concatWith(Flux.defer(() -> {
                R2dbcBatchWriterChunks.BatchChunk last = accumulator.drain();
                return last == null ? Flux.empty() : Flux.just(last);
            }));
        });
    }

    /** 行数满或无法再为最大合法行预留空间时立即发布，不接纳下一缓冲的跨界行。 */
    private static final class ChunkAccumulator {
        private final int parameterCount;
        private final int chunkSize;
        private final long maxRows;
        private final long maxRowBytes;
        private final boolean bounded;
        private final long drainThreshold;
        private List<ProtectedBatchRows.RowView> rows;
        private long bytes;
        private long accepted;

        private ChunkAccumulator(BatchWriteRequest request) {
            this.parameterCount = request.parameterCount();
            this.chunkSize = request.options().bufferSize();
            this.maxRows = request.options().maxRows();
            this.maxRowBytes = request.options().maxRowBytes();
            long byteLimit = request.options().maxBufferedBytes();
            this.bounded = byteLimit != Long.MAX_VALUE;
            this.drainThreshold = byteLimit - maxRowBytes;
            this.rows = new ArrayList<>(Math.min(chunkSize, 16));
        }

        private R2dbcBatchWriterChunks.BatchChunk accept(Object[] values) {
            if (maxRows > 0 && accepted >= maxRows) {
                throw new R2dbcBatchRowLimitExceededException(accepted, maxRows);
            }
            Object[] stable = R2dbcParameterValues.stabilizeBatchParameters(values, parameterCount);
            ProtectedBatchRows.RowView row = ProtectedBatchRows.admit(
                    stable, parameterCount, maxRowBytes, "row bytes");
            rows.add(row);
            accepted++;
            if (bounded) bytes += row.estimatedBytes();
            return rows.size() == chunkSize || bounded && bytes > drainThreshold ? drain() : null;
        }

        private R2dbcBatchWriterChunks.BatchChunk drain() {
            if (rows.isEmpty()) {
                return null;
            }
            R2dbcBatchWriterChunks.BatchChunk completed =
                    new R2dbcBatchWriterChunks.BatchChunk(accepted - rows.size(), rows);
            rows = new ArrayList<>(Math.min(chunkSize, 16));
            bytes = 0L;
            return completed;
        }
    }
}
