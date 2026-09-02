package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchOptimisticLockException;
import com.flying.orm.rdb.batch.BatchRowConflict;

import java.util.List;
import java.util.Objects;

/** 携带输入分片和乐观锁冲突明细的响应式批量内部异常。 */
final class R2dbcBatchChunkConflictFailure extends RuntimeException {

    private static final long serialVersionUID = 1L;

    private final R2dbcBatchWriterChunks.BatchChunk chunk;

    private final List<BatchRowConflict> conflicts;

    R2dbcBatchChunkConflictFailure(R2dbcBatchWriterChunks.BatchChunk chunk, List<BatchRowConflict> conflicts) {
        super(new BatchOptimisticLockException(conflicts));
        this.chunk = Objects.requireNonNull(chunk, "batch conflict chunk must not be null");
        this.conflicts = List.copyOf(Objects.requireNonNull(conflicts, "batch row conflicts must not be null"));
    }

    R2dbcBatchWriterChunks.BatchChunk chunk() {
        return chunk;
    }

    List<BatchRowConflict> conflicts() {
        return conflicts;
    }
}
