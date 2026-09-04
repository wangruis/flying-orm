package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchAffectedRows;
import com.flying.orm.rdb.batch.BatchChunkExecutionFact;
import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchExecutionState;

import java.util.ArrayList;
import java.util.List;

/**
 * R2DBC evidence 分支的单分片计数状态。legacy 分支不创建该对象。
 */
final class R2dbcBatchEvidenceCounts {

    private boolean tracked;
    private boolean observed;
    private boolean known = true;
    private long affectedRows;
    private int successfulRows;
    private boolean databaseWorkAttempted;

    synchronized void markDatabaseWorkAttempted() {
        databaseWorkAttempted = true;
    }

    synchronized boolean databaseWorkAttempted() {
        return databaseWorkAttempted;
    }

    synchronized void trackCounts() {
        tracked = true;
    }

    synchronized void record(long count) {
        observed = true;
        if (count < 0L) {
            known = false;
            return;
        }
        affectedRows = R2dbcExecutionCounts.add(affectedRows, count);
    }

    /** 逐行路径只有在该行结果已完整返回后调用，因此可以保留精确成功前缀。 */
    synchronized void recordRow(long count) {
        tracked = true;
        successfulRows++;
        record(count);
    }

    synchronized void unknownAffectedRows() {
        tracked = true;
        known = false;
    }

    synchronized BatchAffectedRows affectedRows(long legacyAffectedRows) {
        if (!tracked) {
            return BatchAffectedRows.known(legacyAffectedRows);
        }
        return observed && known
                ? BatchAffectedRows.known(affectedRows)
                : BatchAffectedRows.unknown();
    }

    synchronized BatchChunkExecutionFact failureFact(
            R2dbcBatchWriterChunks.BatchChunk chunk,
            BatchExecutionState state,
            Throwable failure) {
        List<Long> successfulOffsets = new ArrayList<>(successfulRows);
        for (int index = 0; index < successfulRows; index++) {
            successfulOffsets.add(chunk.startOffset() + index);
        }
        BatchAffectedRows rows = successfulRows > 0 && observed && known
                ? BatchAffectedRows.known(affectedRows) : BatchAffectedRows.unknown();
        return BatchChunkExecutionFact.of(
                chunk.chunkIndex(),
                chunk.startOffset(),
                chunk.rows().size(),
                successfulOffsets,
                List.of(),
                state,
                rows,
                BatchChunkResult.Failure.from(failure));
    }
}
