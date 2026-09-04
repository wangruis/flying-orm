package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchChunkExecutionFact;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.exception.RdbErrorKind;
import com.flying.orm.rdb.exception.RdbException;
import com.flying.orm.rdb.exception.RdbExceptionTranslator;

import java.util.concurrent.CancellationException;
import java.util.concurrent.TimeoutException;

/** 把逐行路径已经形成的事实带过 Reactor 错误通道，不向公开异常契约泄漏内部类型。 */
final class R2dbcBatchEvidenceFailure extends RuntimeException {

    private final transient BatchChunkExecutionFact fact;

    private R2dbcBatchEvidenceFailure(BatchChunkExecutionFact fact, Throwable cause) {
        super(cause);
        this.fact = fact;
    }

    static R2dbcBatchEvidenceFailure from(R2dbcBatchWriterChunks.BatchChunk chunk,
                                          R2dbcBatchEvidenceCounts evidence,
                                          Throwable error) {
        Throwable classified = error instanceof R2dbcBatchChunkWriteFailure
                && error.getCause() != null ? error.getCause() : error;
        return new R2dbcBatchEvidenceFailure(
                evidence.failureFact(chunk, failureState(classified), classified), error);
    }

    BatchChunkExecutionFact fact() {
        return fact;
    }

    static BatchExecutionState failureState(Throwable failure) {
        RuntimeException translated = RdbExceptionTranslator.translate(failure);
        if (translated instanceof RdbException rdbFailure) {
            if (rdbFailure.kind() == RdbErrorKind.TIMEOUT
                    || rdbFailure.kind() == RdbErrorKind.LOCK_TIMEOUT) {
                return BatchExecutionState.TIMED_OUT;
            }
            if (rdbFailure.kind() == RdbErrorKind.CANCELLED) {
                return BatchExecutionState.CANCELLED;
            }
        }
        if (failure instanceof TimeoutException) {
            return BatchExecutionState.TIMED_OUT;
        }
        if (failure instanceof CancellationException) {
            return BatchExecutionState.CANCELLED;
        }
        return BatchExecutionState.FAILED;
    }
}
