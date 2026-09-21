package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchExecutionEvidenceException;

/**
 * Distinguishes a public row-source error from executor-owned batch result
 * evidence.
 */
final class R2dbcBatchRowSourceFailure extends RuntimeException {

    private static final long serialVersionUID = 1L;

    R2dbcBatchRowSourceFailure(final BatchExecutionEvidenceException cause) {
        super("batch row publisher failed", cause);
    }
}
