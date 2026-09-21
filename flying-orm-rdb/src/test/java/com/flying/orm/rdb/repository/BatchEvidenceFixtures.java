package com.flying.orm.rdb.repository;

import com.flying.orm.rdb.batch.BatchAffectedRows;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionState;

/** Small execution-fact fixtures for Repository/Form contract tests. */
public final class BatchEvidenceFixtures {
    private BatchEvidenceFixtures() { }

    public static BatchExecutionEvidence successful(long rows) {
        BatchExecutionEvidence.Accumulator facts = new BatchExecutionEvidence.Accumulator();
        facts.accept(rows);
        facts.succeeded(rows, BatchAffectedRows.known(rows));
        return facts.snapshot(BatchExecutionState.SUCCESS, null);
    }
}
