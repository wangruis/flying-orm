package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.batch.BatchAffectedRows;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchRowConflict;
import com.flying.orm.rdb.batch.BatchRowCountPolicy;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

/** Bounded SQL facts for the current input window; cancellation snapshots share this monitor. */
final class R2dbcBatchEvidenceCounts {
    private final int size;
    private final long startOffset;
    private final boolean strict;
    private final boolean trackReadyRows;
    private final BitSet successful = new BitSet();
    private final BitSet failed = new BitSet();
    private final List<BatchRowConflict> conflicts = new ArrayList<>();
    private long affected;
    private boolean known = true;
    private boolean observed;
    private boolean unitKnown = true;
    private long unitAffected;
    private int completed;
    private boolean attempted;
    private boolean strictUnknown;
    private boolean businessInFlight;
    private boolean lastRowMatchesPolicy;
    // Complete row-local work; LOB cleanup remains a separate publication gate.
    private int readyPrefix;
    private BitSet sparseReady;

    R2dbcBatchEvidenceCounts(int size, long startOffset, BatchRowCountPolicy policy, boolean trackReadyRows) {
        this.size = size;
        this.startOffset = startOffset;
        this.strict = policy == BatchRowCountPolicy.EXACTLY_ONE;
        this.trackReadyRows = trackReadyRows;
    }

    synchronized void markDatabaseWorkAttempted() { attempted = true; }
    synchronized boolean databaseWorkAttempted() { return attempted; }

    synchronized void startBusinessExecution() {
        attempted = true;
        businessInFlight = true;
    }

    synchronized void record(long count) {
        observed = true;
        if (count < 0) unitKnown = false;
        else unitAffected = Math.addExact(unitAffected, count);
    }

    synchronized void recordRowCount(long count) { record(count); }

    synchronized void completeRow(long ignored) {
        boolean rowKnown = observed && unitKnown;
        long count = unitAffected;
        lastRowMatchesPolicy = !strict || rowKnown && count == 1;
        commitUnit();
        if (strict && rowKnown && count != 1) {
            failed.set(completed);
            conflicts.add(BatchRowConflict.exactlyOne(startOffset + completed, count));
        } else {
            successful.set(completed);
            if (strict && !rowKnown) strictUnknown = true;
        }
        completed++;
    }

    synchronized void completeBusinessRows(int count) {
        commitUnit();
        successful.set(0, count);
        completed = count;
    }

    private void commitUnit() {
        if (!observed || !unitKnown) known = false;
        else affected = Math.addExact(affected, unitAffected);
        observed = false;
        unitKnown = true;
        unitAffected = 0;
        businessInFlight = false;
    }

    synchronized boolean hasConflicts() { return !conflicts.isEmpty() || strictUnknown; }

    boolean tracksReadyRows() { return trackReadyRows; }

    synchronized void rowReady() { rowReady(completed - 1); }

    /** Called immediately after a row's work, or for ANY-policy rows in a completed driver batch. */
    synchronized void rowReady(int index) {
        if (!trackReadyRows || strict && !lastRowMatchesPolicy) return;
        if (sparseReady == null && index == readyPrefix) readyPrefix++;
        else {
            if (sparseReady == null) sparseReady = new BitSet(size);
            sparseReady.set(index);
        }
    }

    synchronized boolean hasReadyRows() { return readyPrefix > 0 || sparseReady != null && !sparseReady.isEmpty(); }

    synchronized boolean isRowReady(int index) { return index < readyPrefix || sparseReady != null && sparseReady.get(index); }

    synchronized void appendTo(BatchExecutionEvidence.Accumulator target, boolean windowComplete) {
        BatchAffectedRows rows = known && !businessInFlight
                ? BatchAffectedRows.known(affected) : BatchAffectedRows.unknown();
        if (windowComplete && !hasConflicts() && completed == size) target.succeeded(size, rows);
        else target.terminal(size, successful, failed, rows, conflicts);
    }
}
