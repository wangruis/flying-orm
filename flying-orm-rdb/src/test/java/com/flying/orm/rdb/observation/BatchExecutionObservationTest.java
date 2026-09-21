package com.flying.orm.rdb.observation;

import com.flying.orm.rdb.batch.BatchAffectedRows;
import com.flying.orm.rdb.batch.BatchExecutionEvidence;
import com.flying.orm.rdb.batch.BatchExecutionState;
import com.flying.orm.rdb.batch.BatchRowConflict;
import com.flying.orm.rdb.exception.RdbErrorKind;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class BatchExecutionObservationTest {
    private static final int CLASSIFICATION_REQUEST_COUNT = 2_048;
    private volatile SqlStatementType statementTypeSink;

    @Test
    void representsProgressAndSummaryAsDistinctEventTypes() {
        var request = request("insert into events(id) values (?)");
        var evidence = success(2);
        var progress = BatchExecutionObservation.progress(request, evidence, 7L);
        var summary = BatchExecutionObservation.summary(request, evidence, 11L);
        assertEquals(BatchExecutionEventType.PROGRESS, progress.eventType());
        assertEquals(BatchExecutionEventType.SUMMARY, summary.eventType());
        assertEquals(BatchExecutionState.SUCCESS, summary.state());
        assertEquals(2L, progress.successfulCount());
        assertEquals(2L, summary.successfulCount());
    }

    @Test
    void keepsCumulativeSummaryMetricsNamedAndAligned() {
        var facts = new BatchExecutionEvidence.Accumulator();
        facts.accept(7);
        facts.succeeded(6, BatchAffectedRows.known(6));
        BitSet failed = new BitSet();
        failed.set(0);
        facts.terminal(1, new BitSet(), failed, BatchAffectedRows.known(0),
                       List.of(BatchRowConflict.exactlyOne(6, 0)));
        var summary = BatchExecutionObservation.summary(request("insert into events(id) values (?)"),
                facts.snapshot(BatchExecutionState.PARTIAL, null), 13L);
        assertEquals(BatchExecutionState.PARTIAL, summary.state());
        assertEquals(7L, summary.inputCount());
        assertEquals(BatchAffectedRows.known(6), summary.affectedRows());
        assertEquals(6L, summary.successfulCount());
        assertEquals(1L, summary.failedCount());
        assertEquals(1L, summary.conflictCount());
        assertEquals(SqlFailureCategory.OPTIMISTIC_LOCK, summary.failureCategory());
        assertEquals(13L, summary.durationNanos());
    }

    @Test
    void failureBeforeAnyFactsKeepsUnknownAffectedRowsAndNoProvenPositions() {
        TimeoutException timeout = new TimeoutException("upstream batch timed out");
        var failed = BatchExecutionObservation.failedSummary(
                request("insert into events values (?)"), 11L, timeout);
        assertEquals(BatchExecutionState.TIMED_OUT, failed.state());
        assertEquals(0L, failed.inputCount());
        assertEquals(BatchAffectedRows.unknown(), failed.affectedRows());
        assertEquals(0L, failed.successfulCount());
        assertEquals(0L, failed.failedCount());
        assertEquals(SqlFailureCategory.TIMEOUT, failed.failureCategory());
        assertEquals(BatchExecutionEvidence.Failure.from(timeout), failed.failure());
    }

    @Test
    void mixedSummaryRetainsFirstFailureWithoutCountingUnknownAsSuccessful() {
        var firstFailure = new BatchExecutionEvidence.Failure(
                "connection", "connection failed", "08006", 0, RdbErrorKind.CONNECTION);
        var facts = new BatchExecutionEvidence.Accumulator();
        facts.accept(5);
        facts.succeeded(2, BatchAffectedRows.known(2));
        facts.terminal(2, new BitSet(), new BitSet(), BatchAffectedRows.unknown(), List.of());
        var summary = BatchExecutionObservation.summary(request("insert into events values (?)"),
                facts.snapshot(BatchExecutionState.UNKNOWN, firstFailure), 13L);
        assertEquals(BatchExecutionState.UNKNOWN, summary.state());
        assertEquals(5L, summary.inputCount());
        assertEquals(BatchAffectedRows.unknown(), summary.affectedRows());
        assertEquals(2L, summary.successfulCount());
        assertEquals(0L, summary.failedCount());
        assertEquals(SqlFailureCategory.CONNECTION, summary.failureCategory());
        assertSame(firstFailure, summary.failure());
    }

    @Test
    void keepsStatementAndTerminalClassificationConsistentAcrossEvents() {
        var request = request("update events set value = ?");
        for (var event : List.of(progress(request), summary(request))) {
            assertEquals(SqlStatementType.UPDATE, event.statementType());
            assertEquals(SqlExecutionResultKind.SUCCESS, event.resultKind());
        }
    }

    @Test
    void keepsConcurrentProgressAndSummaryClassificationIndependent() throws Exception {
        List<Callable<Void>> classifications = new ArrayList<>();
        SqlStatementType[] expectedTypes = SqlStatementType.values();
        String[] statements = {
                "select value from events",
                "insert into events(value) values (?)",
                "update events set value = ?",
                "delete from events where id = ?",
                "merge into events using source on events.id = source.id",
                "create table events(id bigint)",
                "alter table events add value varchar(32)",
                "drop table events",
                "truncate table events",
                "explain select value from events"
        };
        for (int index = 0; index < 256; index++) {
            int statementIndex = index % statements.length;
            BatchExecutionObservation.BatchWriteRequestView request =
                    new BatchExecutionObservation.BatchWriteRequestView(
                            statements[statementIndex],
                            1,
                            SqlExecutionBackend.JDBC);
            BatchExecutionObservation chunk = progress(request);
            BatchExecutionObservation summary = summary(request);
            SqlStatementType expected = expectedTypes[statementIndex];
            classifications.add(() -> {
                assertEquals(expected, chunk.statementType());
                assertEquals(expected, summary.statementType());
                return null;
            });
        }

        try (var executor = Executors.newFixedThreadPool(8)) {
            for (var result : executor.invokeAll(classifications)) {
                result.get();
            }
        }
    }

    @Test
    void statementClassificationDoesNotAllocatePerRequestRegistryEntries() {
        java.lang.management.ThreadMXBean managementBean = ManagementFactory.getThreadMXBean();
        assumeTrue(managementBean instanceof com.sun.management.ThreadMXBean);
        com.sun.management.ThreadMXBean allocationBean = (com.sun.management.ThreadMXBean) managementBean;
        assumeTrue(allocationBean.isThreadAllocatedMemorySupported());
        if (!allocationBean.isThreadAllocatedMemoryEnabled()) {
            allocationBean.setThreadAllocatedMemoryEnabled(true);
        }

        BatchExecutionObservation warmup = progress(request("select warmup from events"));
        for (int iteration = 0; iteration < 1_000; iteration++) {
            statementTypeSink = SqlStatementType.fromSql(warmup.sql());
            statementTypeSink = warmup.statementType();
        }
        List<BatchExecutionObservation> chunks = IntStream.range(0, CLASSIFICATION_REQUEST_COUNT)
                .mapToObj(index -> progress(request("select value from events where marker = " + index)))
                .toList();

        long directBytes = allocatedBytes(allocationBean, chunks, false);
        long observationBytes = allocatedBytes(allocationBean, chunks, true);
        long registryBytes = observationBytes - directBytes;

        assertTrue(registryBytes < CLASSIFICATION_REQUEST_COUNT * 16L,
                   "statement classification allocated per-request registry entries: extra bytes="
                           + registryBytes);
    }


    @Test
    void buildsEventsFromAlreadyValidatedCumulativeEvidence() {
        var request = request("delete from events where id = ?");
        var event = BatchExecutionObservation.progress(request, success(9), 9L);
        assertSame(request, event.request());
        assertEquals(BatchExecutionState.SUCCESS, event.state());
        assertEquals(9L, event.inputCount());
        assertEquals(9L, event.successfulCount());
        assertEquals(BatchAffectedRows.known(9), event.affectedRows());
        assertEquals(SqlStatementType.DELETE, event.statementType());
    }

    @Test
    void keepsDirectConstructionValidated() {
        var request = request("insert into events(id) values (?)");
        assertThrows(IllegalArgumentException.class, () -> new BatchExecutionObservation(
                BatchExecutionEventType.PROGRESS, request, BatchExecutionState.SUCCESS,
                -1, BatchAffectedRows.known(1), 1, 0, 0, 1, SqlFailureCategory.NONE, null));
        assertThrows(IllegalArgumentException.class, () -> new BatchExecutionObservation(
                BatchExecutionEventType.PROGRESS, request, BatchExecutionState.SUCCESS,
                1, BatchAffectedRows.known(1), 1, 1, 0, 1, SqlFailureCategory.NONE, null));
    }

    private long allocatedBytes(com.sun.management.ThreadMXBean bean,
                                List<BatchExecutionObservation> events,
                                boolean throughObservation) {
        long threadId = Thread.currentThread().threadId();
        long before = bean.getThreadAllocatedBytes(threadId);
        for (BatchExecutionObservation event : events) {
            statementTypeSink = throughObservation ? event.statementType() : SqlStatementType.fromSql(event.sql());
        }
        return bean.getThreadAllocatedBytes(threadId) - before;
    }

    private static BatchExecutionObservation.BatchWriteRequestView request(String sql) {
        return new BatchExecutionObservation.BatchWriteRequestView(sql, 1, SqlExecutionBackend.JDBC);
    }

    private static BatchExecutionEvidence success(long count) {
        var facts = new BatchExecutionEvidence.Accumulator();
        facts.accept(count);
        facts.succeeded(count, BatchAffectedRows.known(count));
        return facts.snapshot(BatchExecutionState.SUCCESS, null);
    }

    private static BatchExecutionObservation progress(BatchExecutionObservation.BatchWriteRequestView request) {
        return BatchExecutionObservation.progress(request, success(1), 1L);
    }

    private static BatchExecutionObservation summary(BatchExecutionObservation.BatchWriteRequestView request) {
        return BatchExecutionObservation.summary(request, success(1), 1L);
    }
}
