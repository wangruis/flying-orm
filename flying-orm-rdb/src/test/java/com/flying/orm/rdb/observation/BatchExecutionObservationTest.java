package com.flying.orm.rdb.observation;

import com.flying.orm.rdb.batch.BatchChunkResult;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteResult;
import org.junit.jupiter.api.Test;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

class BatchExecutionObservationTest {

    private static final int CLASSIFICATION_REQUEST_COUNT = 2_048;

    private volatile SqlStatementType statementTypeSink;

    @Test
    void representsChunkAndSummaryAsDistinctEventTypes() {
        BatchExecutionObservation.BatchWriteRequestView request =
                new BatchExecutionObservation.BatchWriteRequestView(
                        "insert into events(id) values (?)",
                        BatchWriteOptions.Mode.ATOMIC,
                        1,
                        SqlExecutionBackend.R2DBC);
        BatchChunkResult committed = BatchChunkResult.committed(0, 0L, 2, 2L);

        BatchExecutionObservation chunk = BatchExecutionObservation.chunk(request, committed, 7L);
        BatchExecutionObservation summary = BatchExecutionObservation.summary(
                request,
                BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, List.of(committed)),
                11L);

        BatchExecutionObservation.Chunk chunkEvent =
                assertInstanceOf(BatchExecutionObservation.Chunk.class, chunk);
        BatchExecutionObservation.Summary summaryEvent =
                assertInstanceOf(BatchExecutionObservation.Summary.class, summary);
        assertEquals(BatchChunkResult.Status.COMMITTED, chunkEvent.status());
        assertEquals(BatchWriteResult.Status.COMMITTED, summaryEvent.status());
        assertEquals(1L, summaryEvent.chunkCount());
    }

    @Test
    void keepsStreamedSummaryMetricsNamedAndAligned() {
        BatchExecutionObservation.BatchWriteRequestView request =
                new BatchExecutionObservation.BatchWriteRequestView(
                        "insert into events(id) values (?)",
                        BatchWriteOptions.Mode.INDEPENDENT,
                        1,
                        SqlExecutionBackend.R2DBC);
        BatchSummaryMetrics metrics = new BatchSummaryMetrics(
                BatchWriteResult.Status.PARTIAL,
                7L,
                6L,
                1L,
                3L,
                2L,
                1L,
                null,
                null);

        BatchExecutionObservation.Summary summary = assertInstanceOf(
                BatchExecutionObservation.Summary.class,
                BatchExecutionObservationFactory.summary(request, metrics, 13L));

        assertEquals(BatchWriteResult.Status.PARTIAL, summary.status());
        assertEquals(7L, summary.inputCount());
        assertEquals(6L, summary.affectedRows());
        assertEquals(3L, summary.chunkCount());
        assertEquals(2L, summary.successfulChunkCount());
        assertEquals(1L, summary.failedChunkCount());
        assertEquals(SqlFailureCategory.OPTIMISTIC_LOCK, summary.failureCategory());
        assertEquals(13L, summary.durationNanos());
    }

    @Test
    void keepsStatementAndTerminalClassificationConsistentAcrossEvents() {
        BatchExecutionObservation.BatchWriteRequestView request =
                new BatchExecutionObservation.BatchWriteRequestView(
                        "update events set value = ?",
                        BatchWriteOptions.Mode.ATOMIC,
                        1,
                        SqlExecutionBackend.JDBC);
        BatchChunkResult committed = BatchChunkResult.committed(0, 0L, 1, 1L);
        BatchExecutionObservation chunk = BatchExecutionObservation.chunk(request, committed, 3L);
        BatchExecutionObservation summary = BatchExecutionObservation.summary(
                request,
                BatchWriteResult.from(BatchWriteOptions.Mode.ATOMIC, List.of(committed)),
                5L);

        assertEquals(SqlStatementType.UPDATE, chunk.statementType());
        assertEquals(SqlStatementType.UPDATE, summary.statementType());
        assertEquals(SqlExecutionResultKind.SUCCESS, chunk.resultKind());
        assertEquals(SqlExecutionResultKind.SUCCESS, summary.resultKind());
    }

    @Test
    void keepsConcurrentChunkAndSummaryClassificationIndependent() throws Exception {
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
                            BatchWriteOptions.Mode.ATOMIC,
                            1,
                            SqlExecutionBackend.JDBC);
            BatchExecutionObservation.Chunk chunk = chunk(request);
            BatchExecutionObservation.Summary summary = summary(request);
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

        BatchExecutionObservation.Chunk warmup = chunk(request("select warmup from events"));
        for (int iteration = 0; iteration < 1_000; iteration++) {
            statementTypeSink = SqlStatementType.fromSql(warmup.sql());
            statementTypeSink = warmup.statementType();
        }
        List<BatchExecutionObservation.Chunk> chunks = IntStream.range(0, CLASSIFICATION_REQUEST_COUNT)
                .mapToObj(index -> chunk(request("select value from events where marker = " + index)))
                .toList();

        long directBytes = allocatedBytes(allocationBean, chunks, false);
        long observationBytes = allocatedBytes(allocationBean, chunks, true);
        long registryBytes = observationBytes - directBytes;

        assertTrue(registryBytes < CLASSIFICATION_REQUEST_COUNT * 16L,
                   "statement classification allocated per-request registry entries: extra bytes="
                           + registryBytes);
    }

    @Test
    void buildsChunkEventsFromTheAlreadyValidatedChunkResult() {
        BatchExecutionObservation.BatchWriteRequestView request =
                new BatchExecutionObservation.BatchWriteRequestView(
                        "delete from events where id = ?",
                        BatchWriteOptions.Mode.ATOMIC,
                        1,
                        SqlExecutionBackend.JDBC);
        BatchChunkResult committed = BatchChunkResult.committed(3, 7L, 2, 2L);

        BatchExecutionObservation.Chunk event = assertInstanceOf(
                BatchExecutionObservation.Chunk.class,
                BatchExecutionObservation.chunk(request, committed, 9L));

        assertEquals(BatchChunkResult.Status.COMMITTED, event.status());
        assertEquals(3, event.chunkIndex());
        assertEquals(7L, event.startOffset());
        assertEquals(2L, event.inputCount());
        assertEquals(2L, event.affectedRows());
        assertEquals(SqlStatementType.DELETE, event.statementType());
    }

    @Test
    void keepsDirectChunkConstructionValidated() {
        BatchExecutionObservation.BatchWriteRequestView request =
                new BatchExecutionObservation.BatchWriteRequestView(
                        "insert into events(id) values (?)",
                        BatchWriteOptions.Mode.ATOMIC,
                        1,
                        SqlExecutionBackend.JDBC);

        assertThrows(IllegalArgumentException.class, () -> new BatchExecutionObservation.Chunk(
                request,
                BatchChunkResult.Status.COMMITTED,
                -1,
                0L,
                1L,
                1L,
                1L,
                SqlFailureCategory.NONE,
                null,
                null));
    }

    private long allocatedBytes(com.sun.management.ThreadMXBean bean,
                                List<BatchExecutionObservation.Chunk> chunks,
                                boolean throughObservation) {
        long threadId = Thread.currentThread().threadId();
        long before = bean.getThreadAllocatedBytes(threadId);
        for (BatchExecutionObservation.Chunk chunk : chunks) {
            statementTypeSink = throughObservation
                    ? chunk.statementType()
                    : SqlStatementType.fromSql(chunk.sql());
        }
        return bean.getThreadAllocatedBytes(threadId) - before;
    }

    private static BatchExecutionObservation.BatchWriteRequestView request(String sql) {
        return new BatchExecutionObservation.BatchWriteRequestView(
                sql,
                BatchWriteOptions.Mode.ATOMIC,
                1,
                SqlExecutionBackend.JDBC);
    }

    private static BatchExecutionObservation.Chunk chunk(
            BatchExecutionObservation.BatchWriteRequestView request) {
        return new BatchExecutionObservation.Chunk(
                request,
                BatchChunkResult.Status.COMMITTED,
                0,
                0L,
                1L,
                1L,
                1L,
                SqlFailureCategory.NONE,
                null,
                null);
    }

    private static BatchExecutionObservation.Summary summary(
            BatchExecutionObservation.BatchWriteRequestView request) {
        return new BatchExecutionObservation.Summary(
                request,
                BatchWriteResult.Status.COMMITTED,
                1L,
                1L,
                1L,
                1L,
                0L,
                1L,
                SqlFailureCategory.NONE,
                null,
                null);
    }
}
