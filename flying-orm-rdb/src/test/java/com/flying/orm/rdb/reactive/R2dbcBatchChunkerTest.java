package com.flying.orm.rdb.reactive;

import com.flying.orm.core.sql.render.SqlBindMarkerStyle;
import com.flying.orm.rdb.batch.BatchWriteOptions;
import com.flying.orm.rdb.batch.BatchWriteRequest;
import com.flying.orm.rdb.batch.BatchMemoryLimitExceededException;
import com.flying.orm.rdb.execution.BatchRowSnapshotter;
import com.flying.orm.rdb.execution.ProtectedBatchRows;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import com.flying.orm.core.sql.render.SqlRequest;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.util.List;
import java.util.Map;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class R2dbcBatchChunkerTest {

    @Test
    void snapshotsRowsAndKeepsGlobalOffsetsAcrossChunks() {
        Object[] first = {"one"};
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "INSERT INTO samples(value) VALUES (?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.just(first, new Object[]{"two"}, new Object[]{"three"}),
                BatchWriteOptions.atomic(2));

        List<R2dbcBatchWriterChunks.BatchChunk> chunks = R2dbcBatchChunker.chunks(request)
                .collectList()
                .block();

        assertNotNull(chunks);
        assertEquals(2, chunks.size());
        assertEquals(0, chunks.getFirst().chunkIndex());
        assertEquals(0L, chunks.getFirst().startOffset());
        assertEquals(2, chunks.getFirst().rows().size());
        assertSame(first, chunks.getFirst().rows().getFirst().row());
        assertEquals("one", chunks.getFirst().rows().getFirst().row()[0]);
        assertEquals(1, chunks.getLast().chunkIndex());
        assertEquals(2L, chunks.getLast().startOffset());
        assertEquals(1, chunks.getLast().rows().size());
    }

    @Test
    void formsASmallerChunkWhenTheNextOwnedRowWouldExceedTheByteBudget() {
        Object[] first = {new byte[64]};
        Object[] second = {new byte[64]};
        long oneRowBytes = BatchRowSnapshotter.snapshotAndEstimate(
                first, 1, Long.MAX_VALUE, "test budget").estimatedBytes();
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "INSERT INTO samples(value) VALUES (?)",
                1,
                List.of(byte[].class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.just(first, second),
                BatchWriteOptions.atomic(2).withMemoryLimits(2, oneRowBytes, 2)
                        .withMaxRowBytes(oneRowBytes));

        List<R2dbcBatchWriterChunks.BatchChunk> chunks = R2dbcBatchChunker.chunks(request)
                .collectList()
                .block();

        assertNotNull(chunks);
        assertEquals(List.of(1, 1), chunks.stream().map(chunk -> chunk.rows().size()).toList());
        assertSame(first, chunks.getFirst().rows().getFirst().row());
        assertSame(second, chunks.getLast().rows().getFirst().row());
        assertTrue(chunks.stream().allMatch(chunk -> chunk.estimatedBytes() <= oneRowBytes));
    }

    @Test
    void unlimitedRowPolicyStillStreamsBoundedChunksWithGlobalOffsets() {
        BatchWriteRequest request = request(
                Flux.range(0, 5).map(value -> new Object[]{value}),
                BatchWriteOptions.unlimitedAtomic(2));

        List<R2dbcBatchWriterChunks.BatchChunk> chunks = R2dbcBatchChunker.chunks(request)
                .collectList()
                .block();

        assertNotNull(chunks);
        assertEquals(List.of(0L, 2L, 4L), chunks.stream().map(
                R2dbcBatchWriterChunks.BatchChunk::startOffset).toList());
        assertEquals(List.of(2, 2, 1), chunks.stream().map(chunk -> chunk.rows().size()).toList());
    }

    @Test
    void explicitTotalRowLimitStillStopsBeforeTheNextChunk() {
        BatchWriteRequest request = request(
                Flux.range(0, 3).map(value -> new Object[]{value}),
                BatchWriteOptions.atomic(2).withMaxRows(2));

        assertThrows(R2dbcBatchRowLimitExceededException.class,
                     () -> R2dbcBatchChunker.chunks(request).collectList().block());
    }

    @Test
    void emitsAFullAdmissionSlotWithoutAcceptingALookaheadRow() {
        AtomicInteger accepted = new AtomicInteger();
        BatchWriteRequest request = bytesRequest(
                Flux.range(0, 3).map(ignored -> new Object[]{new byte[950]})
                        .doOnNext(ignored -> accepted.incrementAndGet()),
                BatchWriteOptions.atomic(8).withMemoryLimits(3, 1024, 3).withMaxRowBytes(1024));

        R2dbcBatchWriterChunks.BatchChunk first = R2dbcBatchChunker.chunks(request)
                .next().block(Duration.ofSeconds(2));

        assertNotNull(first);
        assertEquals(998, first.estimatedBytes());
        assertEquals(1, accepted.get());
        assertEquals(1, first.rows().size());
    }

    @Test
    void leavesRoomForAMaximumRowAfterASmallRowWithoutOwningBoth() {
        AtomicInteger accepted = new AtomicInteger();
        List<Integer> acceptedAtEmission = new java.util.ArrayList<>();
        Object[] small = {new byte[0]};
        Object[] maximum = {new byte[976]};
        BatchWriteRequest request = bytesRequest(
                Flux.just(small, maximum).doOnNext(ignored -> accepted.incrementAndGet()),
                BatchWriteOptions.atomic(8).withMemoryLimits(2, 1024, 2).withMaxRowBytes(1024));

        List<R2dbcBatchWriterChunks.BatchChunk> chunks = R2dbcBatchChunker.chunks(request)
                .doOnNext(ignored -> acceptedAtEmission.add(accepted.get()))
                .collectList().block(Duration.ofSeconds(2));

        assertNotNull(chunks);
        assertEquals(List.of(1, 2), acceptedAtEmission);
        assertEquals(List.of(48L, 1024L), chunks.stream().map(
                R2dbcBatchWriterChunks.BatchChunk::estimatedBytes).toList());
        assertEquals(List.of(0L, 1L), chunks.stream().map(
                R2dbcBatchWriterChunks.BatchChunk::startOffset).toList());
        assertSame(small, chunks.getFirst().rows().getFirst().row());
        assertSame(maximum, chunks.getLast().rows().getFirst().row());
    }

    @Test
    void acceptsAnotherMaximumRowWhenTheRemainingSlotEqualsItsLimit() {
        BatchWriteRequest request = bytesRequest(
                Flux.just(new Object[]{new byte[464]}, new Object[]{new byte[464]}),
                BatchWriteOptions.atomic(8).withMemoryLimits(2, 1024, 2).withMaxRowBytes(512));

        List<R2dbcBatchWriterChunks.BatchChunk> chunks = R2dbcBatchChunker.chunks(request)
                .collectList().block(Duration.ofSeconds(2));

        assertNotNull(chunks);
        assertEquals(1, chunks.size());
        assertEquals(2, chunks.getFirst().rows().size());
        assertEquals(1024L, chunks.getFirst().estimatedBytes());
    }

    @Test
    void rejectsARowAboveItsExplicitAdmissionLimitEvenWhenTheChunkHasSpace() {
        BatchWriteRequest request = bytesRequest(
                Flux.<Object[]>just(new Object[]{new byte[465]}),
                BatchWriteOptions.atomic(8).withMemoryLimits(1, 1024, 1).withMaxRowBytes(512));

        assertThrows(BatchMemoryLimitExceededException.class,
                () -> R2dbcBatchChunker.chunks(request).collectList().block(Duration.ofSeconds(2)));
    }

    @Test
    void defaultAdmissionStillBatchesThousandsOfSmallRows() {
        BatchWriteRequest request = request(
                Flux.range(0, 4097).map(value -> new Object[]{value}), BatchWriteOptions.defaults());

        List<R2dbcBatchWriterChunks.BatchChunk> chunks = R2dbcBatchChunker.chunks(request)
                .collectList().block(Duration.ofSeconds(2));

        assertNotNull(chunks);
        assertEquals(List.of(500, 500, 500, 500, 500, 500, 500, 500, 97),
                chunks.stream().map(chunk -> chunk.rows().size()).toList());
        assertEquals(List.of(0L, 500L, 1000L, 1500L, 2000L, 2500L, 3000L, 3500L, 4000L),
                chunks.stream().map(R2dbcBatchWriterChunks.BatchChunk::startOffset).toList());
    }

    @Test
    void keepsOneDecodedProtectedRowAndBindsOnlyItsParameterPrefix() {
        ProtectedWriteWork work = new ProtectedWriteWork(
                ProtectedWriteWork.Kind.INSERT,
                new SqlRequest("insert into samples(value) values (?)", List.of("value")),
                null,
                List.of("id"),
                Map.of("id", 1L),
                "id = ?",
                "delete from sample_tokens where id = ?",
                "insert into sample_tokens(id, token) values (?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("value", List.of(new byte[]{3}))));
        Object[] receipt = {new byte[]{7}};
        Object[] protectedRow = ProtectedBatchRows.extend(new Object[]{"value"}, work, receipt);
        BatchWriteRequest request = com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "insert into samples(value) values (?)",
                1,
                List.of(String.class),
                SqlBindMarkerStyle.CANONICAL,
                Flux.<Object[]>just(protectedRow),
                BatchWriteOptions.atomic(1));

        R2dbcBatchWriterChunks.BatchChunk chunk = R2dbcBatchChunker.chunks(request).blockFirst();

        assertNotNull(chunk);
        ProtectedBatchRows.RowView rowView = chunk.rows().getFirst();
        assertSame(protectedRow, rowView.row());
        assertSame(work, rowView.work());
        assertEquals(7, ((byte[]) rowView.receiptParameters()[0])[0]);
        assertEquals(new BatchPayloadHasher().hashRows(List.<Object[]>of(receipt)),
                     new R2dbcBatchReceiptSupport().chunkPayloadHash(chunk));
    }

    private static BatchWriteRequest request(Flux<Object[]> rows, BatchWriteOptions options) {
        return com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "INSERT INTO samples(value) VALUES (?)",
                1,
                List.of(Integer.class),
                SqlBindMarkerStyle.CANONICAL,
                rows,
                options);
    }

    private static BatchWriteRequest bytesRequest(Flux<Object[]> rows, BatchWriteOptions options) {
        return com.flying.orm.rdb.batch.BatchWriteRequests.request(
                "INSERT INTO samples(value) VALUES (?)", 1, List.of(byte[].class),
                SqlBindMarkerStyle.CANONICAL, rows, options);
    }
}
