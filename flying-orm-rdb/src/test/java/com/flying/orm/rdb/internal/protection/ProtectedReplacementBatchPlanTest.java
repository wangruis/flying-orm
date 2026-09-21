package com.flying.orm.rdb.internal.protection;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.math.BigDecimal;
import java.util.Map;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ProtectedReplacementBatchPlanTest {

    @Test
    void repeatedOwnerFieldStartsANewDeleteThenInsertSegment() {
        ProtectedWriteWork first = work(7L, (byte) 1);
        ProtectedWriteWork second = work(7L, (byte) 2);

        List<ProtectedReplacementBatchPlan.Segment> segments = StreamSupport.stream(
                ProtectedReplacementBatchPlan.segments(
                        List.of(row(first), row(second)), Long.MAX_VALUE).spliterator(), false).toList();

        assertEquals(2, segments.size());
        assertEquals(List.of(1, 1), segments.stream()
                .map(segment -> segment.deleteParameterSets().size()).toList());
        assertEquals(List.of(1, 1), segments.stream()
                .map(segment -> segment.insertions().size()).toList());
    }

    @Test
    void distinctOwnerFieldsShareOneBoundedDeleteSegment() {
        List<ProtectedReplacementBatchPlan.Row> rows = List.of(
                row(work(7L, (byte) 1)), row(work(8L, (byte) 2)));

        List<ProtectedReplacementBatchPlan.Segment> segments = StreamSupport.stream(
                ProtectedReplacementBatchPlan.segments(rows, Long.MAX_VALUE).spliterator(), false).toList();

        assertEquals(1, segments.size());
        assertEquals(2, segments.getFirst().deleteParameterSets().size());
        assertEquals(2, segments.getFirst().insertions().size());
    }

    private static ProtectedReplacementBatchPlan.Row row(ProtectedWriteWork work) {
        return new ProtectedReplacementBatchPlan.Row() {
            @Override
            public ProtectedWriteWork work() {
                return work;
            }

            @Override
            public List<Map<String, Object>> owners() {
                return List.of();
            }
        };
    }

    @Test
    void numericallyEqualDecimalOwnersKeepSequentialReplacementOrder() {
        BigDecimal first = new BigDecimal("1.0");
        BigDecimal second = new BigDecimal("1.00");
        List<ProtectedReplacementBatchPlan.Segment> segments = StreamSupport.stream(
                ProtectedReplacementBatchPlan.segments(
                        List.of(row(work(first, (byte) 1)), row(work(second, (byte) 1))),
                        Long.MAX_VALUE).spliterator(), false).toList();

        assertEquals(2, segments.size());
        assertEquals(first, segments.get(0).deleteParameterSets().getFirst().getFirst());
        assertEquals(second, segments.get(1).deleteParameterSets().getFirst().getFirst());
    }

    @Test
    void distinctDecimalOwnersRemainBatchableAndBinaryOwnersRemainStructural() {
        var distinct = StreamSupport.stream(ProtectedReplacementBatchPlan.segments(
                List.of(row(work(new BigDecimal("1.0"), (byte) 1)),
                        row(work(new BigDecimal("1.01"), (byte) 1))),
                Long.MAX_VALUE).spliterator(), false).toList();
        assertEquals(1, distinct.size());
        var binary = StreamSupport.stream(ProtectedReplacementBatchPlan.segments(
                List.of(row(work(new byte[]{1, 2}, (byte) 1)),
                        row(work(new byte[]{1, 2}, (byte) 2))),
                Long.MAX_VALUE).spliterator(), false).toList();
        assertEquals(2, binary.size());
    }

    private static ProtectedWriteWork work(Object ownerId, byte token) {
        return new ProtectedWriteWork(
                ProtectedWriteWork.Kind.UPSERT,
                new SqlRequest("update business_row set value_col = ? where id = ?",
                               List.of("value", ownerId)),
                null,
                List.of("id"),
                Map.of("id", ownerId),
                "id = ?",
                "delete from token_index where id = ? and field_tag = ?",
                "insert into token_index(id, field_tag, token) values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens(
                        "phone", List.of(new byte[]{token}))));
    }
}
