package com.flying.orm.rdb.internal.protection;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.ProtectedWriteWork;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ProtectedTokenInsertBatchPlanTest {

    @Test
    void sqlShapeTransitionsPreserveInsertionOrder() {
        var first = work(1L, "token_a");
        var second = work(2L, "token_b");
        var segments = segments(List.of(row(first), row(second), row(first)), Long.MAX_VALUE);

        assertEquals(List.of("insert into token_a values (?, ?, ?)",
                            "insert into token_b values (?, ?, ?)",
                            "insert into token_a values (?, ?, ?)"),
                segments.stream().map(segment -> segment.insertions().getFirst().work().insertSql()).toList());
    }

    @Test
    void operationLimitSplitsTheFiveHundredAndFirstReplacement() {
        var rows = IntStream.range(0, 501).mapToObj(index -> row(work(index, "token_a"))).toList();

        var segments = segments(rows, Long.MAX_VALUE);
        assertTrue(segments.stream().allMatch(segment -> segment.insertions().size() <= 500));
        assertEquals(IntStream.range(0, 501).boxed().toList(), segments.stream()
                .flatMap(segment -> segment.insertions().stream()).map(insertion -> insertion.owner().get("id"))
                .toList());
    }

    @Test
    void parameterLimitCountsEveryCompositeOwnerColumnAndFieldTag() {
        List<String> ownerFields = List.of("a", "b", "c", "d");
        var rows = IntStream.range(0, 401).mapToObj(index -> {
            var base = work(index, "token_a");
            return row(new ProtectedWriteWork(base.kind(), base.writeRequest(), null, ownerFields,
                    Map.of("a", index, "b", 2L, "c", 3L, "d", 4L),
                    "a = ? and b = ? and c = ? and d = ?",
                    "delete from token_a where a = ? and b = ? and c = ? and d = ? and field_tag = ?",
                    "insert into token_a values (?, ?, ?, ?, ?, ?)", base.fields()));
        }).toList();

        var segments = segments(rows, Long.MAX_VALUE);
        assertTrue(segments.stream().allMatch(segment ->
                segment.deleteParameterSets().stream().mapToInt(List::size).sum() <= 2_000));
        assertEquals(IntStream.range(0, 401).boxed().toList(), segments.stream()
                .flatMap(segment -> segment.deleteParameterSets().stream()).map(List::getFirst).toList());
    }

    @Test
    void replacementBudgetSplitsBeforeTheNextOwnerWithoutLosingTokens() {
        var segments = segments(List.of(row(work(1L, "token_a")), row(work(2L, "token_a"))), 200L);

        // Each replacement weighs 141 bytes; two cannot share the supplied 200-byte metadata budget.
        assertTrue(segments.stream().allMatch(segment -> segment.insertions().size() * 141L <= 200L));
        assertEquals(List.of(1L, 2L), segments.stream()
                .flatMap(segment -> segment.insertions().stream()).map(insertion -> insertion.owner().get("id"))
                .toList());
    }

    private static List<ProtectedReplacementBatchPlan.Segment> segments(
            List<? extends ProtectedReplacementBatchPlan.Row> rows, long bytes) {
        return StreamSupport.stream(ProtectedReplacementBatchPlan.segments(rows, bytes).spliterator(), false)
                .toList();
    }

    private static ProtectedReplacementBatchPlan.Row row(ProtectedWriteWork work) {
        return new ProtectedReplacementBatchPlan.Row() {
            public ProtectedWriteWork work() { return work; }
            public List<Map<String, Object>> owners() { return List.of(); }
        };
    }

    private static ProtectedWriteWork work(Object id, String table) {
        return new ProtectedWriteWork(ProtectedWriteWork.Kind.UPSERT,
                new SqlRequest("update business_row set value_col = ? where id = ?", List.of("value", id)),
                null, List.of("id"), Map.of("id", id), "id = ?",
                "delete from " + table + " where id = ? and field_tag = ?",
                "insert into " + table + " values (?, ?, ?)",
                List.of(new ProtectedWriteWork.FieldTokens("phone", List.of(new byte[]{1}))));
    }
}
