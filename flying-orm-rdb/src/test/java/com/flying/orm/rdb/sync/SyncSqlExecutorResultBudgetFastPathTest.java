package com.flying.orm.rdb.sync;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.LinkedList;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SyncSqlExecutorResultBudgetFastPathTest {

    @Test
    void walksSequentialResultsOnceForBudgetChecksAndMapping() {
        CountingLinkedList<DynamicRow> rows = new CountingLinkedList<>();
        for (int index = 0; index < 256; index++) {
            rows.add(DynamicRow.copyOf(Map.of("value", index)));
        }
        SyncSqlExecutor executor = new StubExecutor(rows);
        SqlRequest request = new SqlRequest("select value", List.of());

        assertEquals(rows, executor.query(request, SqlExecutionOptions.unlimited()
                .withMaxResultBytes(1024 * 1024)));
        assertEquals(0, rows.indexedGets);

        List<Integer> mapped = executor.queryMapped(request, null,
                row -> ((Number) row.get("value")).intValue(), 0);

        assertEquals(256, mapped.size());
        assertEquals(0, rows.indexedGets);
    }

    @Test
    void skipsDeepRowEstimationWhenTheResultByteBudgetIsDisabled() {
        CountingText value = new CountingText("payload");
        SyncSqlExecutor executor = new StubExecutor(DynamicRow.copyOf(Map.of("value", value)));

        List<DynamicRow> rows = executor.query(new SqlRequest("select 1", List.of()),
                                               SqlExecutionOptions.unlimited());

        assertEquals(1, rows.size());
        assertEquals(0, value.charReads());
    }

    @Test
    void customExecutorsKeepTheCompatibilityFallbackForMappedTerminalQueries() {
        StubExecutor executor = new StubExecutor(
                DynamicRow.copyOf(Map.of("value", 1)),
                DynamicRow.copyOf(Map.of("value", 2)),
                DynamicRow.copyOf(Map.of("value", 3)));

        List<Integer> rows = executor.queryMapped(
                new SqlRequest("select value", List.of()),
                null,
                row -> ((Number) row.get("value")).intValue(),
                2);

        assertEquals(List.of(1, 2), rows);
        assertThrows(UnsupportedOperationException.class, () -> rows.add(3));
        assertEquals(1, executor.queries());
    }

    @Test
    void mappedTerminalKeepsTheExistingNullResultSemantics() {
        StubExecutor executor = new StubExecutor(DynamicRow.copyOf(Map.of("value", 1)));

        List<Object> rows = executor.queryMapped(
                new SqlRequest("select value", List.of()), null, row -> null, 0);

        assertEquals(1, rows.size());
        assertNull(rows.getFirst());
    }

    private static final class StubExecutor implements SyncSqlExecutor {
        private final List<DynamicRow> rows;
        private int queries;

        private StubExecutor(DynamicRow... rows) {
            this.rows = List.of(rows);
        }

        private StubExecutor(List<DynamicRow> rows) {
            this.rows = rows;
        }

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            queries++;
            return rows;
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            return 0;
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options) {
            return new SqlWriteResult(0, List.of());
        }

        private int queries() {
            return queries;
        }
    }

    private static final class CountingLinkedList<E> extends LinkedList<E> {
        private int indexedGets;

        @Override
        public E get(int index) {
            indexedGets++;
            return super.get(index);
        }
    }

    private static final class CountingText implements CharSequence {
        private final String value;
        private int charReads;

        private CountingText(String value) {
            this.value = value;
        }

        @Override
        public int length() {
            return value.length();
        }

        @Override
        public char charAt(int index) {
            charReads++;
            return value.charAt(index);
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return value.subSequence(start, end);
        }

        private int charReads() {
            return charReads;
        }
    }
}
