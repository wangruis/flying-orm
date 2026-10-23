package com.flying.orm.rdb.sync;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlWriteResult;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SyncSqlExecutorCollectionTest {

    @Test
    void walksSequentialResultsOnceForMapping() {
        CountingLinkedList<DynamicRow> rows = new CountingLinkedList<>();
        for (int index = 0; index < 256; index++) {
            rows.add(DynamicRow.copyOf(Map.of("value", index)));
        }
        SyncSqlExecutor executor = new StubExecutor(rows);
        SqlRequest request = new SqlRequest("select value", List.of());

        List<Integer> mapped = executor.queryMapped(request, null,
                row -> ((Number) row.get("value")).intValue(), 0);

        assertEquals(256, mapped.size());
        assertEquals(0, rows.indexedGets);
    }

    @Test
    void listTerminalForwardsExplicitOptionsToTheStreamingPort() {
        StubExecutor executor = new StubExecutor(DynamicRow.copyOf(Map.of("value", 1)));
        SqlExecutionOptions options = SqlExecutionOptions.unlimited();

        List<DynamicRow> rows = executor.query(new SqlRequest("select 1", List.of()), options);

        assertEquals(1, rows.size());
        assertEquals(1, executor.queries());
        assertSame(options, executor.options);
        assertThrows(UnsupportedOperationException.class, rows::clear);
    }

    @Test
    void mappedTerminalStopsAfterTheRequestedNumberOfRows() {
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
    void mappedTerminalAllowsNullMappedValues() {
        StubExecutor executor = new StubExecutor(DynamicRow.copyOf(Map.of("value", 1)));

        List<Object> rows = executor.queryMapped(
                new SqlRequest("select value", List.of()), null, row -> null, 0);

        assertEquals(1, rows.size());
        assertNull(rows.getFirst());
    }

    private static final class StubExecutor implements SyncSqlExecutor {
        private final List<DynamicRow> rows;
        private int queries;
        private SqlExecutionOptions options;

        private StubExecutor(DynamicRow... rows) {
            this.rows = List.of(rows);
        }

        private StubExecutor(List<DynamicRow> rows) {
            this.rows = rows;
        }

        @Override
        public long queryWhile(SqlRequest request, SqlExecutionOptions options,
                               Predicate<? super DynamicRow> consumer) {
            queries++;
            this.options = options;
            long consumed = 0;
            for (DynamicRow row : rows) {
                consumed++;
                if (!consumer.test(row)) break;
            }
            return consumed;
        }

        @Override
        public long rowsUpdated(SqlRequest request) {
            return 0;
        }

        @Override
        public SqlWriteResult rowsUpdatedReturningKeys(SqlRequest request, SqlExecutionOptions options, String generatedKeyColumn) {
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

}
