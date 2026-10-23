package com.flying.orm.rdb.sync;

import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.result.DynamicRow;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

class StreamingSyncExecutionContractTest {
    private static final SqlRequest REQUEST = new SqlRequest("select value from items", List.of());

    @Test
    void mappedQueriesStopAtTheRequestedRowWithoutCollectingRawResults() {
        StreamingExecutor executor = new StreamingExecutor();
        List<Object> rows = executor.queryMapped(REQUEST, null, row -> row.get("value"), 2);
        assertEquals(List.of(0, 1), rows);
        assertEquals(2, executor.consumed);
        assertThrows(UnsupportedOperationException.class, () -> rows.add(3));
    }

    @Test
    void eachUsesTheSameStreamingContract() {
        StreamingExecutor executor = new StreamingExecutor();
        int[] total = {0};
        assertEquals(3, executor.queryEach(REQUEST, null, row -> total[0] += (Integer) row.get("value")));
        assertEquals(3, total[0]);
    }

    @Test
    void mapperFailureStopsTheSourceAndKeepsTheOriginalFailure() {
        StreamingExecutor executor = new StreamingExecutor();
        IllegalStateException failure = new IllegalStateException("mapping failed");
        assertSame(failure, assertThrows(IllegalStateException.class,
                () -> executor.queryMapped(REQUEST, null, row -> { throw failure; }, 0)));
        assertEquals(1, executor.consumed);
    }

    private static final class StreamingExecutor implements SyncSqlExecutor {
        private int consumed;

        @Override
        public List<DynamicRow> query(SqlRequest request) {
            throw new AssertionError("streaming terminals must not materialize an intermediate list");
        }

        @Override
        public long queryWhile(SqlRequest request, SqlExecutionOptions options,
                               Predicate<? super DynamicRow> consumer) {
            for (int value = 0; value < 3; value++) {
                consumed++;
                if (!consumer.test(DynamicRow.copyOf(Map.of("value", value)))) break;
            }
            return consumed;
        }

        @Override
        public long rowsUpdated(SqlRequest request) { throw new UnsupportedOperationException(); }
    }
}
