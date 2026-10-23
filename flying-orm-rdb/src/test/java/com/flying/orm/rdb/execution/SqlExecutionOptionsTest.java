package com.flying.orm.rdb.execution;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SqlExecutionOptionsTest {

    @Test
    void leavesStreamingAndLobCapacityToTheApplicationByDefault() {
        SqlExecutionOptions options = SqlExecutionOptions.unlimited();
        assertSame(options, SqlExecutionOptions.unlimited());

        assertEquals(0L, options.maxRows());
        assertEquals(0L, options.maxResultBytes());
        assertEquals(0L, options.maxLargeObjectBytes());
        assertEquals(0L, options.maxLargeObjectChars());
        assertEquals(0, options.fetchSize());
    }

    @Test
    void capacityDerivationKeepsEveryOtherSetting() {
        SqlExecutionOptions options = SqlExecutionOptions.unlimited()
                .withMaxRows(7).withMaxResultBytes(1024)
                .withMaxLargeObjectBytes(256).withMaxLargeObjectChars(128).withFetchSize(32);

        assertEquals(new SqlExecutionOptions(7, 1024, 256, 128, 32), options);
        assertEquals(new SqlExecutionOptions(0, 0, 0, 0, 0), SqlExecutionOptions.unlimited());
        assertEquals(new SqlExecutionOptions(7, 1024, 256, 128, 0),
                new SqlExecutionOptions(7, 1024, 256, 128));
    }

    @Test
    void changingOnlyMaxRowsRetainsLobDefaults() {
        assertEquals(SqlExecutionOptions.unlimited().withMaxRows(7), SqlExecutionOptions.maxRows(7));
    }

    @Test
    void rejectsNegativeCapacityAndFetchSize() {
        SqlExecutionOptions defaults = SqlExecutionOptions.unlimited();
        assertThrows(IllegalArgumentException.class, () -> defaults.withMaxRows(-1));
        assertThrows(IllegalArgumentException.class, () -> defaults.withMaxResultBytes(-1));
        assertThrows(IllegalArgumentException.class, () -> defaults.withMaxLargeObjectBytes(-1));
        assertThrows(IllegalArgumentException.class, () -> defaults.withMaxLargeObjectChars(-1));
        assertThrows(IllegalArgumentException.class, () -> defaults.withFetchSize(-1));
    }

}
