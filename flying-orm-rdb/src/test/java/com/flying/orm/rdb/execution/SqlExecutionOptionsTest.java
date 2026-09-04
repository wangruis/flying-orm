package com.flying.orm.rdb.execution;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SqlExecutionOptionsTest {

    @Test
    void leavesOrdinarySqlLifetimeToTheCallerByDefault() {
        assertTrue(SqlExecutionOptions.safeDefaults().timeout().isZero());
    }

    @Test
    void leavesStreamingTotalsUnlimitedButKeepsLobAndCleanupBoundedByDefault() {
        SqlExecutionOptions options = SqlExecutionOptions.safeDefaults();

        assertEquals(0L, options.maxRows());
        assertEquals(0L, options.maxResultBytes());
        assertTrue(options.maxLargeObjectBytes() > 0L);
        assertTrue(options.maxLargeObjectChars() > 0L);
        assertTrue(options.cleanupTimeout().isPositive());
    }

    @Test
    void configuringOnlyATimeoutDoesNotEnableStreamingTotalLimits() {
        SqlExecutionOptions options = SqlExecutionOptions.timeout(Duration.ofSeconds(1));

        assertEquals(0L, options.maxRows());
        assertEquals(0L, options.maxResultBytes());
        assertEquals(Duration.ofSeconds(1), options.timeout());
        assertTrue(options.maxLargeObjectBytes() > 0L);
        assertTrue(options.cleanupTimeout().isPositive());
    }
}
