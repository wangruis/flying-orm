package com.flying.orm.rdb.internal;

import org.junit.jupiter.api.Test;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class DurationLimitsTest {

    @Test
    void clampsDurationsThatCannotBeRepresentedAsNanoseconds() {
        assertEquals(Long.MAX_VALUE, DurationLimits.nanos(Duration.ofSeconds(Long.MAX_VALUE)));
    }

    @Test
    void computesOneMonotonicRemainingBudget() {
        assertEquals(Duration.ofNanos(70), DurationLimits.remaining(Duration.ofNanos(100), 10, 40));
        assertEquals(Duration.ZERO, DurationLimits.remaining(Duration.ofNanos(100), 10, 110));
        assertEquals(Duration.ofNanos(100), DurationLimits.remaining(Duration.ofNanos(100), 40, 10));
    }

    @Test
    void saturatesDeadlineAdditionWithoutAcceptingInvalidClockValues() {
        assertEquals(Long.MAX_VALUE, DurationLimits.addSaturated(Long.MAX_VALUE - 5, 6));
        assertEquals(9L, DurationLimits.addSaturated(4, 5));
        assertEquals(1L, DurationLimits.addSaturated(-1, 2));
        assertThrows(IllegalArgumentException.class, () -> DurationLimits.addSaturated(1, -1));
    }
}
