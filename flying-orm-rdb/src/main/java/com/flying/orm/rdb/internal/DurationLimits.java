package com.flying.orm.rdb.internal;

import java.time.Duration;
import java.util.Objects;

/**
 * Single overflow-safe arithmetic boundary for timeout durations and monotonic deadlines.
 *
 * <p>The class deliberately knows nothing about SQL, transactions, publishers or resources. Those owners retain
 * their own terminal-state and error semantics; only the repeated numeric conversion lives here.</p>
 *
 * @author wangr
 * @date 2026-08-24
 * @version v3.0
 */
@InternalApi
public final class DurationLimits {

    private static final Duration MAX_NANOS_DURATION = Duration.ofNanos(Long.MAX_VALUE);

    private DurationLimits() {
    }

    /** @return the original duration or the largest duration representable as {@code long} nanoseconds */
    public static Duration clamp(Duration duration) {
        Duration safeDuration = Objects.requireNonNull(duration, "duration must not be null");
        return safeDuration.compareTo(MAX_NANOS_DURATION) > 0 ? MAX_NANOS_DURATION : safeDuration;
    }

    /** @return a saturated nanosecond representation suitable for schedulers and timed waits */
    public static long nanos(Duration duration) {
        return clamp(duration).toNanos();
    }

    /**
     * Computes the non-negative remainder of a duration measured by one monotonic clock.
     * A zero duration remains zero; callers decide whether zero means disabled or already elapsed.
     */
    public static Duration remaining(Duration duration, long startedAtNanos, long nowNanos) {
        long elapsed = Math.max(0L, nowNanos - startedAtNanos);
        long remaining = nanos(duration) - elapsed;
        return remaining <= 0L ? Duration.ZERO : Duration.ofNanos(remaining);
    }

    /** @return {@code left + right}, saturated instead of overflowing */
    public static long addSaturated(long left, long right) {
        if (right < 0L) {
            throw new IllegalArgumentException("deadline duration must not be negative");
        }
        return left > Long.MAX_VALUE - right ? Long.MAX_VALUE : left + right;
    }
}
