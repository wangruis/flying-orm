package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.execution.SqlExecutionOptions;
import com.flying.orm.rdb.execution.SqlExecutionTimeoutException;
import com.flying.orm.rdb.execution.SqlResultMemoryLimitExceededException;
import com.flying.orm.rdb.execution.SqlRowLimitExceededException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.nio.charset.StandardCharsets;
import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class ReactiveSqlExecutionProtectionTest {

    @Test
    void protectRowsStopsWhenRowLimitExceeded() {
        StepVerifier.create(ReactiveSqlExecutionProtection
                                    .protectRows(Flux.just("row1", "row2", "row3"),
                                                "select id from users",
                                                SqlExecutionOptions.maxRows(1)))
                    .assertNext(value -> assertEquals("row1", value))
                    .verifyErrorSatisfies(error -> {
                        SqlRowLimitExceededException ex = assertInstanceOf(SqlRowLimitExceededException.class, error);
                        assertEquals(1L, ex.maxRows());
                        assertEquals(1L, ex.overflowIndex());
                    });
    }

    @Test
    void protectRowsStopsWhenMemoryLimitExceeded() {
        StepVerifier.create(ReactiveSqlExecutionProtection
                                    .protectRows(Flux.just("aabb", "bbcc"),
                                                "select comment from events",
                                                SqlExecutionOptions.safeDefaults().withMaxResultBytes(7),
                                                row -> row.getBytes(StandardCharsets.UTF_8).length))
                    .assertNext(value -> assertEquals("aabb", value))
                    .verifyErrorSatisfies(error -> {
                        SqlResultMemoryLimitExceededException ex = assertInstanceOf(
                                SqlResultMemoryLimitExceededException.class, error);
                        assertEquals(7L, ex.maxResultBytes());
                        assertEquals(1L, ex.overflowIndex());
                    });
    }

    /** 行大小无法安全估算时必须失败闭合，不能把未知大小按零字节放过结果内存门禁。 */
    @Test
    void protectRowsFailsClosedWhenMemoryEstimateCannotInspectRow() {
        StepVerifier.create(ReactiveSqlExecutionProtection
                                    .protectRows(Flux.just("opaque-driver-value"),
                                                "select payload from events",
                                                SqlExecutionOptions.safeDefaults().withMaxResultBytes(32),
                                                ignored -> {
                                                    throw new IllegalStateException("driver value cannot be inspected");
                                                }))
                    .verifyErrorSatisfies(error -> {
                        SqlResultMemoryLimitExceededException ex = assertInstanceOf(
                                SqlResultMemoryLimitExceededException.class, error);
                        assertEquals(32L, ex.maxResultBytes());
                        assertEquals(Long.MAX_VALUE, ex.attemptedBytes());
                        assertEquals(0L, ex.overflowIndex());
                    });
    }

    /** 即使显式上限取 long 最大值，无法估算的结果也必须失败关闭。 */
    @Test
    void protectRowsFailsClosedAtMaximumConfiguredLimitWhenMemoryEstimateCannotInspectRow() {
        StepVerifier.create(ReactiveSqlExecutionProtection
                                    .protectRows(Flux.just("opaque-driver-value"),
                                                "select payload from events",
                                                SqlExecutionOptions.safeDefaults()
                                                                   .withMaxResultBytes(Long.MAX_VALUE),
                                                ignored -> {
                                                    throw new IllegalStateException(
                                                            "driver value cannot be inspected");
                                                }))
                    .verifyErrorSatisfies(error -> {
                        SqlResultMemoryLimitExceededException ex = assertInstanceOf(
                                SqlResultMemoryLimitExceededException.class, error);
                        assertEquals(Long.MAX_VALUE, ex.maxResultBytes());
                        assertEquals(Long.MAX_VALUE, ex.attemptedBytes());
                        assertEquals(0L, ex.overflowIndex());
                    });
    }

    /** 行大小估算异常图中的 JVM fatal 必须保持原对象，不能降级为普通内存门禁错误。 */
    @Test
    void protectRowsPropagatesNestedVirtualMachineErrorFromMemoryEstimator() {
        OutOfMemoryError fatal = new OutOfMemoryError("estimator fatal");

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                                                  () -> ReactiveSqlExecutionProtection
                                                          .protectRows(Flux.just("opaque-driver-value"),
                                                                       "select payload from events",
                                                                       SqlExecutionOptions.safeDefaults()
                                                                                          .withMaxResultBytes(32),
                                                                       ignored -> {
                                                                           throw new IllegalStateException(
                                                                                   "driver wrapper", fatal);
                                                                       })
                                                          .blockLast());

        assertSame(fatal, observed);
    }

    @Test
    void protectMonoTimesOut() {
        StepVerifier.withVirtualTime(() -> ReactiveSqlExecutionProtection
                .protectMono(Mono.never(), SqlExecutionOptions.timeout(Duration.ofMillis(50))))
                .thenAwait(Duration.ofMillis(100))
                .expectError(SqlExecutionTimeoutException.class)
                .verify(Duration.ofSeconds(1));
    }

    @Test
    void protectRowsTimesOutWhenExecutionRunsTooLong() {
        StepVerifier.withVirtualTime(() -> ReactiveSqlExecutionProtection
                .protectRows(Flux.concat(Flux.just("row1"), Flux.never()),
                             "select * from users",
                             SqlExecutionOptions.timeout(Duration.ofMillis(50)),
                             null))
                .thenAwait(Duration.ofMillis(100))
                .expectNext("row1")
                .expectError(SqlExecutionTimeoutException.class)
                .verify(Duration.ofSeconds(1));
    }

    /** 合法的极远截止时间不能因 Reactor 的纳秒换算上限而让 Mono 在装配期失败。 */
    @Test
    void protectMonoAcceptsDurationWhoseNanosecondsDoNotFitInLong() {
        StepVerifier.create(ReactiveSqlExecutionProtection.protectMono(
                                    Mono.just("ok"),
                                    SqlExecutionOptions.safeDefaults()
                                                       .withTimeout(Duration.ofSeconds(Long.MAX_VALUE))))
                    .expectNext("ok")
                    .verifyComplete();
    }

    /** 清理错误已持有主错误时，补充上下文不能反向建立 Throwable 环。 */
    @Test
    void doesNotCreateSuppressedCycleWhenCleanupAlreadyReferencesPrimary() {
        IllegalStateException primary = new IllegalStateException("sequence failed");
        IllegalStateException cleanup = new IllegalStateException("cleanup failed");
        cleanup.addSuppressed(primary);

        ReactiveSqlExecutionProtection.addSuppressedIfAcyclic(primary, cleanup);

        assertEquals(0, primary.getSuppressed().length);
        assertEquals(1, cleanup.getSuppressed().length);
        assertSame(primary, cleanup.getSuppressed()[0]);
    }
}
