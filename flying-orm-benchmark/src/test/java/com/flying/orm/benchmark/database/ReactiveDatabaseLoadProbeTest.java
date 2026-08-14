package com.flying.orm.benchmark.database;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 只短跑内存 Publisher，真实数据库长跑由独立性能脚本执行。 */
class ReactiveDatabaseLoadProbeTest {

    @Test
    void recordsBoundedSuccessfulOperationsAndLatencyPercentiles() {
        ReactiveDatabaseLoadProbe.Plan plan = new ReactiveDatabaseLoadProbe.Plan(
                Duration.ofMillis(20), Duration.ofMillis(80), 2, 3, Duration.ofSeconds(1));

        ReactiveDatabaseLoadProbe.Result result = ReactiveDatabaseLoadProbe
                .run(plan, () -> Mono.delay(Duration.ofMillis(1)))
                .block(Duration.ofSeconds(3));

        assertTrue(result.succeeded() > 0);
        assertEquals(0, result.failed());
        assertEquals(result.succeeded() * 3, result.rows());
        assertTrue(result.p50Nanos() > 0);
        assertTrue(result.p99Nanos() >= result.p50Nanos());
    }

    @Test
    void keepsFailuresVisibleWhileOtherOperationsContinue() {
        AtomicInteger calls = new AtomicInteger();
        ReactiveDatabaseLoadProbe.Plan plan = new ReactiveDatabaseLoadProbe.Plan(
                Duration.ZERO, Duration.ofMillis(60), 1, 1, Duration.ofSeconds(1));

        ReactiveDatabaseLoadProbe.Result result = ReactiveDatabaseLoadProbe
                .run(plan, () -> calls.incrementAndGet() % 2 == 0
                        ? Mono.error(new IllegalStateException("planned"))
                        : Mono.empty())
                .block(Duration.ofSeconds(3));

        assertTrue(result.succeeded() > 0);
        assertTrue(result.failed() > 0);
        assertEquals(result.operations(), result.succeeded() + result.failed());
        assertEquals(result.failed(), result.failuresByType().get("IllegalStateException"));
    }

    /** JVM 致命错误不能被性能探针降级成普通失败后继续压测。 */
    @Test
    void propagatesVirtualMachineErrorFromOperationSupplier() {
        OutOfMemoryError fatal = new OutOfMemoryError("planned fatal");
        ReactiveDatabaseLoadProbe.Plan plan = new ReactiveDatabaseLoadProbe.Plan(
                Duration.ZERO, Duration.ofMillis(20), 1, 1, Duration.ofSeconds(1));

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                () -> ReactiveDatabaseLoadProbe.run(plan, () -> {
                    throw fatal;
                }).block(Duration.ofSeconds(3)));

        assertSame(fatal, observed);
    }

    /** 驱动包装在 cause 图中的 JVM 致命错误也必须恢复原对象。 */
    @Test
    void propagatesNestedVirtualMachineErrorFromOperationPublisher() {
        OutOfMemoryError fatal = new OutOfMemoryError("nested fatal");
        ReactiveDatabaseLoadProbe.Plan plan = new ReactiveDatabaseLoadProbe.Plan(
                Duration.ZERO, Duration.ofMillis(20), 1, 1, Duration.ofSeconds(1));

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class,
                () -> ReactiveDatabaseLoadProbe.run(
                        plan, () -> Mono.error(new IllegalStateException("driver wrapper", fatal)))
                        .block(Duration.ofSeconds(3)));

        assertSame(fatal, observed);
    }

    @Test
    void rejectsUnsafePlansBeforeStartingLoad() {
        assertThrows(IllegalArgumentException.class,
                     () -> new ReactiveDatabaseLoadProbe.Plan(Duration.ZERO,
                                                               Duration.ZERO,
                                                               1,
                                                               1,
                                                               Duration.ofSeconds(1)));
        assertThrows(IllegalArgumentException.class,
                     () -> new ReactiveDatabaseLoadProbe.Plan(Duration.ZERO,
                                                               Duration.ofSeconds(1),
                                                               0,
                                                               1,
                                                               Duration.ofSeconds(1)));
    }
}
