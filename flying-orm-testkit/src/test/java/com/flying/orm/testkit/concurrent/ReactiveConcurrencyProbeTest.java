package com.flying.orm.testkit.concurrent;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.time.Duration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 验证并发探针真的限制在指定并发内，并且超时后能看见被取消的任务。
 *
 * @author wangr
 * @date 2026-07-31
 * @version v1.0
 */
class ReactiveConcurrencyProbeTest {

    /**
     * 正常完成时，峰值并发不能突破计划上限。
     */
    @Test
    void capsConcurrencyAndCountsCompletedOperations() {
        ReactiveConcurrencyProbe.Plan plan = new ReactiveConcurrencyProbe.Plan(20,
                                                                                4,
                                                                                Duration.ofSeconds(2));

        ReactiveConcurrencyProbe.Result result = ReactiveConcurrencyProbe.run(
                plan,
                ignored -> Mono.delay(Duration.ofMillis(5)))
                                                                               .block(Duration.ofSeconds(3));

        assertEquals(20, result.requested());
        assertEquals(20, result.completed());
        assertEquals(0, result.failed());
        assertEquals(0, result.cancelled());
        assertEquals(4, result.maxInFlight());
        assertFalse(result.timedOut());
    }

    /**
     * 整体超时会取消正在运行的任务，结果仍然保留已经启动多少个。
     */
    @Test
    void reportsCancelledOperationsWhenProbeTimesOut() {
        ReactiveConcurrencyProbe.Plan plan = new ReactiveConcurrencyProbe.Plan(10,
                                                                                3,
                                                                                Duration.ofMillis(20));

        ReactiveConcurrencyProbe.Result result = ReactiveConcurrencyProbe.run(plan, ignored -> Mono.never())
                                                                               .block(Duration.ofSeconds(1));

        assertEquals(3, result.started());
        assertEquals(3, result.cancelled());
        assertEquals(3, result.maxInFlight());
        assertTrue(result.timedOut());
    }

    /** 单个操作在创建 Publisher 前抛出的普通 Error 也应计入失败，不能终止整个探针。 */
    @Test
    void countsSynchronousOperationErrorAndContinuesProbe() {
        ReactiveConcurrencyProbe.Plan plan = new ReactiveConcurrencyProbe.Plan(3, 1, Duration.ZERO);

        ReactiveConcurrencyProbe.Result result = ReactiveConcurrencyProbe.run(plan, index -> {
            if (index == 1) {
                throw new AssertionError("operation failed before returning a publisher");
            }
            return Mono.empty();
        }).block(Duration.ofSeconds(1));

        assertEquals(3, result.started());
        assertEquals(2, result.completed());
        assertEquals(1, result.failed());
        assertEquals(0, result.cancelled());
        assertEquals(1L, result.failuresByType().get(AssertionError.class.getName()));
    }

    /** 驱动包装在普通异常图中的 JVM 致命错误不能被探针当作一次可继续失败。 */
    @Test
    void propagatesVirtualMachineErrorNestedInOperationFailure() {
        OutOfMemoryError fatal = new OutOfMemoryError("nested operation fatal");
        ReactiveConcurrencyProbe.Plan plan = new ReactiveConcurrencyProbe.Plan(1, 1, Duration.ZERO);

        OutOfMemoryError observed = assertThrows(OutOfMemoryError.class, () -> ReactiveConcurrencyProbe.run(
                plan, ignored -> {
                    throw new IllegalStateException("driver wrapper", fatal);
                }).block(Duration.ofSeconds(1)));

        assertSame(fatal, observed);
    }
}
