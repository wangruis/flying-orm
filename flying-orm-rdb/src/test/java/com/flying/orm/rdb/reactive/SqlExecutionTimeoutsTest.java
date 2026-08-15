package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.execution.SqlExecutionTimeoutException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 验证总耗时截止后会终止下游，同时把取消信号传回正在运行的数据库结果流。 */
class SqlExecutionTimeoutsTest {

    @Test
    void cancelsSubscribedSourceWhenTotalTimeoutExpires() {
        AtomicInteger subscriptions = new AtomicInteger();
        AtomicInteger cancellations = new AtomicInteger();
        Sinks.One<Long> deadline = Sinks.one();
        Flux<Object> source = Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.never().doOnCancel(cancellations::incrementAndGet);
        });

        StepVerifier.create(SqlExecutionTimeouts.total(
                        source,
                        Duration.ofSeconds(1),
                        deadline::asMono))
                    .then(() -> assertEquals(1, subscriptions.get()))
                    .then(() -> deadline.tryEmitValue(1L).orThrow())
                    .expectError(SqlExecutionTimeoutException.class)
                    .verify();

        assertEquals(1, subscriptions.get());
        assertEquals(1, cancellations.get());
    }

    /** 正常完成后必须撤销尚未到期的截止任务，避免高 QPS 查询长期堆积定时器。 */
    @Test
    void cancelsDeadlineWhenSourceCompletesNormally() {
        AtomicInteger deadlineSubscriptions = new AtomicInteger();
        AtomicInteger deadlineCancellations = new AtomicInteger();

        assertEquals(1,
                     SqlExecutionTimeouts.total(
                             Flux.just(1),
                             Duration.ofSeconds(30),
                             () -> Mono.defer(() -> {
                                 deadlineSubscriptions.incrementAndGet();
                                 return Mono.never().doOnCancel(deadlineCancellations::incrementAndGet);
                             }))
                                         .single()
                                         .block());

        assertTrue(deadlineSubscriptions.get() > 0);
        assertEquals(deadlineSubscriptions.get(), deadlineCancellations.get());
    }

    /** 结果流先失败时也必须撤销截止任务，并保留原始失败对象。 */
    @Test
    void cancelsDeadlineWhenSourceFails() {
        AtomicInteger deadlineCancellations = new AtomicInteger();
        IllegalStateException primary = new IllegalStateException("query failed");

        assertSame(primary,
                   assertThrows(IllegalStateException.class,
                                () -> SqlExecutionTimeouts.total(
                                                Flux.error(primary),
                                                Duration.ofSeconds(30),
                                                () -> Mono.never().doOnCancel(
                                                        deadlineCancellations::incrementAndGet))
                                                            .blockLast()));

        assertEquals(1, deadlineCancellations.get());
    }

    /** 驱动或自定义执行器主动报告 TimeoutException 时，也必须进入统一的执行超时分类。 */
    @Test
    void classifiesSourceTimeoutExceptionAsSqlExecutionTimeout() {
        TimeoutException sourceFailure = new TimeoutException("driver timeout");

        SqlExecutionTimeoutException actual = assertThrows(
                SqlExecutionTimeoutException.class,
                () -> SqlExecutionTimeouts.total(
                                Flux.error(sourceFailure),
                                Duration.ofSeconds(30),
                                Mono::never)
                                                  .blockLast());

        assertSame(sourceFailure, actual.getCause());
    }

    /** 下游取消结果流时必须同时撤销截止任务，不能把定时器留到超时。 */
    @Test
    void cancelsDeadlineWhenDownstreamCancels() {
        AtomicInteger deadlineCancellations = new AtomicInteger();

        StepVerifier.create(SqlExecutionTimeouts.total(
                        Flux.never(),
                        Duration.ofSeconds(30),
                        () -> Mono.never().doOnCancel(deadlineCancellations::incrementAndGet)))
                    .thenCancel()
                    .verify();

        assertEquals(1, deadlineCancellations.get());
    }
}
