package com.flying.orm.rdb.reactive;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** 验证总超时已耗尽时不会再订阅下一段批量操作。 */
class R2dbcBatchDeadlineTest {

    @Test
    void doesNotSubscribeNextOperationAfterDeadlineExpired() throws Exception {
        R2dbcBatchDeadline deadline = R2dbcBatchDeadline.start(Duration.ofNanos(1));
        AtomicBoolean subscribed = new AtomicBoolean();

        Thread.sleep(1L);

        StepVerifier.create(deadline.protect(Mono.defer(() -> {
                        subscribed.set(true);
                        return Mono.just("unexpected");
                    })))
                    .expectError(TimeoutException.class)
                    .verify();

        assertFalse(subscribed.get());
    }

    @Test
    void acceptsAValidDurationWhoseNanosecondsDoNotFitInLong() {
        R2dbcBatchDeadline deadline = R2dbcBatchDeadline.start(Duration.ofSeconds(Long.MAX_VALUE));

        StepVerifier.create(deadline.protect(Mono.just("ok")))
                    .expectNext("ok")
                    .verifyComplete();
    }

    /** Flux 持续发出元素也不能重置整批截止时间；回执重放的流式哈希必须受总预算约束。 */
    @Test
    void fluxSignalsCannotResetTheTotalDeadline() {
        StepVerifier.withVirtualTime(() -> R2dbcBatchDeadline.start(Duration.ofSeconds(1))
                                                       .protect(Flux.interval(Duration.ofMillis(150)).take(20)))
                    .thenAwait(Duration.ofSeconds(1))
                    .expectNextCount(6)
                    .expectError(TimeoutException.class)
                    .verify();
    }

    /** 批量流正常完成后必须立刻撤销截止任务，不能把计时器保留到超时。 */
    @Test
    void cancelsFluxDeadlineWhenBatchCompletesNormally() {
        AtomicInteger subscriptions = new AtomicInteger();
        AtomicInteger cancellations = new AtomicInteger();

        assertEquals(1,
                     R2dbcBatchDeadline.start(Duration.ofSeconds(30))
                                         .protect(Flux.just(1), ignored -> Mono.defer(() -> {
                                             subscriptions.incrementAndGet();
                                             return Mono.never().doOnCancel(cancellations::incrementAndGet);
                                         }))
                                         .single()
                                         .block());

        assertTrue(subscriptions.get() > 0);
        assertEquals(subscriptions.get(), cancellations.get());
    }

    /** 批量流先失败时必须撤销截止任务，并保留原始失败对象。 */
    @Test
    void cancelsFluxDeadlineWhenBatchFails() {
        AtomicInteger cancellations = new AtomicInteger();
        IllegalStateException primary = new IllegalStateException("batch failed");

        assertSame(primary,
                   assertThrows(IllegalStateException.class,
                                () -> R2dbcBatchDeadline.start(Duration.ofSeconds(30))
                                                          .protect(Flux.error(primary), ignored -> Mono.never()
                                                                                                        .doOnCancel(
                                                                                                                cancellations::incrementAndGet))
                                                          .blockLast()));

        assertEquals(1, cancellations.get());
    }

    /** 下游取消批量输入时必须同时撤销截止任务。 */
    @Test
    void cancelsFluxDeadlineWhenDownstreamCancels() {
        AtomicInteger cancellations = new AtomicInteger();

        StepVerifier.create(R2dbcBatchDeadline.start(Duration.ofSeconds(30))
                                               .protect(Flux.never(), ignored -> Mono.never()
                                                                                         .doOnCancel(
                                                                                                 cancellations::incrementAndGet)))
                    .thenCancel()
                    .verify();

        assertEquals(1, cancellations.get());
    }
}
