package com.flying.orm.rdb.reactive;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;

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
}
