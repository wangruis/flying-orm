package com.flying.orm.rdb.reactive;

import com.flying.orm.rdb.execution.SqlExecutionTimeoutException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** 验证总耗时截止后会终止下游，同时把取消信号传回正在运行的数据库结果流。 */
class SqlExecutionTimeoutsTest {

    @Test
    void cancelsSubscribedSourceWhenTotalTimeoutExpires() {
        AtomicInteger subscriptions = new AtomicInteger();
        AtomicInteger cancellations = new AtomicInteger();
        Flux<Object> source = Flux.defer(() -> {
            subscriptions.incrementAndGet();
            return Flux.never().doOnCancel(cancellations::incrementAndGet);
        });

        assertThrows(SqlExecutionTimeoutException.class,
                     () -> SqlExecutionTimeouts.total(source, Duration.ofMillis(50))
                                               .blockLast(Duration.ofSeconds(1)));

        assertEquals(1, subscriptions.get());
        assertEquals(1, cancellations.get());
    }
}
