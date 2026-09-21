package com.flying.orm.rdb.reactive;
import com.flying.orm.rdb.batch.*;
import com.flying.orm.rdb.observation.*;
import reactor.core.publisher.*;
import org.junit.jupiter.api.Test;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

import java.util.concurrent.atomic.AtomicInteger;
class R2dbcBatchAdmissionBudgetTest {
    @Test void noSecondInputWindowIsAcceptedBeforeActiveSqlFinishes() {
        AtomicInteger accepted = new AtomicInteger();
        Sinks.One<Long> first = Sinks.one();
        var h = new R2dbcOrdinaryBatchConnectionTest.Harness(
                index -> index == 0 ? first.asMono() : Flux.never());
        var request = R2dbcOrdinaryBatchConnectionTest.request(Flux.range(0, 20)
                .map(i -> new Object[]{i}).doOnNext(row -> accepted.incrementAndGet()), 2, BatchRowCountPolicy.ANY);
        var subscription = h.executor().writeBatch(request).subscribe();
        assertEquals(2, accepted.get());
        assertEquals(1, h.executions);
        first.tryEmitValue(2L);
        assertEquals(4, accepted.get());
        assertEquals(2, h.executions);
        assertEquals(1, h.gets);
        subscription.dispose();
        assertEquals(List.of(SignalType.CANCEL), h.releases);
    }

    @Test void completedWindowWaitsForPostBeforeAcceptingMoreInput() {
        AtomicInteger accepted = new AtomicInteger();
        Sinks.Empty<Void> post = Sinks.empty();
        var h = new R2dbcOrdinaryBatchConnectionTest.Harness(index -> Mono.just(2L));
        var request = R2dbcOrdinaryBatchConnectionTest.request(Flux.range(0, 10)
                .map(i -> new Object[]{i}).doOnNext(row -> accepted.incrementAndGet()), 2, BatchRowCountPolicy.ANY);
        var subscription = h.executor().writeBatch(request, offset -> post.asMono()).subscribe();
        assertEquals(2, accepted.get());
        assertEquals(1, h.executions);
        subscription.dispose();
        assertEquals(1, h.releases.size());
    }
}
