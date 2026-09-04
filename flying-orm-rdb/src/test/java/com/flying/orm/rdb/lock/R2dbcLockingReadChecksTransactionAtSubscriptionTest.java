package com.flying.orm.rdb.lock;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class R2dbcLockingReadChecksTransactionAtSubscriptionTest {

    @Test
    void resolvesExternalTransactionLazilyAndNeverOwnsIt() {
        AtomicInteger lookups = new AtomicInteger();
        AtomicInteger queries = new AtomicInteger();
        AtomicInteger lifecycleCalls = new AtomicInteger();
        AtomicReference<com.flying.orm.core.sql.render.SqlRequest> request = new AtomicReference<>();
        var transaction = LockingReadTestSupport.r2dbcTransaction(lifecycleCalls);
        var client = LockingReadTestSupport.reactiveClient(
                RdbDialect.sqlServer(), () -> {
                    lookups.incrementAndGet();
                    return Mono.just(transaction);
                }, queries, request);
        var result = client.lockingRead(LockingReadSpec.of(
                QuerySpec.of(LockingReadTestSupport.form(), ConditionGroup.and().build()),
                ReadLock.updateSkipLocked()));

        assertEquals(0, lookups.get());
        assertEquals(0, queries.get());

        result.collectList().block();

        assertEquals(1, lookups.get());
        assertEquals(1, queries.get());
        assertEquals(0, lifecycleCalls.get());
        assertTrue(request.get().sql().contains(
                " WITH (UPDLOCK, ROWLOCK, READPAST)"), request.get().sql());
    }

    @Test
    void missingReactiveTransactionFailsBeforeQuery() {
        AtomicInteger lookups = new AtomicInteger();
        AtomicInteger queries = new AtomicInteger();
        var client = LockingReadTestSupport.reactiveClient(
                RdbDialect.postgresql(), () -> {
                    lookups.incrementAndGet();
                    return Mono.empty();
                }, queries, new AtomicReference<>());
        var result = client.lockingRead(LockingReadSpec.of(
                QuerySpec.of(LockingReadTestSupport.form(), ConditionGroup.and().build()),
                ReadLock.update()));

        assertEquals(0, lookups.get());
        assertThrows(LockingReadRequiredTransactionException.class,
                     () -> result.collectList().block());
        assertEquals(1, lookups.get());
        assertEquals(0, queries.get());
    }
}
