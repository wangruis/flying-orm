package com.flying.orm.rdb.lock;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;

import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LockingReadFailBeforeConnectionTest {

    @Test
    void unsupportedDialectFailsBeforeTransactionLookupOrExecution() {
        AtomicInteger transactionLookups = new AtomicInteger();
        AtomicInteger queries = new AtomicInteger();
        RdbDialect custom = RdbDialect.of(
                "custom",
                RdbDialect.h2().schema(),
                RdbDialect.h2().pagination(),
                RdbDialect.h2().upsert());
        var client = LockingReadTestSupport.syncClient(custom, () -> {
            transactionLookups.incrementAndGet();
            return Optional.of(LockingReadTestSupport.jdbcTransaction(new AtomicInteger()));
        }, queries, new AtomicReference<>());

        assertThrows(UnsupportedOperationException.class,
                     () -> client.lockingRead(LockingReadSpec.of(
                             QuerySpec.of(LockingReadTestSupport.form(),
                                          ConditionGroup.and().build()),
                             ReadLock.update())));
        assertEquals(0, transactionLookups.get());
        assertEquals(0, queries.get());
    }
}
