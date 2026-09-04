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

class LockingReadRequiresExternalTransactionTest {

    @Test
    void jdbcRejectsMissingCallerTransactionBeforeSqlExecution() {
        AtomicInteger queries = new AtomicInteger();
        var client = LockingReadTestSupport.syncClient(
                RdbDialect.postgresql(), Optional::empty, queries, new AtomicReference<>());
        LockingReadSpec spec = LockingReadSpec.of(
                QuerySpec.of(LockingReadTestSupport.form(), ConditionGroup.and().build()),
                ReadLock.update());

        assertThrows(LockingReadRequiredTransactionException.class,
                     () -> client.lockingRead(spec));
        assertEquals(0, queries.get());
    }
}
