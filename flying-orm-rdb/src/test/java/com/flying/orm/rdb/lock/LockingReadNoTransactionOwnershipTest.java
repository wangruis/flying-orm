package com.flying.orm.rdb.lock;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LockingReadNoTransactionOwnershipTest {

    @Test
    void jdbcRetainsNowaitSqlWithoutTransactionConfiguration() {
        AtomicInteger queries = new AtomicInteger();
        AtomicReference<com.flying.orm.core.sql.render.SqlRequest> request = new AtomicReference<>();
        var client = LockingReadTestSupport.syncClient(
                RdbDialect.postgresql(), queries, request);

        client.lockingRead(LockingReadSpec.of(
                QuerySpec.of(LockingReadTestSupport.form(), ConditionGroup.and().build()),
                ReadLock.updateNowait()));

        assertEquals(1, queries.get());
        assertTrue(request.get().sql().endsWith(" FOR UPDATE NOWAIT"), request.get().sql());
    }

    @Test
    void cacheSeparatesAllReachableLockWaitModesOnTheSameClient() {
        AtomicInteger queries = new AtomicInteger();
        AtomicReference<com.flying.orm.core.sql.render.SqlRequest> request = new AtomicReference<>();
        var client = LockingReadTestSupport.syncClient(
                RdbDialect.postgresql(), queries, request);
        QuerySpec query = QuerySpec.of(
                LockingReadTestSupport.form(), ConditionGroup.and().build());

        client.lockingRead(LockingReadSpec.of(query, ReadLock.update()));
        String waitSql = request.get().sql();
        client.lockingRead(LockingReadSpec.of(query, ReadLock.updateNowait()));
        String noWaitSql = request.get().sql();
        client.lockingRead(LockingReadSpec.of(query, ReadLock.updateSkipLocked()));
        String skipLockedSql = request.get().sql();

        assertEquals(3, queries.get());
        assertTrue(waitSql.endsWith(" FOR UPDATE"), waitSql);
        assertTrue(noWaitSql.endsWith(" FOR UPDATE NOWAIT"), noWaitSql);
        assertTrue(skipLockedSql.endsWith(" FOR UPDATE SKIP LOCKED"), skipLockedSql);
    }
}
