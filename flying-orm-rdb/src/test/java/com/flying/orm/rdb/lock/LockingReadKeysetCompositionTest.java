package com.flying.orm.rdb.lock;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.condition.QueryShapeLimits;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.NullOrder;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUseOrigin;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.scope.ScopeAccessException;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LockingReadKeysetCompositionTest {

    @Test
    void lockingKeysetUsesTheSameStableSortBudgetAndLockSuffix() {
        AtomicInteger queries = new AtomicInteger();
        AtomicReference<com.flying.orm.core.sql.render.SqlRequest> request = new AtomicReference<>();
        var transaction = LockingReadTestSupport.jdbcTransaction(new AtomicInteger());
        var client = LockingReadTestSupport.syncClient(
                RdbDialect.postgresql(), () -> Optional.of(transaction), queries, request);
        LockingReadSpec spec = LockingReadSpec.of(
                QuerySpec.of(LockingReadTestSupport.form(), ConditionGroup.and().build()),
                ReadLock.updateSkipLocked());
        KeysetPageQuery page = KeysetPageQuery.first(
                20, KeysetSort.asc("tenant_id", NullOrder.LAST));

        client.lockingRead(spec, page);
        assertTrue(request.get().sql().endsWith(" FOR UPDATE SKIP LOCKED"), request.get().sql());
        assertEquals(1, queries.get());

        var limited = client.withQueryShapeLimits(
                QueryShapeLimits.defaults().withMaxSortCount(1));
        assertThrows(IllegalArgumentException.class, () -> limited.lockingRead(spec, page));
        assertEquals(1, queries.get());
    }

    @Test
    void keysetCacheSeparatesLockWaitModesOnTheSameClient() {
        AtomicInteger queries = new AtomicInteger();
        AtomicReference<com.flying.orm.core.sql.render.SqlRequest> request = new AtomicReference<>();
        var transaction = LockingReadTestSupport.jdbcTransaction(new AtomicInteger());
        var client = LockingReadTestSupport.syncClient(
                RdbDialect.postgresql(), () -> Optional.of(transaction), queries, request);
        QuerySpec query = QuerySpec.of(
                LockingReadTestSupport.form(), ConditionGroup.and().build());
        KeysetPageQuery page = KeysetPageQuery.first(
                20, KeysetSort.asc("tenant_id", NullOrder.LAST));

        client.lockingRead(LockingReadSpec.of(query, ReadLock.update()), page);
        String waitSql = request.get().sql();
        client.lockingRead(LockingReadSpec.of(query, ReadLock.updateNowait()), page);
        String noWaitSql = request.get().sql();
        client.lockingRead(LockingReadSpec.of(query, ReadLock.updateSkipLocked()), page);
        String skipLockedSql = request.get().sql();

        assertEquals(3, queries.get());
        assertTrue(waitSql.endsWith(" FOR UPDATE"), waitSql);
        assertTrue(noWaitSql.endsWith(" FOR UPDATE NOWAIT"), noWaitSql);
        assertTrue(skipLockedSql.endsWith(" FOR UPDATE SKIP LOCKED"), skipLockedSql);
    }

    @Test
    void governedLockingKeysetRejectsHiddenTieBreakerBeforeExecution() {
        AtomicInteger queries = new AtomicInteger();
        AtomicReference<com.flying.orm.core.sql.render.SqlRequest> request = new AtomicReference<>();
        var transaction = LockingReadTestSupport.jdbcTransaction(new AtomicInteger());
        var client = LockingReadTestSupport.syncClient(
                        RdbDialect.postgresql(), () -> Optional.of(transaction), queries, request)
                .withFieldUsePolicy(FieldUsePolicy.builder()
                                            .visibility("tenant_id", FieldVisibility.FULL)
                                            .allow("tenant_id", FieldUse.SORT)
                                            .allowInternal(
                                                    "id", FieldUseOrigin.INTERNAL_TIE_BREAKER,
                                                    FieldUse.SORT)
                                            .build());
        QuerySpec query = QuerySpec.of(
                        LockingReadTestSupport.form(), ConditionGroup.and().build())
                .withProjection(List.of("tenant_id"), List.of());
        LockingReadSpec spec = LockingReadSpec.of(query, ReadLock.update());
        KeysetPageQuery page = KeysetPageQuery.first(
                20, KeysetSort.asc("tenant_id", NullOrder.LAST));

        assertThrows(ScopeAccessException.class, () -> client.lockingRead(spec, page));
        assertEquals(0, queries.get());
    }

    @Test
    void oracleLockingKeysetFailsBeforeQueryExecution() {
        AtomicInteger queries = new AtomicInteger();
        AtomicReference<com.flying.orm.core.sql.render.SqlRequest> request = new AtomicReference<>();
        var transaction = LockingReadTestSupport.jdbcTransaction(new AtomicInteger());
        var client = LockingReadTestSupport.syncClient(
                RdbDialect.oracle(), () -> Optional.of(transaction), queries, request);
        LockingReadSpec spec = LockingReadSpec.of(
                QuerySpec.of(LockingReadTestSupport.form(), ConditionGroup.and().build()),
                ReadLock.update());
        KeysetPageQuery page = KeysetPageQuery.first(
                20, KeysetSort.asc("tenant_id", NullOrder.LAST));

        assertThrows(UnsupportedOperationException.class,
                     () -> client.lockingRead(spec, page));
        assertEquals(0, queries.get());
    }
}
