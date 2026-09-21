package com.flying.orm.rdb.lock;

import com.flying.orm.core.condition.ConditionGroup;
import com.flying.orm.core.page.KeysetPageQuery;
import com.flying.orm.core.page.KeysetSort;
import com.flying.orm.core.page.NullOrder;
import com.flying.orm.core.scope.FieldUse;
import com.flying.orm.core.scope.FieldUseOrigin;
import com.flying.orm.core.scope.FieldUsePolicy;
import com.flying.orm.core.scope.FieldVisibility;
import com.flying.orm.core.sql.render.SqlRequest;
import com.flying.orm.rdb.dialect.RdbDialect;
import com.flying.orm.rdb.form.spec.QuerySpec;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LockingReadTransactionBoundaryTest {

    @Test
    void lockingPlansContainSqlSemanticsButNoConnectionRoutingContract() {
        for (Class<?> type : List.of(com.flying.orm.rdb.sync.SyncSqlExecutor.class,
                                    com.flying.orm.rdb.reactive.ReactiveSqlExecutor.class)) {
            assertFalse(Arrays.stream(type.getMethods())
                    .anyMatch(method -> method.getName().toLowerCase().contains("transaction")), type.getName());
        }
        for (Class<?> type : List.of(LockingReadSpec.class, LockingReadPlan.class)) {
            assertFalse(Arrays.stream(type.getDeclaredMethods())
                    .anyMatch(method -> method.getName().equals("routingIntent")), type.getName());
            assertFalse(Arrays.stream(type.getDeclaredFields())
                    .anyMatch(field -> field.getName().equals("routingIntent")), type.getName());
        }
        assertFalse(java.nio.file.Files.exists(java.nio.file.Path.of(
                "src/main/java/com/flying/orm/rdb/execution/QueryRoutingIntent.java")));
    }

    @Test
    void jdbcPlainReadsDoNotProbeTransactions() {
        verifyJdbc(false);
    }

    @Test
    void jdbcGovernedReadsDoNotProbeTransactions() {
        verifyJdbc(true);
    }

    @Test
    void reactivePlainReadsStayColdWithoutTransactionProbes() {
        verifyReactive(false);
    }

    @Test
    void reactiveGovernedReadsStayColdWithoutTransactionProbes() {
        verifyReactive(true);
    }

    @Test
    void sqlServerLockHintsRemainWithoutTransactionAdmission() {
        AtomicInteger queries = new AtomicInteger();
        AtomicReference<SqlRequest> request = new AtomicReference<>();
        var client = LockingReadTestSupport.reactiveClient(RdbDialect.sqlServer(),
                queries, request);
        assertDoesNotThrow(() -> client.lockingRead(spec()).collectList().block());
        assertEquals(1, queries.get());
        assertTrue(request.get().sql().contains(" WITH (UPDLOCK, ROWLOCK, READPAST)"), request.get().sql());
    }

    private static void verifyJdbc(boolean governed) {
        AtomicInteger queries = new AtomicInteger();
        AtomicReference<SqlRequest> request = new AtomicReference<>();
        var base = LockingReadTestSupport.syncClient(RdbDialect.postgresql(), queries, request);
        var client = governed ? base.withFieldUsePolicy(policy()) : base;
        assertDoesNotThrow(() -> client.lockingRead(spec()));
        assertDoesNotThrow(() -> client.lockingRead(spec(), IdRow.class));
        assertDoesNotThrow(() -> client.lockingRead(spec(), page()));
        assertDoesNotThrow(() -> client.lockingRead(spec(), page(), IdRow.class));
        assertEquals(4, queries.get());
        assertLockAndParameters(request.get());
    }

    private static void verifyReactive(boolean governed) {
        AtomicInteger queries = new AtomicInteger();
        AtomicReference<SqlRequest> request = new AtomicReference<>();
        var base = LockingReadTestSupport.reactiveClient(RdbDialect.postgresql(), queries, request);
        var client = governed ? base.withFieldUsePolicy(policy()) : base;
        var rows = client.lockingRead(spec());
        var page = client.lockingRead(spec(), page());
        assertEquals(0, queries.get());
        for (int subscription = 0; subscription < 2; subscription++) {
            assertDoesNotThrow(() -> rows.collectList().block());
            assertDoesNotThrow(() -> { page.block(); });
        }
        assertEquals(4, queries.get());
        assertLockAndParameters(request.get());
    }

    private static LockingReadSpec spec() {
        return LockingReadSpec.of(QuerySpec.of(LockingReadTestSupport.form(),
                ConditionGroup.and().where("id", ">", 7L).build())
                .withProjection(List.of("id"), List.of()), ReadLock.updateSkipLocked());
    }

    private static KeysetPageQuery page() {
        return KeysetPageQuery.first(20, KeysetSort.asc("id", NullOrder.LAST));
    }

    private static FieldUsePolicy policy() {
        return FieldUsePolicy.builder().visibility("id", FieldVisibility.FULL)
                .allow("id", FieldUse.FILTER, FieldUse.SORT)
                .allowInternal("id", FieldUseOrigin.INTERNAL_TIE_BREAKER, FieldUse.SORT).build();
    }

    private static void assertLockAndParameters(SqlRequest request) {
        assertTrue(request.sql().endsWith(" FOR UPDATE SKIP LOCKED"), request.sql());
        assertTrue(request.parameters().contains(7L), request.parameters().toString());
    }

    public record IdRow(Long id) {
    }
}
